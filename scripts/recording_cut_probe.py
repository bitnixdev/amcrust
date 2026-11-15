#!/usr/bin/env python3
"""Native recording-local cut controls while the single shared acquisition continues."""
import argparse
import asyncio
from collections import OrderedDict
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import tempfile
import time

from audio_history import HistorySource
from av_output_probe import decoded_audio, decoded_video, mux, pack_wire, pcm_difference
from ingest_probe import inspect, stop_process
from late_audio_probe import adts_units
from packet_ingest_probe import read_frame
from packet_wire import encode_frame
from recording_cut import RecordingCutInput
from rtsp_clock import ClockNormalizer
from rtsp_fixture import CameraFixture, make_media


def sha(raw):
    return hashlib.sha256(raw).digest()


async def case(worker, sink, directory, media, generation, offset, action, replacement=None):
    clock = ClockNormalizer(generation, report_timeout=Fraction(5, 4))
    source = HistorySource(generation)
    observer = recorder = process = None
    oracle, expected = OrderedDict(), []
    ordinary_expected, ordinary_actual = hashlib.sha256(), hashlib.sha256()
    counts, received = [0, 0], 0
    artifact = None
    cut_at = prepared_at = None
    active_at_commit = denied = False
    failure = None
    mode = "parameter-change" if action == "source-failure" else "disconnect"
    async with CameraFixture(media, mode, clocked=True, audio_offset=offset,
            staggered_reports=bool(offset), send_seconds=3.2, replacement_sps=replacement,
            parameter_change_frame=30 if replacement else 15) as camera:
        try:
            process = await asyncio.create_subprocess_exec(str(worker), "--rtsp-fixture", str(camera.port),
                str(generation), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            while True:
                clock.expire()
                timeout = max(0.001, min(2.5, clock.next_deadline - time.monotonic()))
                try:
                    raw = await asyncio.wait_for(read_frame(process.stdout), timeout)
                except asyncio.TimeoutError:
                    clock.expire()
                    raise ValueError("source read deadline exceeded")
                if raw is None:
                    raise ValueError("native source EOF")
                for obj in clock.feed(raw):
                    if obj["type"] == "end":
                        raise ValueError("unexpected finite source end")
                    frame = await source.feed(encode_frame(obj))
                    if obj["type"] == "packet":
                        counts[obj["stream"]] += 1
                        if obj["stream"] == 1:
                            oracle[obj["sequence"]] = sha(frame.raw)
                            if len(oracle) > 64:
                                oracle.popitem(last=False)
                    if observer is None:
                        observer = source.join()
                    if observer is not None:
                        ordinary_expected.update(frame.raw)
                        with await observer.queue.receive() as delivery:
                            ordinary_actual.update(delivery.frame.raw)
                            received += 1
                    if recorder is None and counts[0] >= 8:
                        handoff = source.open(preceding=1)
                        if handoff is not None:
                            expected = [(r.sequence, oracle[r.sequence]) for r in handoff.history._records]
                            recorder = RecordingCutInput(handoff)
                    if recorder is not None and recorder.state == "open":
                        expected.append((obj["sequence"], sha(frame.raw)))
                        with await recorder._handoff.receive() as delivery:
                            recorder.feed(delivery)
                        if counts[0] == 24:
                            prepared_at = sum(counts)
                            cut_at = recorder.prepare_cut()
                    elif (recorder is not None and recorder.state == "prepared" and
                          sum(counts) > prepared_at and action != "source-failure"):
                        # There is now newer media queued for this subscriber. Cut excludes it.
                        if action == "subscriber-cancel":
                            recorder.abort()
                            try:
                                recorder.commit_cut()
                            except ValueError:
                                denied = True
                            else:
                                raise ValueError("cancelled recording committed")
                        else:
                            active_at_commit = (source._gate._epoch.active and not source._gate._ended and
                                                process.returncode is None and
                                                not recorder._handoff.admission.queue.queue.empty())
                            artifact = recorder.commit_cut()
                    await asyncio.sleep(0)
                    del frame
        except ValueError as error:
            failure = str(error)
            expected_error = "parameter sets changed" if action == "source-failure" else "native source EOF"
            if expected_error not in failure:
                raise
        finally:
            try:
                try:
                    source.close("source_failed")
                finally:
                    clock._abort()
                if recorder is not None and recorder.state == "prepared":
                    try:
                        recorder.commit_cut()
                    except ValueError:
                        denied = True
                    else:
                        raise ValueError("retired source committed prepared recording")
            finally:
                try:
                    if recorder is not None and recorder.state != "committed":
                        recorder.abort()
                finally:
                    if process is not None:
                        await stop_process(process)
        if (observer is None or recorder is None or cut_at is None or camera.connections != 1 or
                camera.played != 1 or process.returncode is None or
                received != sum(counts) - observer.snapshot.first_sequence or
                ordinary_expected.digest() != ordinary_actual.digest()):
            raise ValueError(f"recording cut lifecycle mismatch: gen={generation}, observer={observer is not None}, "
                             f"recorder={recorder is not None}, cut={cut_at}, connections={camera.connections}, "
                             f"played={camera.played}, exit={process.returncode}, received={received}, counts={counts}, "
                             f"hash={ordinary_expected.digest() == ordinary_actual.digest()}")
        if source.history.bytes or source.history._timer is not None:
            raise ValueError("recording source retained history")
    result = {"generation": generation, "action": action, "published_packets": counts,
              "ordinary_deliveries_exact": received, "ordinary_first_sequence": observer.snapshot.first_sequence,
              "source_failure": failure,
              "cut_source_sequence": cut_at, "later_source_packets": sum(counts) - prepared_at,
              "connections": camera.connections, "worker_reaped": True,
              "state": recorder.state, "committed_while_source_active_with_newer_queued_media": active_at_commit,
              "prepared_cut_refused": denied}
    if action != "commit":
        if not denied or artifact is not None or recorder.state != "aborted" or recorder._frames or recorder._bytes:
            raise ValueError("aborted recording retained or emitted private input")
        return result
    if not active_at_commit or sum(counts) - prepared_at < 20 or source._gate._ended:
        raise ValueError("cut did not leave native acquisition running")
    packets = [obj for obj in artifact if obj["type"] == "packet"]
    if len(packets) != len(expected) or artifact[-1]["packets"] != len(expected):
        raise ValueError("private cut count differs from consumed input")
    for obj, (sequence, hashed) in zip(packets, expected):
        if sha(encode_frame({**obj, "sequence": sequence})) != hashed:
            raise ValueError("cut changed source packet fields or selection")
    output = directory / f"recording-cut-{generation}.mp4"
    await mux(sink, output, pack_wire(artifact))
    info = await inspect(output)
    for track, rate in ((0, 90000), (1, 48000)):
        before = [obj for obj in packets if obj["stream"] == track]
        after = [obj for obj in info["packets"] if obj["stream_index"] == track]
        if len(before) != len(after):
            raise ValueError("cut mux changed packet count")
        tb = Fraction(info["streams"][track]["time_base"])
        for a, b in zip(before, after):
            if any(Fraction(a[k], rate) != b[k] * tb for k in ("pts", "dts", "duration")):
                raise ValueError("cut mux changed source clocks")
            if track == 1 and b["data_hash"] != "SHA256:" + hashlib.sha256(bytes.fromhex(a["data"])).hexdigest():
                raise ValueError("cut mux changed AAC payload")
    video = [obj for obj in packets if obj["stream"] == 0]
    audio = [obj for obj in packets if obj["stream"] == 1]
    width = artifact[1]["width"]
    if len(video) != 9 or await decoded_video(output) != (await decoded_video(directory / f"{width}.h264"))[15:24]:
        raise ValueError("cut video differs from selected original frames")
    units = adts_units((directory / f"{width}.aac").read_bytes())
    first = Fraction(audio[0]["pts"] - int(offset * 48000), 1024)
    if first.denominator != 1:
        raise ValueError("cut AAC not on source AU boundary")
    first = int(first)
    direct = directory / f"cut-control-{generation}.aac"
    direct.write_bytes(b"".join(units[first:first + len(audio)]))
    actual = await decoded_audio(output)
    if actual != await decoded_audio(direct):
        raise ValueError("cut PCM differs from direct AAC copy control")
    continuous = (await decoded_audio(directory / f"{width}.aac"))[first * 2048:(first + len(audio)) * 2048]
    lead_samples = Fraction(video[0]["pts"], 90000) * 48000 - audio[0]["pts"]
    if lead_samples.denominator != 1 or len(actual) != len(audio) * 2048:
        raise ValueError("unexpected AAC sample boundary or decoder output length")
    boundary = int(lead_samples) * 2
    result.update(private_packet_count=len(packets), video_frames=len(video), aac_packets=len(audio),
                  source_clocks_exact=True, direct_aac_pcm_exact=True,
                  decoded_audio_samples=len(actual) // 2,
                  continuous_pcm_difference=pcm_difference(continuous, actual),
                  continuous_pcm_from_video_boundary=pcm_difference(continuous[boundary:], actual[boundary:]),
                  audio_lead_seconds=str(Fraction(video[0]["pts"], 90000) - Fraction(audio[0]["pts"], 48000)),
                  audio_end_after_video_seconds=str(Fraction(audio[-1]["pts"] + audio[-1]["duration"], 48000) -
                                                   Fraction(video[-1]["pts"] + video[-1]["duration"], 90000)))
    return result


async def run(worker, sink):
    with tempfile.TemporaryDirectory(prefix="amcrust-recording-cut-") as temporary:
        root = Path(temporary)
        main_dir, high_dir = root / "main", root / "high"
        main_dir.mkdir()
        high_dir.mkdir()
        main = await make_media(main_dir, 320, 180, profile="main")
        high = await make_media(high_dir, 352, 198, profile="high")
        replacement = next(nal for nal in high[0][0] if nal[0] & 31 == 7)
        results = []
        for generation, (directory, media, offset, action, sps) in enumerate((
                (main_dir, main, 0.25, "commit", None),
                (high_dir, high, 0, "commit", None),
                (main_dir, main, 0.25, "subscriber-cancel", None),
                (main_dir, main, 0.25, "source-failure", replacement)), 1):
            results.append(await case(worker, sink, directory, media, generation, offset, action, sps))
        return {"cases": results, "scope": "bounded recording-local IPC cut; no HDS/cloud, privacy erasure or Apple acceptance"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--sink", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.worker.resolve(), args.sink.resolve())), indent=2))


if __name__ == "__main__":
    main()
