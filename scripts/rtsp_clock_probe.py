#!/usr/bin/env python3
"""Clocked loopback RTSP -> bounded timestamp repair -> packet-copy fMP4 proof."""
import argparse
import asyncio
from fractions import Fraction
import hashlib
import json
from packet_wire import decode_frame, encode_frame
from pathlib import Path
import tempfile
import time

from av_output_probe import assemble, decoded, decoded_audio, decoded_video, mux, pack_wire, split_recording
from ingest_probe import command, inspect, stop_process
from h264_reorder import evidence_for, parse_sps
from packet_ingest_probe import MAX_FRAME, MediaQueue, Protocol, read_frame
from rtsp_clock import ClockNormalizer
from rtsp_fixture import CameraFixture, make_media, nals


async def capture(worker, media, generation, offset, mode="stall", staggered=False, replacement_sps=None):
    # Fixture SR period is 0.5s: tolerate one lost report, not indefinite loss.
    normalizer = ClockNormalizer(generation, report_timeout=Fraction(5, 4))
    original, normalized, counts = [], [], [0, 0]
    async with CameraFixture(media, mode, clocked=True, audio_offset=offset,
                             staggered_reports=staggered, replacement_sps=replacement_sps) as camera:
        process = await asyncio.create_subprocess_exec(
            str(worker), "--rtsp-fixture", str(camera.port), str(generation),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            while counts[0] < 30 or counts[1] < 80:
                normalizer.expire()
                timeout = min(2.5, normalizer.next_deadline - time.monotonic())
                try:
                    raw = await asyncio.wait_for(read_frame(process.stdout), max(0.001, timeout))
                except asyncio.TimeoutError:
                    normalizer.expire()  # distinguish clock expiry from a transport timeout
                    raise ValueError("source read deadline exceeded")
                if raw is None:
                    raise ValueError("source ended before intentional clip cut")
                frame = decode_frame(raw)
                original.append(frame)
                normalized.extend(normalizer.feed(raw))
                if frame["type"] == "packet":
                    counts[frame["stream"]] += 1
            if process.returncode is not None:
                raise ValueError("source failed before intentional clip cut")
            terminal = {"type": "end", "generation": generation, "packets": sum(counts)}
            normalized.extend(normalizer.feed(encode_frame(terminal)))
            original.append(terminal)
        finally:
            if not normalizer.protocol.ended:
                normalizer._abort()
            await stop_process(process)
    return original, normalized, normalizer


async def late_report_gate(worker, media, generation, mode, expected_error, late_tracks):
    """Observe the actual late SRs after refusal; never re-admit ambiguous packets."""
    clock, protocol = ClockNormalizer(generation), Protocol(generation)
    reports, before = set(), [0, 0]
    failure, first_audio_pts = None, None
    async with CameraFixture(media, mode, clocked=True, audio_offset=0.25) as camera:
        process = await asyncio.create_subprocess_exec(
            str(worker), "--rtsp-fixture", str(camera.port), str(generation),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            while len(reports) < 2:
                raw = await asyncio.wait_for(read_frame(process.stdout), 4)
                if raw is None:
                    raise ValueError("source ended before late sender reports")
                frame = protocol.accept(raw)
                if failure is None:
                    try:
                        released = clock.feed(raw)
                    except ValueError as error:
                        if expected_error not in str(error):
                            raise
                        failure = expected_error
                    else:
                        if released:
                            raise ValueError("ambiguous startup media escaped admission")
                if frame["type"] == "packet":
                    track = frame["stream"]
                    if track == 1 and before[1] == 0 and 1 not in reports:
                        first_audio_pts = frame["pts"]
                    if "rtcp_sr" in frame:
                        reports.add(track)
                    elif track not in reports:
                        before[track] += 1
            if failure is None or not all(before[t] > 0 for t in late_tracks):
                raise ValueError("fixture did not exercise refused media before SR")
            if first_audio_pts == 12000:
                raise ValueError("fixture no longer exposes the pre-SR audio-origin ambiguity")
            try:
                clock.feed(raw)
            except ValueError:
                if not clock.failed or clock.pending or clock.state:
                    raise ValueError("late reports left clock admission state alive")
            else:
                raise ValueError("late SR revived rejected clock generation")
            return {"mode": mode, "reason": failure, "packets_before_first_sr": before,
                    "first_audio_pts": first_audio_pts, "sender_first_audio_pts": 12000,
                    "late_reports_observed_and_refused": True}
        finally:
            clock._abort()
            await stop_process(process)


def check_repair(original, normalized, offset):
    if len(original) != len(normalized):
        raise ValueError("clock repair lost frames")
    counts = [0, 0]
    for before, after in zip(original, normalized):
        if before["type"] != "packet":
            if before != after:
                raise ValueError("clock repair changed configuration/lifecycle")
            continue
        track = before["stream"]
        expected = counts[track] * (6000 if track == 0 else 1024)
        if track == 1:
            expected += int(offset * 48000)
        if after["pts"] != expected or after["dts"] != expected:
            raise ValueError("normalized timestamps disagree with independent sender schedule")
        for key, value in before.items():
            if key in ("pts", "dts") and value is None:
                continue
            if after[key] != value:
                raise ValueError("clock repair changed known timestamps, payload or side data")
        counts[track] += 1
    return counts


async def output_case(worker, sink, directory, media, generation, offset, staggered, mode="stall", profile=None):
    original, frames, clock = await capture(worker, media, generation, offset, mode, staggered)
    counts = check_repair(original, frames, offset)
    video_stream = next(f for f in frames if f["type"] == "stream" and f["media"] == "video")
    if profile is not None and (video_stream["profile"] != profile or
                               clock.state[video_stream["index"]]["reorder_evidence"] is None):
        raise ValueError("fixture did not exercise explicit Main/High reorder evidence")
    # Only feed duration queues after common-epoch clock admission, never raw RTSP.
    healthy = MediaQueue(MAX_FRAME * 2, Fraction(1, 2), generation=generation)
    stalled = MediaQueue(MAX_FRAME * 2, Fraction(1, 2), generation=generation)
    terminal = "source_failed"
    try:
        for frame in frames:
            raw = encode_frame(frame)
            if not healthy.offer(raw) or await healthy.receive() != raw:
                raise ValueError("clock-admitted queue changed or refused healthy media")
            stalled.offer(raw)
        if stalled.overflow_limit != "media" or not healthy.protocol.ended:
            raise ValueError("clock-admitted media budget isolation failed")
        queue_peak = str(healthy.peak_duration)
        terminal = "source_complete"
    except asyncio.CancelledError:
        terminal = "cancelled"
        raise
    finally:
        healthy.detach(terminal)
        stalled.detach(terminal)
    path = directory / f"clock-{generation}.mp4"
    await mux(sink, path, pack_wire(frames))
    info = await inspect(path)
    for track in (0, 1):
        expected = [f for f in frames if f["type"] == "packet" and f["stream"] == track]
        actual = [p for p in info["packets"] if p["stream_index"] == track]
        if len(actual) != len(expected):
            raise ValueError("mux lost packets")
        input_clock = Fraction(1, 90000 if track == 0 else 48000)
        output_clock = Fraction(info["streams"][track]["time_base"])
        for left, right in zip(expected, actual):
            for field in ("pts", "dts", "duration"):
                if left[field] * input_clock != right[field] * output_clock:
                    raise ValueError("mux changed A/V clock")
            if track == 1 and right["data_hash"] != "SHA256:" + hashlib.sha256(bytes.fromhex(left["data"])).hexdigest():
                raise ValueError("AAC payload changed")
    video, audio = await decoded(path)
    reference_video = (await decoded_video(directory / "320.h264"))[:counts[0]]
    reference_audio = (await decoded_audio(directory / "320.aac"))[:counts[1] * 1024 * 2]
    if video != reference_video or audio != reference_audio:
        raise ValueError("decoded A/V differs from source elementary streams")
    init, fragments = split_recording(generation, path.read_bytes())
    rebuilt = directory / f"clock-rebuilt-{generation}.mp4"
    rebuilt.write_bytes(assemble(init, fragments))
    if await decoded(rebuilt) != (video, audio):
        raise ValueError("fragment assembly changed media")
    return {"generation": generation, "audio_offset_seconds": offset, "staggered_first_reports": staggered,
            "report_mode": mode, "video_profile": video_stream["profile"],
            "video_frames": counts[0], "audio_packets": counts[1], "fragments": len(fragments),
            "repaired_video_pts": clock.repaired_pts, "repaired_video_dts": clock.repaired_dts,
            "source_clock_and_output_timing_exact": True, "decoded_av_exact": True,
            "media_budget_seconds": 0.5, "healthy_peak_media_seconds": queue_peak,
            "media_stalled_consumer_detached": True}


async def run(worker, sink):
    with tempfile.TemporaryDirectory(prefix="amcrust-clock-") as temp:
        directory = Path(temp)
        media = await make_media(directory, 320, 180)
        results = [await output_case(worker, sink, directory, media, generation, offset, staggered)
                   for generation, offset, staggered in ((1, 0, False), (2, 0.25, False), (3, 0.25, True))]
        try:
            await capture(worker, media, 4, 0, "clock-jump")
        except ValueError as error:
            if "sender clock/SSRC discontinuity" not in str(error):
                raise
        else:
            raise ValueError("sender report jump accepted")
        results.append(await output_case(worker, sink, directory, media, 5, 0, False, "report-once-lost"))
        expired = []
        for generation, mode in enumerate(("reports-lost", "video-reports-lost", "clock-silent"), 6):
            try:
                await capture(worker, media, generation, 0, mode)
            except ValueError as error:
                if "sender report freshness expired" not in str(error):
                    raise
                expired.append(mode)
            else:
                raise ValueError("stale sender clock accepted")
        profiles = {}
        for generation, name, profile in ((9, "main", 77), (10, "high", 100)):
            profile_dir = directory / name
            profile_dir.mkdir()
            profile_media = await make_media(profile_dir, 320, 180, profile=name)
            profiles[name] = profile_media
            results.append(await output_case(worker, sink, profile_dir, profile_media,
                                             generation, 0.25, True, profile=profile))
        replacement = next(nal for au in profiles["high"][0] for nal in au if nal[0] & 31 == 7)
        try:
            await capture(worker, profiles["main"], 11, 0, "parameter-change", replacement_sps=replacement)
        except ValueError as error:
            if "parameter sets changed within clock generation" not in str(error):
                raise
        else:
            raise ValueError("in-session SPS replacement retained old reorder evidence")
        # Inspect a real reordered encoder configuration without feeding it to the
        # fixture's no-B RTP schedule. A deliberately false video_delay=0 must not
        # override the SPS evidence.
        reordered = directory / "reordered.h264"
        await command("ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
                      "testsrc2=size=320x180:rate=15", "-t", "2", "-c:v", "libx264",
                      "-preset", "veryfast", "-profile:v", "high", "-bf", "2", "-g", "15",
                      "-x264-params", "aud=1:repeat-headers=1:scenecut=0", "-f", "h264", str(reordered))
        units = nals(reordered.read_bytes())
        sps = next(n for n in units if n[0] & 31 == 7)
        pps = next(n for n in units if n[0] & 31 == 8)
        candidate = {"profile": 100, "video_delay": 0, "width": 320, "height": 180,
                     "extradata": (b"\x00\x00\x00\x01" + sps + b"\x00\x00\x00\x01" + pps).hex()}
        if ((await inspect(reordered))["streams"][0]["has_b_frames"] <= 0 or
                parse_sps(sps).reorder in (None, 0) or evidence_for(candidate) is not None):
            raise ValueError("real reordered SPS did not deny timestamp repair")
        late = []
        for generation, mode, reason, tracks in (
                (12, "late-reports", "missing shared source epoch", (0, 1)),
                (13, "audio-report-late", "first packet lacks sender-clock evidence", (1,))):
            late.append(await late_report_gate(worker, media, generation, mode, reason, tracks))
        return {"cases": results, "late_report_gates": late, "rtcp_clock_jump_rejected": True,
                "b_frame_sps_denies_repair": True,
                "in_session_sps_change_rejected": True,
                "freshness_faults_rejected": expired,
                "scope": "early SRs; Baseline or explicit zero-reorder Main/High; not general RTSP/HomeKit acceptance"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--sink", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(asyncio.wait_for(run(args.worker.resolve(), args.sink.resolve()), 60)), indent=2))
