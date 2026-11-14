#!/usr/bin/env python3
"""Finite late-IDR mux and AAC startup diagnostics; no production preroll policy."""
import argparse
from fractions import Fraction
import json
from pathlib import Path
import tempfile
import asyncio

from av_output_probe import compare_timing, decoded_audio, decoded_video, mux, pack_wire, pcm_difference
from audio_window import AudioWindowIndex
from audio_window_output import output_windows
from ingest_probe import inspect
from late_mux_input import LateMuxInput
from packet_wire import encode_frame
from rtsp_clock_probe import capture, check_repair
from rtsp_fixture import aac_frames, make_media
from shared_admission import LateAdmission


def adts_units(data):
    aac_frames(data)  # existing strict AAC-LC / 48 kHz / mono fixture contract
    units = []
    while data:
        size = ((data[3] & 3) << 11) | (data[4] << 3) | (data[5] >> 5)
        units.append(data[:size])
        data = data[size:]
    return units


async def case(worker, sink, directory, media, generation, offset, pns):
    original, frames, _ = await capture(worker, media, generation, offset, staggered=bool(offset))
    check_repair(original, frames, offset)
    original_wire = pack_wire(frames)
    gate = LateAdmission(generation)
    admission = adapter = observer = window = None
    plans = None
    video_seen = 0
    try:
        for obj in frames:
            gate.feed(encode_frame(obj))
            if observer is None:
                observer = gate.join()
                if observer is not None:
                    window = AudioWindowIndex(observer)
            if observer is not None:
                while not observer.queue.queue.empty():
                    with await observer.queue.receive() as delivery:
                        window.observe(delivery)
            if obj["type"] == "packet" and obj["stream"] == 0:
                video_seen += 1
            if admission is None and video_seen >= 8:
                admission = gate.join()
                if admission is not None:
                    adapter = LateMuxInput(admission)
                    plans = {n: window.plan(admission, preceding=n) for n in (0, 1)}
                    if any(plan is None for plan in plans.values()):
                        raise ValueError("bounded AAC history did not cover the IDR")
            if admission is not None:
                while not admission.queue.queue.empty():
                    with await admission.queue.receive() as delivery:
                        adapter.feed(delivery)
        if adapter is None:
            raise ValueError("no late IDR admission")
        clip = adapter.finish()
        first_sequence = admission.snapshot.first_sequence
    finally:
        if window is not None:
            window.close()
        gate.close()
    if pack_wire(frames) != original_wire:
        raise ValueError("private mux adapter mutated published source input")
    packets = [obj for obj in clip if obj["type"] == "packet"]
    source_packets = [obj for obj in frames if obj["type"] == "packet"]
    if packets != [{**obj, "sequence": i} for i, obj in enumerate(source_packets[first_sequence:])]:
        raise ValueError("mux adapter changed fields other than private sequence")
    audio = [obj for obj in frames if obj["type"] == "packet" and obj["stream"] == 1]
    late_audio = [obj for obj in packets if obj["stream"] == 1]
    late_video = [obj for obj in packets if obj["stream"] == 0]
    audio_start = sum(obj["sequence"] < first_sequence for obj in audio)
    video_start = sum(obj["stream"] == 0 for obj in source_packets[:first_sequence])
    if video_start != 15 or len(late_video) != 15 or not late_audio or audio_start < 1:
        raise ValueError("fixture did not establish second-GOP admission")
    if any(obj["duration"] != 1024 for obj in audio):
        raise ValueError("fixture requires 1024-sample AAC access units")
    gap = Fraction(late_audio[0]["pts"], 48000) - Fraction(late_video[0]["pts"], 90000)
    if gap <= 0:
        raise ValueError("fixture did not expose demux-order audio gap")

    full_path, late_path = directory / f"full-{generation}.mp4", directory / f"late-{generation}.mp4"
    await mux(sink, full_path, original_wire)
    unadapted = [obj for obj in frames if obj["type"] in ("hello", "stream")]
    unadapted += source_packets[first_sequence:] + [frames[-1]]
    await mux(sink, directory / f"unadapted-{generation}.mp4", pack_wire(unadapted),
              success=False, failure_contains="packet gap/duplicate")
    await mux(sink, late_path, pack_wire(clip))
    full_info, late_info = await inspect(full_path), await inspect(late_path)
    # Both outputs must retain the original source clocks, not independent track zeroes.
    for source, output in ((source_packets, full_info), (packets, late_info)):
        for track, rate in ((0, 90000), (1, 48000)):
            left = [obj for obj in source if obj["stream"] == track]
            right = [obj for obj in output["packets"] if obj["stream_index"] == track]
            if len(left) != len(right):
                raise ValueError("mux changed packet count")
            tb = Fraction(output["streams"][track]["time_base"])
            for a, b in zip(left, right):
                if any(Fraction(a[key], rate) != b[key] * tb for key in ("pts", "dts", "duration")):
                    raise ValueError("mux changed source clock")
    expected_packets = [obj for obj in full_info["packets"] if obj["stream_index"] == 0][video_start:]
    expected_packets += [obj for obj in full_info["packets"] if obj["stream_index"] == 1][audio_start:]
    if compare_timing({"streams": full_info["streams"], "packets": expected_packets}, late_info) != 0:
        raise ValueError("late mux payload/clock differs from full mux control")
    reference_video = await decoded_video(directory / "320.h264")
    if await decoded_video(late_path) != reference_video[15:30]:
        raise ValueError("late mux video differs from source")

    elementary = (directory / "320.aac").read_bytes()
    units = adts_units(elementary)
    if [bytes.fromhex(obj["data"]) for obj in audio] != aac_frames(elementary)[:len(audio)]:
        raise ValueError("AAC worker packets differ from elementary source")
    continuous = (await decoded_audio(directory / "320.aac"))[:len(audio) * 1024 * 2]
    if await decoded_audio(full_path) != continuous:
        raise ValueError("full mux differs from continuous AAC decoder control")
    fresh_path = directory / f"fresh-{generation}.aac"
    fresh_path.write_bytes(b"".join(units[audio_start:len(audio)]))
    fresh = await decoded_audio(fresh_path)
    actual = await decoded_audio(late_path)
    if actual != fresh:
        raise ValueError("late mux PCM differs from direct fresh-AAC control")
    reference = continuous[audio_start * 1024 * 2:]
    difference = pcm_difference(reference, actual)
    if not difference["different_samples"]:
        raise ValueError("fixture did not expose AAC startup state")
    warmups = {}
    for preceding in (1, 2, 4, 8, audio_start):
        warm_path = directory / f"warm-{generation}-{preceding}.aac"
        warm_path.write_bytes(b"".join(units[audio_start - preceding:len(audio)]))
        warm = await decoded_audio(warm_path)
        if len(warm) != len(reference) + preceding * 1024 * 2:
            raise ValueError("AAC warmup sample count changed")
        # Analytical comparison only. Actual output is never trimmed or replaced.
        warmups[preceding] = pcm_difference(reference, warm[preceding * 1024 * 2:])
    if warmups[audio_start]["different_samples"]:
        raise ValueError("full preceding history did not recover continuous PCM")
    short_exact = warmups[1]["different_samples"] == 0
    if pns is False:
        if not short_exact or difference["last"] >= 1024:
            raise ValueError("PNS-disabled fixture no longer isolates one-AU startup state")
    elif any(warmups[n]["different_samples"] == 0 for n in (1, 2, 4, 8)):
        raise ValueError("default-PNS fixture no longer demonstrates residual short-preroll differences")
    window_results = await output_windows(sink, directory, generation, frames, first_sequence,
                                          plans, units, continuous, full_info, reference_video, pns)
    if pack_wire(frames) != original_wire:
        raise ValueError("window oracle mux changed source input")
    return {"generation": generation, "profile": clip[1]["profile"], "audio_offset": offset,
            "timestamp_windows": window_results,
            "window_index": {"peak_records": window.peak_records,
                             "peak_represented_wire_bytes": window.peak_wire_bytes,
                             "evicted_records": window.evicted,
                             "max_media_seconds": "1/4", "max_age_seconds": 0.5,
                             "retained_media_bodies": 0},
            "aac_pns": "encoder default" if pns is None else pns,
            "aac_configuration": {key: clip[2][key] for key in
                                  ("extradata", "profile", "sample_rate", "channels", "frame_size", "time_base")},
            "unadapted_suffix_rejected": True,
            "first_global_sequence": first_sequence, "first_video_pts": late_video[0]["pts"],
            "first_audio_pts": late_audio[0]["pts"], "first_audio_source_index": audio_start,
            "audio_gap_seconds_exact": str(gap), "audio_gap_samples_48k": str(gap * 48000),
            "audio_end_after_video_seconds_exact": str(
                Fraction(late_audio[-1]["pts"] + late_audio[-1]["duration"], 48000) -
                Fraction(late_video[-1]["pts"] + late_video[-1]["duration"], 90000)),
            "late_video_frames": len(late_video), "late_aac_packets": len(late_audio),
            "decoded_audio_samples": len(actual) // 2, "fresh_start_difference": difference,
            "mux_clocks_and_payloads_preserved": True, "late_mux_video_exact": True,
            "fresh_aac_control_exact": True, "one_au_warmup_recovers_reference": short_exact,
            "warmup_differences_by_preceding_aus": warmups,
            "fresh_difference_after_first_au": pcm_difference(reference[2048:], actual[2048:]),
            "actual_output_trimmed_or_padded": False}


async def run(worker, sink):
    with tempfile.TemporaryDirectory(prefix="amcrust-late-audio-") as temporary:
        root = Path(temporary)
        results = []
        for name, profile, offsets, pns in (("main", "main", (0, 0.25), None),
                                            ("high", "high", (0.25,), None),
                                            ("main-no-pns", "main", (0.25,), False)):
            directory = root / name
            directory.mkdir()
            media = await make_media(directory, 320, 180, profile=profile, aac_pns=pns)
            for offset in offsets:
                results.append(await case(worker, sink, directory, media, len(results) + 1, offset, pns))
        if any(case["aac_configuration"] != results[0]["aac_configuration"] for case in results):
            raise ValueError("AAC controls did not share identical configuration descriptors")
        return {"cases": results,
                "scope": "finite diagnostic captures; no production AAC preroll, mux API, or Apple acceptance claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--sink", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.worker.resolve(), args.sink.resolve())), indent=2))


if __name__ == "__main__":
    main()
