#!/usr/bin/env python3
"""Clocked loopback RTSP -> bounded timestamp repair -> packet-copy fMP4 proof."""
import argparse
import asyncio
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import tempfile
import time

from av_output_probe import assemble, decoded, decoded_audio, decoded_video, mux, pack_wire, split_recording
from ingest_probe import inspect, stop_process
from packet_ingest_probe import read_frame
from rtsp_clock import ClockNormalizer
from rtsp_fixture import CameraFixture, make_media


async def capture(worker, media, generation, offset, mode="stall", staggered=False):
    normalizer = ClockNormalizer(generation)
    original, normalized, counts = [], [], [0, 0]
    async with CameraFixture(media, mode, clocked=True, audio_offset=offset,
                             staggered_reports=staggered) as camera:
        process = await asyncio.create_subprocess_exec(
            str(worker), "--rtsp-fixture", str(camera.port), str(generation),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            while counts[0] < 30 or counts[1] < 80:
                timeout = 2.5 if normalizer.ready else min(4, normalizer.deadline - time.monotonic())
                raw = await asyncio.wait_for(read_frame(process.stdout), max(0.001, timeout))
                if raw is None:
                    raise ValueError("source ended before intentional clip cut")
                frame = json.loads(raw)
                original.append(frame)
                normalized.extend(normalizer.feed(raw))
                if frame["type"] == "packet":
                    counts[frame["stream"]] += 1
            if process.returncode is not None:
                raise ValueError("source failed before intentional clip cut")
            terminal = {"type": "end", "generation": generation, "packets": sum(counts)}
            normalized.extend(normalizer.feed(json.dumps(terminal).encode()))
            original.append(terminal)
        finally:
            await stop_process(process)
    return original, normalized, normalizer


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


async def output_case(worker, sink, directory, media, generation, offset, staggered):
    original, frames, clock = await capture(worker, media, generation, offset, staggered=staggered)
    counts = check_repair(original, frames, offset)
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
            "video_frames": counts[0], "audio_packets": counts[1], "fragments": len(fragments),
            "repaired_video_pts": clock.repaired_pts, "repaired_video_dts": clock.repaired_dts,
            "source_clock_and_output_timing_exact": True, "decoded_av_exact": True}


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
        return {"cases": results, "rtcp_clock_jump_rejected": True,
                "scope": "early common-epoch SRs, baseline H264; not general RTSP or HomeKit acceptance"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--sink", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(asyncio.wait_for(run(args.worker.resolve(), args.sink.resolve()), 60)), indent=2))
