#!/usr/bin/env python3
"""Bounded, synthetic-only FFmpeg NUT fanout experiment; not production ingest.

No camera access, credentials, or persistent media. Requires ffmpeg/ffprobe with
libx264 and AAC. JSON goes to stdout; all generated media lives in a temp directory.
"""

import argparse
import asyncio
import hashlib
import json
from fractions import Fraction
from pathlib import Path
import shutil
import tempfile
import time


CHUNK_BYTES = 16 * 1024


class Subscriber:
    """A byte-bounded queue; overflow detaches, never blocks the producer.

    NUT chunks are NOT access units. A detached subscriber cannot resume safely.
    Production requires packet-aware IDR admission and media-duration budgets.
    """

    def __init__(self, capacity):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.queue = asyncio.Queue(maxsize=capacity)
        self.detached = False
        self.peak_bytes = 0
        self.queued_bytes = 0

    def offer(self, chunk):
        if len(chunk) > CHUNK_BYTES:
            raise ValueError("oversized chunk")
        if self.detached:
            return
        try:
            self.queue.put_nowait(chunk)
            self.queued_bytes += len(chunk)
            self.peak_bytes = max(self.peak_bytes, self.queued_bytes)
        except asyncio.QueueFull:
            self.detached = True
            while not self.queue.empty():
                self.queue.get_nowait()
            self.queued_bytes = 0

    async def receive(self):
        chunk = await self.queue.get()
        self.queued_bytes -= len(chunk)
        return chunk


async def command(*args, timeout=30):
    # Temporary stderr files cannot fill an unread subprocess pipe.
    with tempfile.TemporaryFile() as errors:
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=errors
        )
        try:
            output, _ = await asyncio.wait_for(process.communicate(), timeout)
            if process.returncode:
                errors.seek(0)
                raise RuntimeError(errors.read(8192).decode(errors="replace"))
            return output
        finally:
            if process.returncode is None:
                process.kill()
            await process.wait()


async def inspect(path):
    return json.loads(await command(
        "ffprobe", "-v", "error", "-show_packets", "-show_streams",
        "-show_data_hash", "sha256", "-of", "json", str(path)
    ))


def compare_packets(source, destination):
    """Compare encoded payloads and timing separately, allowing one epoch shift.

    Missing DTS is reported, not silently replaced by PTS. ffprobe's decimal
    timestamps are rounded. Cross-track epoch comparison allows an output clock
    tick plus two microseconds of decimal rounding, reporting the actual spread.
    """
    src = source["packets"]
    dst = destination["packets"]
    report = {}
    shifts = []
    for stream in source["streams"]:
        index = stream["index"]
        left = [p for p in src if p["stream_index"] == index]
        right = [p for p in dst if p["stream_index"] == index]
        same_payloads = [p["data_hash"] for p in left] == [p["data_hash"] for p in right]
        timings = {}
        for field in ("pts_time", "dts_time", "duration_time"):
            available = bool(left) and len(left) == len(right) and all(
                field in a and field in b for a, b in zip(left, right)
            )
            if available:
                delta = [float(b[field]) - float(a[field]) for a, b in zip(left, right)]
                constant = max(delta) - min(delta) <= 0.000002
                timings[field] = {"constant_shift": constant, "shift_seconds": delta[0]}
                if field == "pts_time":
                    shifts.append(delta[0] if constant else None)
            else:
                timings[field] = {"available_for_all_packets": False}
        target_stream = next(s for s in destination["streams"] if s["index"] == index)
        report[stream["codec_type"]] = {
            "source_packets": len(left), "output_packets": len(right),
            "same_payloads": same_payloads, "timing": timings,
            "source_missing_dts": sum("dts_time" not in p for p in left),
            "output_missing_dts": sum("dts_time" not in p for p in right),
            "same_packet_flags": [p.get("flags") for p in left] == [p.get("flags") for p in right],
            "same_codec_config": (stream.get("extradata_hash") is not None
                                  and stream.get("extradata_hash") == target_stream.get("extradata_hash")),
        }
    tolerance = max(float(Fraction(s.get("time_base", "0")))
                    for s in destination["streams"]) + 0.000002
    comparable = len(shifts) == len(source["streams"]) and all(s is not None for s in shifts)
    spread = max(shifts) - min(shifts) if comparable else None
    report["av_pts_shift_spread_seconds"] = spread
    report["av_pts_shift_tolerance_seconds"] = tolerance
    report["common_av_pts_shift"] = comparable and spread <= tolerance
    return report


async def fanout(source, directory):
    healthy = [Subscriber(64), Subscriber(64)]  # <= 1 MiB each
    stalled = Subscriber(1)  # deliberately never drained, <= 16 KiB
    subscribers = healthy + [stalled]
    finished = asyncio.Event()

    async def save(subscriber, path):
        with path.open("wb") as output:
            while not (finished.is_set() and subscriber.queue.empty()):
                try:
                    chunk = await asyncio.wait_for(subscriber.receive(), 0.1)
                except asyncio.TimeoutError:
                    continue
                output.write(chunk)

    paths = [directory / f"consumer-{i}.nut" for i in range(2)]
    writers = [asyncio.create_task(save(s, p)) for s, p in zip(healthy, paths)]
    started = time.monotonic()
    first_byte = None
    total = 0
    digest = hashlib.sha256()
    with tempfile.TemporaryFile() as errors:
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
                "-re", "-i", str(source), "-map", "0:v:0", "-map", "0:a:0",
                "-c", "copy", "-f", "nut", "-flush_packets", "1", "pipe:1",
                stdout=asyncio.subprocess.PIPE, stderr=errors,
            )
            while True:
                chunk = await asyncio.wait_for(process.stdout.read(CHUNK_BYTES), 10)
                if not chunk:
                    break
                if first_byte is None:
                    first_byte = time.monotonic() - started
                total += len(chunk)
                digest.update(chunk)
                for subscriber in subscribers:
                    subscriber.offer(chunk)
            await asyncio.wait_for(process.wait(), 5)
            if process.returncode:
                errors.seek(0)
                raise RuntimeError(errors.read(8192).decode(errors="replace"))
            finished.set()
            await asyncio.wait_for(asyncio.gather(*writers), 5)
        finally:
            if process is not None:
                if process.returncode is None:
                    process.kill()
                await process.wait()
            for writer in writers:
                writer.cancel()
            await asyncio.gather(*writers, return_exceptions=True)
    if any(s.detached for s in healthy):
        raise RuntimeError("healthy consumer overflowed")
    if not stalled.detached:
        raise RuntimeError("fixture did not exercise overflow")
    for path in paths:
        if hashlib.sha256(path.read_bytes()).digest() != digest.digest():
            raise RuntimeError("consumer lost or duplicated container bytes")
    return paths, {
        "acquisition_processes": 1,
        "consumers": 2,
        "stalled_consumer_detached": stalled.detached,
        "peak_queued_bytes": [s.peak_bytes for s in subscribers],
        "queue_caps_bytes": [64 * CHUNK_BYTES, 64 * CHUNK_BYTES, CHUNK_BYTES],
        "container_bytes": total,
        "first_container_byte_seconds": first_byte,
        "elapsed_seconds": time.monotonic() - started,
        "consumer_bytes_match_source": True,
    }


async def experiment(seconds, bframes):
    with tempfile.TemporaryDirectory(prefix="amcrust-ingest-") as temp:
        directory = Path(temp)
        source = directory / "source.mp4"
        await command(
            "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=15",
            "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000",
            "-t", str(seconds), "-c:v", "libx264", "-preset", "ultrafast",
            "-g", "30", "-bf", str(bframes), "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-ac", "1", str(source),
        )
        paths, report = await fanout(source, directory)
        original = await inspect(source)
        copies = [await inspect(path) for path in paths]
        if copies[0]["packets"] != copies[1]["packets"]:
            raise RuntimeError("consumer packet metadata differs")
        # Decode every consumer, checking errors, not merely demux success.
        for path in paths:
            await command("ffmpeg", "-nostdin", "-v", "error", "-xerror",
                          "-i", str(path), "-map", "0:v:0", "-map", "0:a:0",
                          "-f", "null", "-")
        report["packet_comparison"] = compare_packets(original, copies[0])
        report["both_consumers_decode"] = True
        report["bframes"] = bframes
        report["scope"] = "synthetic file acquisition; no RTSP or late-join proof"
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=4, choices=range(2, 31))
    parser.add_argument("--bframes", type=int, default=0, choices=(0, 2))
    args = parser.parse_args()
    for program in ("ffmpeg", "ffprobe"):
        if not shutil.which(program):
            parser.error(f"{program} must be on PATH")
    print(json.dumps(asyncio.run(asyncio.wait_for(
        experiment(args.seconds, args.bframes), args.seconds + 90
    )), indent=2))


if __name__ == "__main__":
    main()
