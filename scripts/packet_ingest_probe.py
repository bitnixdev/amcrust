#!/usr/bin/env python3
"""Prove local-file libavformat packet IPC; never access a camera.

Worker and ffprobe must use the same FFmpeg version. This experiment checks exact
packet metadata/payload preservation, not HomeKit playback or RTSP acquisition.
"""
import argparse
import asyncio
from collections import deque
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile

from ingest_probe import command, inspect, stop_process

MAX_FRAME = 16 * 1024 * 1024
MAX_PAYLOAD = 4 * 1024 * 1024
MAX_SIDE = 1024 * 1024


def integer(value, low=0, high=(1 << 63) - 1):
    if type(value) is not int or not low <= value <= high:
        raise ValueError("invalid integer")
    return value


def byte_field(value, maximum=MAX_PAYLOAD):
    if (not isinstance(value, str) or len(value) > maximum * 2 or len(value) % 2
            or any(c not in "0123456789abcdef" for c in value)):
        raise ValueError("invalid byte field")
    return bytes.fromhex(value)


def side_data(value):
    if not isinstance(value, list) or len(value) > 64:
        raise ValueError("invalid side data")
    total = 0
    for entry in value:
        integer(entry["type"], high=(1 << 31) - 1)
        total += len(byte_field(entry["data"], MAX_SIDE))
        if total > MAX_SIDE:
            raise ValueError("side data limit exceeded")


class Protocol:
    """Strict lifecycle validation: one generation, all configs before packets."""
    def __init__(self, generation=1):
        self.generation = integer(generation, 1, (1 << 31) - 1)
        self.hello = None
        self.streams = {}
        self.sequence = 0
        self.ended = False

    def accept(self, raw):
        if not raw or len(raw) > MAX_FRAME or self.ended:
            raise ValueError("invalid frame/lifecycle")
        try:
            obj = json.loads(raw)
            if not isinstance(obj, dict) or integer(obj["generation"]) != self.generation:
                raise ValueError("invalid generation")
            kind = obj["type"]
            if self.hello is None:
                if kind != "hello" or integer(obj["version"]) != 1:
                    raise ValueError("missing/unsupported hello")
                integer(obj["streams"], 1, 8)
                integer(obj["avcodec_version"], 1)
                integer(obj["skip_samples_type"], high=(1 << 31) - 1)
                self.hello = obj
            elif kind == "stream":
                index = integer(obj["index"], high=self.hello["streams"] - 1)
                if self.sequence or index in self.streams:
                    raise ValueError("duplicate/late stream configuration")
                if obj["media"] not in ("audio", "video") or obj["codec"] not in ("h264", "aac"):
                    raise ValueError("fixture codec not supported")
                if not isinstance(obj["time_base"], list) or len(obj["time_base"]) != 2:
                    raise ValueError("invalid time base")
                for n in obj["time_base"]:
                    integer(n, 1, (1 << 31) - 1)
                byte_field(obj["extradata"])
                side_data(obj["side_data"])
                for field in ("profile", "level", "width", "height", "sample_rate", "channels",
                              "initial_padding", "trailing_padding", "seek_preroll"):
                    integer(obj[field], -(1 << 31), (1 << 31) - 1)
                self.streams[index] = obj
            elif kind in ("packet", "end"):
                if len(self.streams) != self.hello["streams"]:
                    raise ValueError("incomplete stream configuration")
                if kind == "end":
                    if integer(obj["packets"]) != self.sequence:
                        raise ValueError("incorrect final packet count")
                    self.ended = True
                else:
                    if integer(obj["sequence"]) != self.sequence:
                        raise ValueError("packet gap/duplicate")
                    if integer(obj["stream"]) not in self.streams:
                        raise ValueError("unknown stream")
                    for field in ("pts", "dts"):
                        if obj[field] is not None:
                            # AV_NOPTS_VALUE must be null, not a numeric sentinel.
                            integer(obj[field], -(1 << 63) + 1)
                    integer(obj["duration"])
                    integer(obj["flags"], high=(1 << 31) - 1)
                    byte_field(obj["data"])
                    side_data(obj["side_data"])
                    self.sequence += 1
            else:
                raise ValueError("unknown/duplicate frame type")
            return obj
        except (KeyError, TypeError, UnicodeError, json.JSONDecodeError) as error:
            raise ValueError("invalid frame schema") from error


async def read_frame(reader):
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as error:
        if not error.partial:
            return None
        raise ValueError("truncated frame header") from error
    size = struct.unpack("!I", header)[0]
    if not 0 < size <= MAX_FRAME:
        raise ValueError("invalid frame size")
    try:
        return await reader.readexactly(size)
    except asyncio.IncompleteReadError as error:
        raise ValueError("truncated frame body") from error


class QueueClosed(RuntimeError):
    """Terminal subscriber outcome, distinct from finite-input end frames."""
    def __init__(self, reason):
        self.reason = reason
        super().__init__(f"packet subscriber closed: {reason}")


class PacketQueue:
    """Single-event-loop bounded fanout; abort clears data and wakes all receivers."""
    CLOSE_REASONS = frozenset(("overflow", "generation_changed", "source_failed",
                               "source_complete", "cancelled"))
    def __init__(self, max_bytes, max_frames=256):
        if max_bytes <= 0 or max_frames <= 0:
            raise ValueError("queue limits must be positive")
        self.queue = asyncio.Queue(maxsize=max_frames)
        self.max_bytes = max_bytes
        self.bytes = 0
        self.peak = 0
        self.detached = False
        self.reason = None
        self.available = asyncio.Event()

    def offer(self, frame):
        if self.detached:
            return False
        if self.bytes + len(frame) > self.max_bytes or self.queue.full():
            self.detach("overflow")
            return False
        self.queue.put_nowait(frame)
        self.bytes += len(frame)
        self.peak = max(self.peak, self.bytes)
        self.available.set()
        return True

    def detach(self, reason="cancelled"):
        if reason not in self.CLOSE_REASONS:
            raise ValueError("unknown subscriber close reason")
        if self.detached:
            return  # preserve the first terminal cause
        self.detached = True
        self.reason = reason
        while not self.queue.empty():
            self.queue.get_nowait()
        self.bytes = 0
        self.available.set()

    async def receive(self):
        while True:
            if self.detached:
                raise QueueClosed(self.reason)
            if not self.queue.empty():
                frame = self.queue.get_nowait()
                self.bytes -= len(frame)
                if self.queue.empty():
                    self.available.clear()
                return frame
            # No await between checking/clearing readiness and entering wait:
            # offer/detach cannot interleave here on the owning event loop.
            self.available.clear()
            await self.available.wait()


class MediaQueue(PacketQueue):
    """Duration bound for already clock-admitted media, not a clock normalizer.

    Count the retained decode/presentation envelope against generation-wide media
    progress, in exact seconds across tracks. Delivered/in-flight data and receiver
    wall time are outside this queue's budget. Metadata still costs bytes/count.
    """
    def __init__(self, max_bytes, max_duration, max_frames=256, generation=1):
        integer(max_bytes, 1)
        integer(max_frames, 1)
        if type(max_duration) not in (int, Fraction) or max_duration <= 0:
            raise ValueError("media budget must be positive exact seconds")
        super().__init__(max_bytes, max_frames)
        self.max_duration = Fraction(max_duration)
        self.protocol = Protocol(generation)
        self.spans = deque()
        self.last_dts = {}
        self.watermark = None
        self.peak_duration = Fraction(0)
        self.overflow_limit = None

    @property
    def media_duration(self):
        starts = [span[0] for span in self.spans if span is not None]
        return self.watermark - min(starts) if starts else Fraction(0)

    def offer(self, raw):
        if self.detached:
            return False
        try:
            if not isinstance(raw, bytes):
                raise ValueError("media queue requires immutable wire bytes")
            frame = self.protocol.accept(raw)
            span = None
            if frame["type"] == "packet":
                pts = integer(frame["pts"], -(1 << 63) + 1)
                dts = integer(frame["dts"], -(1 << 63) + 1)
                duration = integer(frame["duration"], 1)
                track = frame["stream"]
                if track in self.last_dts and dts <= self.last_dts[track]:
                    raise ValueError("media queue requires increasing decode timestamps")
                tb = Fraction(*self.protocol.streams[track]["time_base"])
                span = (min(pts, dts) * tb, (max(pts, dts) + duration) * tb)
                self.last_dts[track] = dts
        except ValueError:
            self.detach("source_failed")
            raise
        watermark = self.watermark
        if span is not None:
            watermark = span[1] if watermark is None else max(watermark, span[1])
        starts = [item[0] for item in self.spans if item is not None]
        if span is not None:
            starts.append(span[0])
        duration = watermark - min(starts) if starts else Fraction(0)
        if duration > self.max_duration:
            self.overflow_limit = "media"
            self.detach("overflow")
            return False
        if self.bytes + len(raw) > self.max_bytes:
            self.overflow_limit = "bytes"
        elif self.queue.full():
            self.overflow_limit = "frames"
        if not super().offer(raw):
            return False
        # No suspension between the wire enqueue and its accounting entry.
        self.spans.append(span)
        self.watermark = watermark
        self.peak_duration = max(self.peak_duration, duration)
        return True

    async def receive(self):
        raw = await super().receive()
        self.spans.popleft()  # atomic with the base dequeue on the owning event loop
        return raw

    def detach(self, reason="cancelled"):
        super().detach(reason)
        self.spans.clear()
        self.last_dts.clear()
        self.watermark = None
        self.protocol = None


def summarize(packet):
    return {**{key: packet[key] for key in ("stream", "pts", "dts", "duration", "flags", "side_data")},
            "hash": "SHA256:" + hashlib.sha256(bytes.fromhex(packet["data"])).hexdigest()}


async def acquire(worker, source, *, started=None):
    healthy = [MediaQueue(MAX_FRAME * 2, Fraction(1, 2)) for _ in range(2)]
    stalled = PacketQueue(MAX_FRAME, max_frames=1)
    media_stalled = MediaQueue(MAX_FRAME * 2, Fraction(1, 2))
    protocol = Protocol()
    terminal = "source_failed"

    async def consume(subscriber):
        state = Protocol()
        packets = []
        while not state.ended:
            raw = await subscriber.receive()
            obj = state.accept(raw)
            if obj["type"] == "packet":
                packets.append(summarize(obj))
        if not state.ended:
            raise ValueError("consumer did not receive end frame")
        return packets

    tasks = [asyncio.create_task(consume(s)) for s in healthy]
    with tempfile.TemporaryFile() as errors:
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                str(worker), str(source), stdout=asyncio.subprocess.PIPE, stderr=errors)
            if started is not None:
                started.set_result(process)
            while True:
                raw = await asyncio.wait_for(read_frame(process.stdout), 10)
                if raw is None:
                    break
                obj = protocol.accept(raw)
                for subscriber in healthy + [media_stalled]:
                    subscriber.offer(raw)
                # Model a subscriber that stops consuming media after setup.
                if obj["type"] == "packet":
                    stalled.offer(raw)
                # A burst of immediately available reads must not starve consumers.
                await asyncio.sleep(0)
            await asyncio.wait_for(process.wait(), 5)
            if process.returncode or not protocol.ended:
                raise RuntimeError("worker failed or omitted end frame")
            copies = await asyncio.wait_for(asyncio.gather(*tasks), 5)
            if any(s.detached for s in healthy) or not stalled.detached or copies[0] != copies[1]:
                raise ValueError("fanout isolation/equality failed")
            if not media_stalled.detached or media_stalled.overflow_limit != "media":
                raise ValueError("media-duration isolation failed")
            terminal = "source_complete"
            return protocol, copies[0], [s.peak for s in healthy + [stalled, media_stalled]]
        except asyncio.CancelledError:
            terminal = "cancelled"
            raise
        finally:
            for subscriber in healthy + [stalled, media_stalled]:
                subscriber.detach(terminal)
            if process is not None:
                await stop_process(process)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def compare(protocol, packets, reference):
    if len(packets) != len(reference["packets"]):
        raise ValueError("packet count changed")
    if len(protocol.streams) != len(reference["streams"]):
        raise ValueError("stream count changed")
    for stream in reference["streams"]:
        actual = protocol.streams[stream["index"]]
        tb = "/".join(str(n) for n in actual["time_base"])
        if (actual["codec"] != stream["codec_name"] or tb != stream["time_base"]
                or "SHA256:" + hashlib.sha256(bytes.fromhex(actual["extradata"])).hexdigest()
                != stream["extradata_hash"]):
            raise ValueError("codec configuration/time base changed")
        for field in ("width", "height", "sample_rate", "channels", "level"):
            if field in stream and actual[field] != int(stream[field]):
                raise ValueError(f"stream {field} changed")
        # Fail rather than silently assert coverage for untested stream-side data.
        if actual["side_data"] or stream.get("side_data_list"):
            raise ValueError("uncovered fixture stream side data")
    skip_samples = 0
    for actual, expected in zip(packets, reference["packets"]):
        for field in ("pts", "dts", "duration"):
            if actual[field] != expected.get(field):
                raise ValueError(f"{field} changed")
        # These fixtures exercise keyframe/discard flags, not every AVPacket flag.
        flags = (1 if "K" in expected["flags"] else 0) | (4 if "D" in expected["flags"] else 0)
        if (actual["stream"] != expected["stream_index"] or actual["hash"] != expected["data_hash"]
                or actual["flags"] != flags):
            raise ValueError("packet stream/payload/flags changed")
        side = expected.get("side_data_list", [])
        if len(actual["side_data"]) != len(side):
            raise ValueError("side data count changed")
        for raw, decoded in zip(actual["side_data"], side):
            if decoded["side_data_type"] != "Skip Samples":
                raise ValueError("uncovered fixture side data")
            expected_bytes = struct.pack("<IIBB", decoded["skip_samples"], decoded["discard_padding"],
                                         decoded["skip_reason"], decoded["discard_reason"])
            if (raw["type"] != protocol.hello["skip_samples_type"]
                    or bytes.fromhex(raw["data"]) != expected_bytes):
                raise ValueError("AAC skip/discard side data changed")
            skip_samples += 1
    if not skip_samples:
        raise ValueError("fixture did not exercise AAC side data")
    return skip_samples


async def experiment(worker, bframes):
    with tempfile.TemporaryDirectory(prefix="amcrust-packet-") as temp:
        source = Path(temp) / "fixture.mp4"
        await command("ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
                      "testsrc2=size=640x360:rate=15", "-f", "lavfi", "-i",
                      "sine=frequency=1000:sample_rate=48000", "-t", "3", "-c:v", "libx264",
                      "-preset", "ultrafast", "-g", "30", "-bf", str(bframes),
                      "-pix_fmt", "yuv420p", "-c:a", "aac", "-ac", "1", str(source))
        protocol, packets, peaks = await acquire(worker, source)
        versions = json.loads(await command("ffprobe", "-v", "error", "-show_library_versions", "-of", "json"))
        codec_version = next(v["version"] for v in versions["library_versions"] if v["name"] == "libavcodec")
        if protocol.hello["avcodec_version"] != codec_version:
            raise ValueError("worker and ffprobe must use the same libavcodec version")
        skips = compare(protocol, packets, await inspect(source))
        return {"bframes": bframes, "packets": len(packets), "aac_side_data_packets": skips,
                "exact_packet_comparison": True, "healthy_consumers": 2,
                "stalled_consumer_detached": True, "peak_queue_bytes": peaks,
                "media_stalled_consumer_detached": True, "media_budget_seconds": 0.5,
                "scope": "local file packet preservation; no remux/playback/RTSP proof"}


async def cancellation_check(worker):
    # Block the actual native worker opening a local FIFO, then cancel its supervisor.
    # No camera/network or timing-dependent large fixture is required.
    with tempfile.TemporaryDirectory(prefix="amcrust-cancel-") as temp:
        fifo = Path(temp) / "blocked-input"
        os.mkfifo(fifo)
        started = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(acquire(worker, fifo, started=started))
        process = None
        try:
            process = await asyncio.wait_for(asyncio.shield(started), 5)
            if process.returncode is not None or task.done():
                raise ValueError("worker exited before cancellation exercise")
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise ValueError("supervisor did not propagate cancellation")
            if process.returncode is None:
                raise ValueError("cancelled worker not reaped")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    return {"blocked_native_worker_cancelled_and_reaped": True}


async def run(worker):
    results = [await experiment(worker, mode) for mode in (0, 2)]
    results.append(await cancellation_check(worker))
    # Open failure must propagate rather than look like an empty successful source.
    with tempfile.TemporaryDirectory(prefix="amcrust-missing-") as temp:
        try:
            await acquire(worker, Path(temp) / "missing.mp4")
        except RuntimeError:
            results.append({"native_open_failure_propagated": True})
        else:
            raise ValueError("missing source reported success")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(asyncio.wait_for(run(args.worker.resolve()), 90)), indent=2))
