#!/usr/bin/env python3
"""Bounded synthetic AMC2 fanout load; no camera, codec decoding or inspection hex.

Opaque H264-tagged transport samples exercise ownership and queues, not valid AVC.
CPU/wall time includes generation, hashing, weak tracking and tracemalloc overhead.
Run each CLI case in a fresh process: ru_maxrss is a process-lifetime high-water mark.
"""
import argparse
import asyncio
from fractions import Fraction
import gc
import hashlib
import json
import platform
import resource
import sys
import time
import tracemalloc
import weakref

from packet_ingest_probe import SharedSource, SharedCursor, SharedMediaQueue
from packet_wire import HEADER, MAGIC, ByteRange


def envelope(metadata, blob=b""):
    encoded = json.dumps(metadata, separators=(",", ":")).encode()
    return HEADER.pack(MAGIC, len(encoded), len(blob)) + encoded + blob


class Fixture:
    def __init__(self, payload_bytes):
        self.payload = bytes(range(256)) * (payload_bytes // 256) + bytes(range(payload_bytes % 256))
        self.digest = hashlib.sha256(self.payload).digest()

    def startup(self):
        yield envelope({"type": "hello", "version": 1, "generation": 1, "streams": 1,
                        "avcodec_version": 1, "skip_samples_type": 11})
        yield envelope({"type": "stream", "generation": 1, "index": 0,
            "codec": "h264", "media": "video", "time_base": [1, 90000],
            "extradata": {"offset": 0, "length": 4}, "side_data": [],
            **{key: 0 for key in ("profile", "level", "width", "height", "sample_rate",
                "channels", "initial_padding", "trailing_padding", "seek_preroll")}}, b"stub")

    def packet(self, sequence):
        return envelope({"type": "packet", "generation": 1, "sequence": sequence,
            "stream": 0, "pts": sequence * 6000, "dts": sequence * 6000,
            "duration": 6000, "flags": 0, "data": {"offset": 0, "length": len(self.payload)},
            "side_data": []}, self.payload)


class Owners:
    """Weak observation only: tracking itself must not keep publications alive."""
    def __init__(self):
        self.frames = weakref.WeakValueDictionary()
        self.ranges = weakref.WeakValueDictionary()
        self.views = weakref.WeakValueDictionary()

    def publish(self, frame):
        self.frames[id(frame)] = frame
        metadata = frame.metadata
        if metadata["type"] in ("stream", "packet"):
            primary = metadata["extradata"] if metadata["type"] == "stream" else metadata["data"]
            for item in (primary, *(entry["data"] for entry in metadata["side_data"])):
                if not isinstance(item, ByteRange):
                    raise ValueError("publication materialized byte fields")
                self.ranges[id(item)] = item

    def view(self, view):
        self.views[id(view)] = view

    def snapshot(self):
        bodies = {}
        for frame in self.frames.values():
            bodies[id(frame.raw)] = len(frame.raw)
        for item in self.ranges.values():
            bodies[id(item.body)] = len(item.body)
        for view in self.views.values():
            bodies[id(view.obj)] = len(view.obj)
        return {"frames": len(self.frames), "ranges": len(self.ranges), "views": len(self.views),
                "unique_wire_bodies": len(bodies), "wire_bytes": sum(bodies.values())}


def validate_options(packets, subscribers, payload_bytes, burst):
    for value in (packets, subscribers, payload_bytes, burst):
        if type(value) is not int:
            raise ValueError("integer load options required")
    if not 64 <= packets <= 1000000 or not 1 <= subscribers <= 8:
        raise ValueError("load count outside bounded range")
    if not 256 <= payload_bytes <= 256 * 1024 or not 1 <= burst <= 4:
        raise ValueError("payload/burst outside bounded range")


async def run(packets=10000, subscribers=4, payload_bytes=16384, burst=4):
    validate_options(packets, subscribers, payload_bytes, burst)
    if tracemalloc.is_tracing():
        raise ValueError("probe requires ownership of tracemalloc")
    fixture = Fixture(payload_bytes)  # fixed input storage, outside traced allocation interval
    owners = Owners()
    queues = [SharedMediaQueue(max(1024 * 1024, payload_bytes * 8 + 65536), Fraction(1, 2), 32)
              for _ in range(subscribers + 1)]
    cursors = [SharedCursor() for _ in range(subscribers)]
    source = SharedSource()
    held, tasks, samples = [], [], []
    sample_every = max(burst, (packets + 15) // 16)
    next_sample = sample_every
    retained_observed = False
    terminal = "source_failed"
    cpu_start, wall_start = time.process_time(), time.monotonic()
    tracemalloc.start()

    async def consume(index, queue, cursor):
        count = 0
        while not cursor.ended:
            frame = await queue.receive()
            metadata = cursor.accept(frame)
            if metadata is not frame.metadata:
                raise ValueError("subscriber copied metadata")
            if metadata["type"] == "packet":
                view = metadata["data"].view()
                owners.view(view)
                if hashlib.sha256(view).digest() != fixture.digest:
                    raise ValueError("subscriber payload mismatch")
                if index == 0 and count == 0:
                    held.append(view)  # intentionally keep one delivered body until midpoint
                else:
                    view.release()
                count += 1
                del view
            del frame, metadata  # do not pin last delivery while waiting for the next
        return count

    try:
        for raw in fixture.startup():
            frame = source.accept(raw)
            owners.publish(frame)
            for index, queue in enumerate(queues):
                queue.offer(frame)
                received = await queue.receive()
                if index < subscribers:
                    cursors[index].accept(received)
            del frame, received, raw
        tasks = [asyncio.create_task(consume(i, queue, cursor))
                 for i, (queue, cursor) in enumerate(zip(queues, cursors))]
        # Do not retain setup-loop aliases during the reclamation check.
        del queue
        for sequence in range(packets):
            raw = fixture.packet(sequence)
            frame = source.accept(raw)
            owners.publish(frame)
            for queue in queues:
                queue.offer(frame)
            del raw, frame, queue
            completed = sequence + 1
            if completed % burst == 0 or completed == packets:
                await asyncio.sleep(0)
                for task in tasks:
                    if task.done():
                        task.result()  # surface early consumer failures instead of hanging
                        raise ValueError("consumer ended before source end")
                if completed >= next_sample or completed == packets:
                    gc.collect()
                    snapshot = owners.snapshot()
                    current, peak = tracemalloc.get_traced_memory()
                    queued = sum(queue.bytes for queue in queues)
                    samples.append({"packets": completed, "traced_current_bytes": current,
                                    "traced_peak_bytes": peak, "queued_wire_bytes": queued, **snapshot})
                    if completed < packets // 2:
                        retained_observed |= snapshot["views"] == 1 and queued == 0
                    if completed > packets // 2 + burst and (snapshot["frames"] or snapshot["views"]
                            or snapshot["ranges"] != 1 or snapshot["unique_wire_bodies"] != 1):
                        raise ValueError("packet owners survived a drained steady-state checkpoint")
                    next_sample += sample_every
                if completed >= packets // 2 and held:
                    held.pop().release()
        end = source.accept(envelope({"type": "end", "generation": 1, "packets": packets}))
        owners.publish(end)
        for queue in queues:
            queue.offer(end)
        del end, queue
        counts = await asyncio.wait_for(asyncio.gather(*tasks), 10)
        if counts != [packets] * subscribers or not source.ended or source.parse_count != packets + 3:
            raise ValueError("fanout count/parse mismatch")
        if queues[-1].overflow_limit != "media" or any(queue.detached for queue in queues[:-1]):
            raise ValueError("stalled subscriber was not independently isolated")
        if not retained_observed:
            raise ValueError("delivered owner was not visible outside queue accounting")
        queue_peaks = [queue.peak for queue in queues]
        duration_peaks = [str(queue.peak_duration) for queue in queues]
        terminal = "source_complete"
        for queue in queues:
            queue.detach(terminal)
        del queue, source, cursors
        tasks.clear()
        gc.collect()
        final_owners = owners.snapshot()
        if any(final_owners.values()):
            raise ValueError("published owners survived complete teardown")
        final_current, peak = tracemalloc.get_traced_memory()
        late = [sample["traced_current_bytes"] for sample in samples if sample["packets"] >= packets * 3 // 4]
        late_spread = max(late) - min(late)
        if late_spread > 2 * 1024 * 1024:
            raise ValueError("late traced allocations did not stay within probe tolerance")
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return {"python": platform.python_version(), "platform": sys.platform,
                "machine": platform.machine(),
                "packets": packets, "subscribers": subscribers, "payload_bytes": payload_bytes,
                "burst": burst, "source_parses": packets + 3, "consumer_counts": counts,
                "stalled_reason": queues[-1].reason, "stalled_limit": queues[-1].overflow_limit,
                "queue_peak_bytes": queue_peaks, "queue_peak_media_seconds": duration_peaks,
                "samples": samples, "final_owners": final_owners,
                "traced_final_bytes": final_current, "traced_peak_bytes": peak,
                "late_traced_spread_bytes": late_spread,
                "process_peak_rss_bytes": rss if sys.platform == "darwin" else rss * 1024,
                "cpu_seconds": time.process_time() - cpu_start,
                "wall_seconds": time.monotonic() - wall_start,
                "scope": "synthetic instrumented fanout; no codec, RTSP, HomeKit or production performance claim"}
    except asyncio.CancelledError:
        terminal = "cancelled"
        raise
    finally:
        for queue in queues:
            queue.detach(terminal)
        for view in held:
            view.release()
        held.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        tracemalloc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packets", type=int, default=10000)
    parser.add_argument("--subscribers", type=int, default=4)
    parser.add_argument("--payload-bytes", type=int, default=16384)
    parser.add_argument("--burst", type=int, default=4)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(**vars(args))), indent=2))
