#!/usr/bin/env python3
"""Synthetic admission/retirement ownership stress; payloads are NOT decodable AVC."""
import argparse
import asyncio
import gc
import hashlib
import json
import tracemalloc
import weakref

from packet_ingest_probe import QueueClosed, integer
from packet_wire import encode_frame
from shared_admission import LateAdmission
from test_h264_reorder import descriptor, picture, wire
from test_packet_probe import hello, packet


async def cycle(generation, payload_bytes, frames, ranges):
    gate = LateAdmission(generation)
    terminal = ("generation_changed", "source_failed", "cancelled", "source_complete")[(generation - 1) % 4]
    profile = 77 if generation % 2 else 100
    peaks = {"inflight_frames": 0, "inflight_bytes": 0, "queue_bytes": 0}
    digests = [hashlib.sha256() for _ in range(3)]
    expected = [hashlib.sha256(), hashlib.sha256()]
    counts = [0, 0, 0]
    early = held_peer = late = held = None

    def publish(obj):
        frame = gate.feed(encode_frame(obj))
        frames[id(frame)] = frame
        for name in ("data", "extradata"):
            if name in frame.metadata:
                value = frame.metadata[name]
                ranges[id(value)] = value
        return frame

    async def consume(admission, index, retain=False):
        delivery = await admission.queue.receive()
        try:
            if delivery.frame.metadata["type"] == "packet":
                digests[index].update(delivery.frame.metadata["data"].view())
                counts[index] += 1
            for name, value in (("inflight_frames", admission.queue.inflight_frames),
                                ("inflight_bytes", admission.queue.inflight_bytes),
                                ("queue_bytes", admission.queue.peak)):
                peaks[name] = max(peaks[name], value)
        finally:
            if not retain:
                delivery.release()
        return delivery if retain else None

    try:
        publish({**hello(), "generation": generation})
        publish({**descriptor(profile=profile), "generation": generation})
        for sequence in range(30):
            idr = sequence % 15 == 0
            data = wire(picture(2 if idr else 0))
            data += b"\x55" * (payload_bytes - len(data))
            frame = publish({**packet(), "generation": generation, "sequence": sequence,
                             "pts": sequence * 6000, "dts": sequence * 6000,
                             "duration": 6000, "flags": int(idr), "data": data.hex()})
            expected[0].update(frame.metadata["data"].view())
            if sequence >= 15:
                expected[1].update(frame.metadata["data"].view())
            if early is None:
                early, held_peer = gate.join(), gate.join()
            if late is None and sequence >= 7:
                late = gate.join()
            await consume(early, 0)
            if sequence == 0:
                held = await consume(held_peer, 1, retain=True)
            else:
                await consume(held_peer, 1)
            if late is not None:
                await consume(late, 2)
        if (counts != [30, 30, 15] or
                [item.digest() for item in digests] != [expected[0].digest()] * 2 + [expected[1].digest()] or
                late.snapshot.first_sequence != 15 or late.snapshot.first_video_pts != 90000 or
                late.snapshot.generation != generation or late.snapshot.streams[0]["profile"] != profile):
            raise ValueError("generation churn changed admission or delivery")
        if terminal == "source_complete":
            publish({"type": "end", "generation": generation, "packets": 30})
            for index, admission in enumerate((early, held_peer, late)):
                await consume(admission, index)
                if not admission.queue.protocol.ended:
                    raise ValueError("finite source end not validated")
        if held_peer.queue.bytes != 0 or held_peer.queue.inflight_bytes <= 0:
            raise ValueError("held delivery disappeared behind empty queue accounting")
        waiters = [asyncio.create_task(item.queue.receive()) for item in (early, held_peer, late)]
        await asyncio.sleep(0)
        gate.close(terminal)
        outcomes = await asyncio.wait_for(asyncio.gather(*waiters, return_exceptions=True), 1)
        if any(not isinstance(item, QueueClosed) or item.reason != terminal for item in outcomes):
            raise ValueError("retirement did not wake every waiter with original reason")
        for admission in (early, held_peer, late):
            queue = admission.queue
            if queue.bytes or queue.inflight_bytes or queue.inflight_frames or queue.next_deadline is not None or queue._timer is not None:
                raise ValueError("retired subscriber retained managed ownership/timer")
        try:
            held.frame
        except ValueError:
            pass
        else:
            raise ValueError("retired delivery still grants frame access")
        if gate._candidate_timer is not None:
            raise ValueError("retired source retained candidate timer")
        return {"terminal": terminal, "counts": counts, "peaks": peaks}
    finally:
        gate.close("cancelled")


async def run(generations=500, payload_bytes=4096):
    integer(generations, 4, 10000)
    integer(payload_bytes, 256, 256 * 1024)
    frames, ranges = weakref.WeakValueDictionary(), weakref.WeakValueDictionary()
    terminals = {}
    totals = [0, 0, 0]
    peaks = {"inflight_frames": 0, "inflight_bytes": 0, "queue_bytes": 0}
    samples = []
    tracemalloc.start()
    try:
        for generation in range(1, generations + 1):
            result = await cycle(generation, payload_bytes, frames, ranges)
            # Let completed callbacks go, then check weakly observed frame/range ownership.
            await asyncio.sleep(0)
            gc.collect()
            if frames or ranges:
                raise ValueError("retired generation retained registered frame/range owners")
            terminals[result["terminal"]] = terminals.get(result["terminal"], 0) + 1
            totals = [left + right for left, right in zip(totals, result["counts"])]
            peaks = {key: max(value, result["peaks"][key]) for key, value in peaks.items()}
            if generation == 1 or generation % max(1, generations // 6) == 0 or generation == generations:
                samples.append({"generation": generation, "traced_bytes": tracemalloc.get_traced_memory()[0],
                                "frames": len(frames), "ranges": len(ranges)})
        traced_current, traced_peak = tracemalloc.get_traced_memory()
        return {"generations": generations, "payload_bytes": payload_bytes, "delivered_packets": totals,
                "terminal_counts": terminals, "peaks": peaks, "samples": samples,
                "traced_final_bytes": traced_current, "traced_peak_bytes": traced_peak,
                "final_frames": len(frames), "final_ranges": len(ranges),
                "scope": "synthetic lifecycle/managed ownership; not codec, RSS or production performance evidence"}
    finally:
        tracemalloc.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generations", type=int, default=500)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.generations, args.payload_bytes)), indent=2))


if __name__ == "__main__":
    main()
