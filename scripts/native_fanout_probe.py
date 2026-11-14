#!/usr/bin/env python3
"""Continuous loopback native ingest -> clock admission -> leased shared fanout.

Rolling hashes/counters only: no accumulated native packet capture or output clip.
The fixture still owns finite generated input media; this is not a throughput test.
"""
import argparse
import asyncio
from fractions import Fraction
import gc
import hashlib
import json
from pathlib import Path
import tempfile
import time
import weakref

from ingest_probe import stop_process
from packet_ingest_probe import QueueClosed, integer, read_frame
from packet_wire import encode_frame
from rtsp_clock import ClockNormalizer
from rtsp_fixture import CameraFixture, make_media
from shared_admission import LateAdmission


class Peer:
    def __init__(self, admission, hold=False, retained_event=None):
        self.admission, self.hold = admission, hold
        self.retained_event = retained_event
        self.expected, self.actual = hashlib.sha256(), hashlib.sha256()
        self.offered = self.received = 0
        self.reason = None
        self.task = asyncio.create_task(self.consume())

    async def consume(self):
        retained = None
        try:
            while True:
                delivery = await self.admission.queue.receive()
                try:
                    self.actual.update(delivery.frame.raw)
                    self.received += 1
                    if self.hold and retained is None:
                        retained = delivery
                        if self.retained_event is not None:
                            self.retained_event.set()
                finally:
                    if delivery is not retained:
                        delivery.release()
        except QueueClosed as error:
            self.reason = error.reason
        finally:
            if retained is not None:
                retained.release()
                try:
                    retained.frame
                except ValueError:
                    pass
                else:
                    raise ValueError("closed peer retained lease access")

    def expect(self, frame):
        self.expected.update(frame.raw)
        self.offered += 1

    def summary(self):
        queue = self.admission.queue
        if queue.inflight_bytes or queue.bytes or queue.next_deadline is not None or queue._timer is not None:
            raise ValueError("retired native subscriber retained managed state")
        if not self.hold and (self.offered != self.received or self.expected.digest() != self.actual.digest()):
            raise ValueError("healthy native consumer lost or changed normalized frames")
        snapshot = self.admission.snapshot
        video = next(stream for stream in snapshot.streams if stream["media"] == "video")
        return {"received": self.received, "terminal": self.reason,
                "first_sequence": snapshot.first_sequence, "first_video_pts": snapshot.first_video_pts,
                "video_config": [video["profile"], video["width"], video["height"]],
                "peak_queue_bytes": queue.peak, "peak_inflight_bytes": queue.peak_inflight_bytes,
                "peak_inflight_frames": queue.peak_inflight_frames,
                "exact_normalized_wire_hash": not self.hold}


async def generation_case(worker, media, generation, *, mode, seconds, expected_error,
                          expect_late=True, replacement_sps=None, owners=None,
                          admitted=None, lifecycle=None):
    clock = ClockNormalizer(generation, report_timeout=Fraction(5, 4))
    gate = LateAdmission(generation, max_residence=0.4)
    peers = {}
    counts = [0, 0]
    failure = None
    terminal = "source_failed"
    peak_owners = peak_ranges = 0
    origin = None
    process = None
    worker_running_at_failure = False
    started = time.monotonic()
    async with CameraFixture(media, mode, clocked=True, audio_offset=0.25,
                             staggered_reports=True, send_seconds=seconds,
                             replacement_sps=replacement_sps) as camera:
        try:
            process = await asyncio.create_subprocess_exec(
                str(worker), "--rtsp-fixture", str(camera.port), str(generation),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
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
                # ClockNormalizer's bounded startup batch is the only packet batch;
                # after admission each iteration releases one inspection object.
                for obj in clock.feed(raw):
                    if obj["type"] == "end":
                        raise ValueError("unexpected finite end from live source")
                    frame = gate.feed(encode_frame(obj))
                    if owners is not None:
                        owners["frames"][id(frame)] = frame
                        for name in ("data", "extradata"):
                            if name in frame.metadata:
                                value = frame.metadata[name]
                                owners["ranges"][id(value)] = value
                                del value
                        for entry in frame.metadata.get("side_data", ()):
                            owners["ranges"][id(entry["data"])] = entry["data"]
                            del entry  # do not retain the last side-data body in the observer
                        peak_owners = max(peak_owners, len(owners["frames"]))
                        peak_ranges = max(peak_ranges, len(owners["ranges"]))
                    if obj["type"] == "packet":
                        counts[obj["stream"]] += 1
                    for name, eligible in (("early", True), ("holding", True), ("late", counts[0] >= 8)):
                        if name not in peers and eligible:
                            admission = gate.join()
                            if admission is not None:
                                peers[name] = Peer(admission, hold=name == "holding",
                                                   retained_event=admitted if name == "holding" else None)
                                origin = admission.snapshot.origin
                        if name in peers:
                            peer = peers[name]
                            if not peer.admission.queue.detached:
                                peer.expect(frame)
                            elif not peer.hold:
                                raise ValueError("healthy native subscriber detached before source failure")
                    # Explicit dispatch fairness: a startup batch cannot starve readers.
                    await asyncio.sleep(0)
                    for peer in peers.values():
                        if peer.task.done() and not peer.task.cancelled() and peer.task.exception() is not None:
                            raise peer.task.exception()
                    del frame
        except asyncio.CancelledError:
            terminal = "cancelled"
            raise
        except ValueError as error:
            failure = str(error)
            worker_running_at_failure = process is not None and process.returncode is None
            if expected_error not in failure:
                raise
        finally:
            # Invalidate and wake consumers BEFORE waiting for worker cleanup/reconnect.
            held_at_close = any(peer.hold and peer.admission.queue.inflight_frames > 0
                                for peer in peers.values())
            gate.close(terminal)
            clock._abort()
            try:
                await asyncio.wait_for(asyncio.gather(*(peer.task for peer in peers.values())), 2)
            finally:
                for peer in peers.values():
                    if not peer.task.done():
                        peer.task.cancel()
                await asyncio.gather(*(peer.task for peer in peers.values()), return_exceptions=True)
                if process is not None:
                    await stop_process(process)
                if lifecycle is not None:
                    lifecycle.update(terminal=terminal, held_delivery_at_close=held_at_close,
                        worker_reaped=process is not None and process.returncode is not None,
                        peers_done=all(peer.task.done() for peer in peers.values()),
                        managed_bytes=sum(peer.admission.queue.bytes + peer.admission.queue.inflight_bytes
                                          for peer in peers.values()),
                        candidate_timer_cleared=gate._candidate_timer is None)
        if not peers or not counts[0] or not counts[1] or process.returncode is None:
            raise ValueError("native generation did not admit A/V and reap its worker")
        if mode == "clock-silent" and not worker_running_at_failure:
            raise ValueError("silent-clock case did not fail while worker was alive")
        if ("late" in peers) != expect_late:
            raise ValueError("unexpected native late-admission outcome")
        if expect_late and (peers["late"].admission.snapshot.first_sequence != 54 or
                            peers["late"].admission.snapshot.first_video_pts != 90000):
            raise ValueError("native late admission changed original packet position/clock")
        if peers["holding"].reason != "residence_expired":
            raise ValueError("held native delivery did not expire independently")
        summaries = {name: peer.summary() for name, peer in peers.items()}
        if any(peer.reason != "source_failed" for peer in peers.values() if not peer.hold):
            raise ValueError("native failure did not retire healthy subscribers")
        if camera.played != 1 or camera.connections != 1:
            raise ValueError("consumers opened additional source connections")
        return {"generation": generation, "mode": mode, "packets": counts, "failure": failure,
                "peers": summaries, "connections": camera.connections,
                "worker_reaped": True, "worker_running_at_failure": worker_running_at_failure,
                "peak_registered_frames": peak_owners,
                "peak_registered_ranges": peak_ranges,
                "elapsed_seconds": time.monotonic() - started}, origin


async def run(worker, seconds=8):
    integer(seconds, 3, 20)
    with tempfile.TemporaryDirectory(prefix="amcrust-native-fanout-") as temporary:
        root = Path(temporary)
        main_dir, high_dir = root / "main", root / "high"
        main_dir.mkdir()
        high_dir.mkdir()
        main = await make_media(main_dir, 320, 180, profile="main", duration=seconds + 1)
        high = await make_media(high_dir, 352, 198, profile="high")
        replacement = next(nal for nal in high[0][0] if nal[0] & 31 == 7)
        owners = {name: weakref.WeakValueDictionary() for name in ("frames", "ranges")}
        results, origins = [], []
        cases = [(main, "disconnect", seconds, "native source EOF", True, None),
                 (high, "clock-silent", 2.2, "sender report freshness expired", False, None),
                 (high, "disconnect", 2.2, "native source EOF", True, None),
                 (main, "parameter-change", 2.2, "parameter sets changed", False, replacement)]
        for generation, (media, mode, duration, error, late, sps) in enumerate(cases, 1):
            result, origin = await generation_case(worker, media, generation, mode=mode,
                seconds=duration, expected_error=error, expect_late=late, replacement_sps=sps, owners=owners)
            if any(origin is previous for previous in origins):
                raise ValueError("native reconnect reused a source identity")
            origins.append(origin)  # identity tokens only, not media/configuration captures
            await asyncio.sleep(0)
            gc.collect()
            if any(owners.values()):
                raise ValueError("retired native generation retained registered frames/ranges")
            results.append(result)
        if results[0]["packets"][0] < (seconds - 1) * 15:
            raise ValueError("continuous native case ended too early")
        for result in results:
            expected = [77, 320, 180] if result["generation"] in (1, 4) else [100, 352, 198]
            if any(peer["video_config"] != expected for peer in result["peers"].values()):
                raise ValueError("reconnected subscriber reused retired codec configuration")
        admitted, cancellation = asyncio.Event(), {}
        task = asyncio.create_task(generation_case(worker, main, 5, mode="stall", seconds=2.2,
            expected_error="", owners=owners, admitted=admitted, lifecycle=cancellation))
        try:
            await asyncio.wait_for(admitted.wait(), 5)
            task.cancel()  # readiness comes from the consumer actually retaining a lease
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise ValueError("native owner cancellation was swallowed")
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if cancellation != dict(terminal="cancelled", held_delivery_at_close=True,
                                worker_reaped=True, peers_done=True,
                                managed_bytes=0, candidate_timer_cleared=True):
            raise ValueError("native cancellation did not clean up ownership and worker")
        del task
        await asyncio.sleep(0)
        gc.collect()
        if any(owners.values()):
            raise ValueError("cancelled native generation retained registered owners")
        return {"cases": results, "cancellation": cancellation,
                "final_registered_frames": len(owners["frames"]),
                "final_registered_ranges": len(owners["ranges"]),
                "scope": "continuous synthetic RTSP/IPC lifecycle; no accumulated output capture, native performance or Apple acceptance claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--seconds", type=int, default=8)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.worker.resolve(), args.seconds)), indent=2))


if __name__ == "__main__":
    main()
