#!/usr/bin/env python3
"""Paced native acquisition through owned AAC history; hashes only, no output capture."""
import argparse
import asyncio
from collections import OrderedDict
from fractions import Fraction
import gc
import hashlib
import json
from pathlib import Path
import tempfile
import time
import weakref

from audio_history import HistorySource
from ingest_probe import stop_process
from native_fanout_probe import Peer
from packet_ingest_probe import QueueClosed, integer, read_frame
from packet_wire import encode_frame
from rtsp_clock import ClockNormalizer
from rtsp_fixture import CameraFixture, make_media


def digest(raw):
    return hashlib.sha256(raw).digest()


class HistoryPeer(Peer):
    def __init__(self, handoff, oracle, *, hold=False, retained_event=None):
        self.handoff = handoff
        self.prefix_sequences = tuple(r.sequence for r in handoff.history._records)
        # Independent bounded source hashes, not hashes of the stored history bodies.
        expected = [oracle[sequence] for sequence in self.prefix_sequences]
        super().__init__(handoff.admission, hold, retained_event)
        for value in expected:
            self.expected.update(value)
            self.offered += 1
        self.expected_prefix = self.expected.digest()
        self.actual_prefix = None

    def expect(self, frame):
        self.expected.update(digest(frame.raw))
        self.offered += 1

    async def consume(self):
        try:
            for raw in self.handoff.history.frames:
                self.actual.update(digest(raw))
                self.received += 1
            del raw
            self.actual_prefix = self.actual.digest()
            if self.hold:
                if self.retained_event is not None:
                    self.retained_event.set()
                self.reason = await self.handoff.wait_closed()
            else:
                self.handoff.history.release()
                while True:
                    with await self.handoff.receive() as delivery:
                        self.actual.update(digest(delivery.frame.raw))
                        self.received += 1
        except QueueClosed as error:
            self.reason = error.reason
        finally:
            self.handoff.close(self.reason or "cancelled")
            self.handoff.history.release()

    def summary(self):
        result = super().summary()
        if self.expected_prefix != self.actual_prefix:
            raise ValueError("native history prefix differs from source hash oracle")
        if not self.handoff.history.released or not self.handoff.closed:
            raise ValueError("native history peer retained its prefix lease")
        result.update(prefix_sequences=self.prefix_sequences, prefix_exact=True)
        return result


def observe(owners, frame, source):
    owners["frames"][id(frame)] = frame
    for name in ("data", "extradata"):
        if name in frame.metadata:
            value = frame.metadata[name]
            owners["ranges"][id(value)] = value
    for entry in frame.metadata.get("side_data", ()):
        value = entry["data"]
        owners["ranges"][id(value)] = value
    if source.history is not None:
        for body in source.history._bodies.values():
            owners["bodies"][id(body)] = body


async def generation_case(worker, media, generation, *, mode, seconds, expected_error,
                          expect_history=True, replacement_sps=None, owners=None,
                          retained_event=None, lifecycle=None, holding_terminal="residence_expired"):
    if owners is None:
        owners = {name: weakref.WeakValueDictionary() for name in ("frames", "ranges", "bodies")}
    clock = ClockNormalizer(generation, report_timeout=Fraction(5, 4))
    source = HistorySource(generation, history_budgets={"max_age": 0.4})
    peers, oracle = {}, OrderedDict()
    counts = [0, 0]
    peaks = {name: 0 for name in owners}
    process = origin = failure = None
    terminal = "source_failed"
    worker_alive = False
    bytes_at_failure = None
    started = time.monotonic()
    async with CameraFixture(media, mode, clocked=True, audio_offset=0.25,
                             staggered_reports=True, send_seconds=seconds,
                             replacement_sps=replacement_sps,
                             parameter_change_frame=30 if replacement_sps is not None else 15) as camera:
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
                for obj in clock.feed(raw):
                    if obj["type"] == "end":
                        raise ValueError("unexpected finite end from live source")
                    frame = await source.feed(encode_frame(obj))
                    observe(owners, frame, source)
                    for name in peaks:
                        peaks[name] = max(peaks[name], len(owners[name]))
                    if obj["type"] == "packet":
                        counts[obj["stream"]] += 1
                        if obj["stream"] == 1:
                            oracle[obj["sequence"]] = digest(frame.raw)
                            if len(oracle) > 64:
                                oracle.popitem(last=False)
                    if "early" not in peers:
                        admission = source.join()
                        if admission is not None:
                            peers["early"] = Peer(admission)
                            origin = admission.snapshot.origin
                    if "early" in peers:
                        if peers["early"].admission.queue.detached:
                            raise ValueError("healthy live peer detached prematurely")
                        peers["early"].expect(frame)
                    if counts[0] >= 8:
                        for name in ("history", "holding"):
                            if name not in peers:
                                handoff = source.open(preceding=1)
                                if handoff is not None:
                                    peers[name] = HistoryPeer(handoff, oracle, hold=name == "holding",
                                        retained_event=retained_event if name == "holding" else None)
                            if name in peers and name == "history":
                                if peers[name].admission.queue.detached:
                                    raise ValueError("healthy history peer detached prematurely")
                                peers[name].expect(frame)
                    await asyncio.sleep(0)  # dispatch each publication, including bounded startup batches
                    for peer in peers.values():
                        if peer.task.done() and not peer.task.cancelled() and peer.task.exception() is not None:
                            raise peer.task.exception()
                    del frame
        except asyncio.CancelledError:
            terminal = "cancelled"
            raise
        except ValueError as error:
            failure = str(error)
            worker_alive = process is not None and process.returncode is None
            bytes_at_failure = source.history.bytes if source.history else 0
            if expected_error not in failure:
                raise
        finally:
            held = any(isinstance(p, HistoryPeer) and p.hold and not p.handoff.history.released
                       for p in peers.values())
            history = source.history
            try:
                try:
                    source.close(terminal)
                finally:
                    clock._abort()
                if history is not None and (history.bytes or history._bodies or history._handoffs or
                                            history.leased_bytes or history._timer is not None or
                                            history.index.queue.bytes or history.index.queue.inflight_bytes or
                                            history.index.queue._timer is not None):
                    raise ValueError("history retirement left managed state")
                await asyncio.wait_for(asyncio.gather(*(p.task for p in peers.values())), 2)
            finally:
                for peer in peers.values():
                    if not peer.task.done():
                        peer.task.cancel()
                try:
                    await asyncio.gather(*(p.task for p in peers.values()), return_exceptions=True)
                finally:
                    if process is not None:
                        await stop_process(process)
                if lifecycle is not None:
                    lifecycle.update(terminal=terminal, held_prefix_at_close=held,
                        worker_reaped=process is not None and process.returncode is not None,
                        peers_done=all(p.task.done() for p in peers.values()),
                        owned_bytes=history.bytes if history else 0,
                        queue_bytes=sum(p.admission.queue.bytes + p.admission.queue.inflight_bytes
                                        for p in peers.values()),
                        timer_cleared=history is not None and history._timer is None,
                        candidate_timer_cleared=source._gate._candidate_timer is None,
                        queue_timers_cleared=all(p.admission.queue._timer is None and
                                                p.admission.queue.next_deadline is None for p in peers.values()))
        if not peers or not all(counts) or process.returncode is None:
            raise ValueError("native history generation lacked A/V or worker cleanup")
        if ("history" in peers) != expect_history or ("holding" in peers) != expect_history:
            raise ValueError("unexpected native history admission outcome")
        if mode in ("clock-silent", "stall") and (not worker_alive or bytes_at_failure != 0):
            raise ValueError("idle history did not expire before live-worker clock failure")
        if expect_history:
            for name in ("history", "holding"):
                snapshot = peers[name].admission.snapshot
                if snapshot.first_sequence != 54 or snapshot.first_video_pts != 90000:
                    raise ValueError("history handoff changed native IDR position")
            if peers["holding"].reason != holding_terminal:
                raise ValueError("unexpected held native prefix terminal cause")
            if holding_terminal == "source_failed" and not held:
                raise ValueError("early EOF did not retire a still-held prefix")
        if any(p.reason != "source_failed" for p in peers.values() if not p.hold):
            raise ValueError("source failure did not retire healthy peers")
        if camera.connections != 1 or camera.played != 1:
            raise ValueError("history consumers created additional source connections")
        return {"generation": generation, "mode": mode, "packets": counts, "failure": failure,
                "peers": {name: p.summary() for name, p in peers.items()},
                "connections": camera.connections, "worker_reaped": True,
                "worker_running_at_failure": worker_alive, "owned_bytes_before_failure_close": bytes_at_failure,
                "peak_owned_bytes": history.peak_bytes, "peak_owned_bodies": history.peak_bodies,
                "closed_owned_bytes": history.bytes, "held_prefix_at_close": held,
                "peak_registered_owners": peaks,
                "elapsed_seconds": time.monotonic() - started}, origin


async def run(worker, seconds=8):
    integer(seconds, 3, 20)
    with tempfile.TemporaryDirectory(prefix="amcrust-native-history-") as temporary:
        root = Path(temporary)
        main_dir, high_dir = root / "main", root / "high"
        main_dir.mkdir()
        high_dir.mkdir()
        main = await make_media(main_dir, 320, 180, profile="main", duration=seconds + 1)
        high = await make_media(high_dir, 352, 198, profile="high")
        sps = next(nal for nal in high[0][0] if nal[0] & 31 == 7)
        owners = {name: weakref.WeakValueDictionary() for name in ("frames", "ranges", "bodies")}
        results, origins = [], []
        cases = ((main, "disconnect", seconds, "native source EOF", True, None, "residence_expired"),
                 (high, "clock-silent", 2.2, "sender report freshness expired", False, None, None),
                 (high, "disconnect", 2.2, "native source EOF", True, None, "residence_expired"),
                 (high, "stall", 2.2, "sender report freshness expired", True, None, "residence_expired"),
                 (main, "parameter-change", 3.2, "parameter sets changed", True, sps, "residence_expired"),
                 (main, "disconnect", 1.2, "native source EOF", True, None, "source_failed"))
        for generation, (media, mode, duration, error, late, replacement, hold_end) in enumerate(cases, 1):
            result, origin = await generation_case(worker, media, generation, mode=mode, seconds=duration,
                expected_error=error, expect_history=late, replacement_sps=replacement, owners=owners,
                holding_terminal=hold_end)
            if any(origin is old for old in origins):
                raise ValueError("native history restart reused source identity")
            origins.append(origin)
            await asyncio.sleep(0)
            gc.collect()
            if any(owners.values()):
                raise ValueError("retired generation retained registered history/frame/range owners")
            expected = [100, 352, 198] if generation in (2, 3, 4) else [77, 320, 180]
            if any(p["video_config"] != expected for p in result["peers"].values()):
                raise ValueError("history restart reused retired codec configuration")
            results.append(result)
        if results[0]["packets"][0] < (seconds - 1) * 15 or results[4]["packets"][0] < 20:
            raise ValueError("paced history/configuration cases ended too early")
        ready, cancellation = asyncio.Event(), {}
        task = asyncio.create_task(generation_case(worker, main, 7, mode="stall", seconds=2.2,
            expected_error="", owners=owners, retained_event=ready, lifecycle=cancellation))
        try:
            await asyncio.wait_for(ready.wait(), 5)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise ValueError("history source cancellation was swallowed")
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if cancellation != dict(terminal="cancelled", held_prefix_at_close=True, worker_reaped=True,
                                peers_done=True, owned_bytes=0, queue_bytes=0, timer_cleared=True,
                                candidate_timer_cleared=True, queue_timers_cleared=True):
            raise ValueError("native history cancellation left active ownership")
        del task
        await asyncio.sleep(0)
        gc.collect()
        if any(owners.values()):
            raise ValueError("cancelled history generation retained registered owners")
        return {"cases": results, "cancellation": cancellation,
                "final_registered_owners": {name: len(values) for name, values in owners.items()},
                "scope": "paced synthetic native history lifecycle; bounded hash oracle, no accumulated media, decoder/performance or Apple claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--seconds", type=int, default=8)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.worker.resolve(), args.seconds)), indent=2))


if __name__ == "__main__":
    main()
