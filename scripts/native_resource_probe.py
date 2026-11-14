#!/usr/bin/env python3
"""Matched shared/independent native acquisition; synthetic loopback only.

Fixture generation is in a separate process and outside measured CPU intervals.
RSS is sampled accounting, not unique physical memory or a true aggregate peak.
"""
import argparse
import asyncio
import hashlib
from fractions import Fraction
import json
import math
import os
from pathlib import Path
import platform
import signal
import struct
import sys
import tempfile
import time

from ingest_probe import stop_process
from native_fanout_probe import Peer
from packet_ingest_probe import QueueClosed, integer, read_frame
from packet_wire import encode_frame, inspect_metadata
from rtsp_clock import ClockNormalizer
from rtsp_fixture import CameraFixture, make_media
from shared_admission import LateAdmission


def resource_report(data):
    if len(data) > 65536:
        raise ValueError("native resource log exceeds bound")
    lines = data.decode("utf-8").splitlines()
    records = [json.loads(line[len("AMC_RESOURCE "):]) for line in lines if line.startswith("AMC_RESOURCE ")]
    if len(records) != 1 or len(lines) != 2 or lines[0] != "packet worker: input read failed":
        raise ValueError("missing/ambiguous native resource report or unexpected exit")
    result = records[0]
    if set(result) != {"user_cpu_seconds", "system_cpu_seconds", "peak_rss_bytes"}:
        raise ValueError("unexpected native resource fields")
    for key in ("user_cpu_seconds", "system_cpu_seconds"):
        if type(result[key]) not in (int, float) or not math.isfinite(result[key]) or result[key] < 0:
            raise ValueError("invalid native CPU report")
    integer(result["peak_rss_bytes"], 1)
    return result


def rss_rows(text, allowed):
    rows = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 2:
            raise ValueError("unexpected ps columns")
        pid, kib = map(int, fields)
        if pid not in allowed or pid in rows or kib < 0:
            raise ValueError("unexpected ps identity/RSS")
        rows[pid] = kib * 1024
    return rows


async def rss_snapshot(pids):
    # Darwin's OS-provided ps is not on a Nix build's PATH; Linux uses repo procps.
    program = "/bin/ps" if sys.platform == "darwin" else "ps"
    process = await asyncio.create_subprocess_exec(program, "-p", ",".join(map(str, pids)),
        "-o", "pid=,rss=", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    try:
        output, _ = await asyncio.wait_for(process.communicate(), 3)
        if process.returncode != 0:
            raise ValueError("RSS sampler failed")
        return rss_rows(output.decode(), set(pids))
    finally:
        if process.returncode is None:
            await stop_process(process)


async def fixture(seconds):
    with tempfile.TemporaryDirectory(prefix="amcrust-resource-input-") as directory:
        media = await make_media(directory, 320, 180, profile="main", duration=seconds + 1)
        start = time.process_time()
        async with CameraFixture(media, "disconnect", clocked=True, audio_offset=0.25,
                                 staggered_reports=True, send_seconds=seconds) as camera:
            print(json.dumps({"port": camera.port}), flush=True)
            reader = asyncio.StreamReader()
            transport, _ = await asyncio.get_running_loop().connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
            try:
                if await reader.readline() != b"stop\n":
                    raise ValueError("fixture control closed unexpectedly")
            finally:
                transport.close()
        print(json.dumps({"cpu_seconds": time.process_time() - start,
                          "connections": camera.connections, "played": camera.played}), flush=True)


class MeteredPeer(Peer):
    def __init__(self, admission):
        self.hash_release_cpu = 0.0
        self.workload = hashlib.sha256()
        # Same codec/epoch contract across acquisitions; generation IDs may differ.
        config = [inspect_metadata(admission.snapshot.hello)] + [
            inspect_metadata(stream) for stream in admission.snapshot.streams]
        for obj in config:
            obj.pop("generation")
        self.workload.update(json.dumps(config, sort_keys=True, separators=(",", ":")).encode())
        super().__init__(admission)

    def summary(self):
        return {**super().summary(), "workload_sha256": self.workload.hexdigest()}

    async def consume(self):
        try:
            while True:
                delivery = await self.admission.queue.receive()
                start = time.process_time()
                try:
                    self.actual.update(delivery.frame.raw)
                    packet = delivery.frame.metadata
                    with packet["data"].view() as data:
                        self.workload.update(struct.pack("!QqqqqqI", packet["sequence"], packet["stream"],
                            packet["pts"], packet["dts"], packet["duration"], packet["flags"], len(data)))
                        self.workload.update(data)
                    del packet
                    self.received += 1
                finally:
                    delivery.release()
                    self.hash_release_cpu += time.process_time() - start
        except QueueClosed as error:
            self.reason = error.reason


async def acquire(worker, port, generation, subscribers, seconds, processes):
    clock = ClockNormalizer(generation, report_timeout=Fraction(5, 4))
    gate = LateAdmission(generation)
    peers = []
    counts = [0, 0]
    costs = dict(normalization_cpu_seconds=0.0, publication_cpu_seconds=0.0,
                 oracle_hash_cpu_seconds=0.0)
    process = None
    with tempfile.TemporaryFile() as errors:
        try:
            process = await asyncio.create_subprocess_exec(str(worker), "--rtsp-resource-fixture",
                str(port), str(generation), stdout=asyncio.subprocess.PIPE, stderr=errors)
            processes.append(process)
            while True:
                clock.expire()
                try:
                    raw = await asyncio.wait_for(read_frame(process.stdout),
                        max(0.001, min(2.5, clock.next_deadline - time.monotonic())))
                except asyncio.TimeoutError:
                    clock.expire()
                    raise ValueError("resource source read timeout")
                if raw is None:
                    break
                start = time.process_time()
                batch = clock.feed(raw)
                costs["normalization_cpu_seconds"] += time.process_time() - start
                for obj in batch:
                    if obj["type"] == "end":
                        raise ValueError("live source emitted finite end")
                    start = time.process_time()
                    frame = gate.feed(encode_frame(obj))
                    if obj["type"] == "packet":
                        counts[obj["stream"]] += 1
                    if not peers:
                        admission = gate.join()
                        if admission is not None:
                            peers.append(MeteredPeer(admission))
                            for _ in range(subscribers - 1):
                                peers.append(MeteredPeer(gate.join()))
                    costs["publication_cpu_seconds"] += time.process_time() - start
                    start = time.process_time()
                    for peer in peers:
                        if peer.admission.queue.detached or peer.task.done():
                            raise ValueError("resource consumer stopped early")
                        peer.expect(frame)
                    costs["oracle_hash_cpu_seconds"] += time.process_time() - start
                    await asyncio.sleep(0)
                    del frame
            # EOF is a source failure even in a successful measurement. Retire
            # subscriber state before awaiting exit/report collection.
            gate.close("source_failed")
            clock._abort()
            await asyncio.wait_for(process.wait(), 2)
            if process.returncode != 1:
                raise ValueError("unexpected native live EOF exit code")
            errors.seek(0)
            native = resource_report(errors.read(65537))
        finally:
            gate.close("source_failed")
            clock._abort()
            try:
                await asyncio.wait_for(asyncio.gather(*(peer.task for peer in peers)), 2)
            finally:
                for peer in peers:
                    if not peer.task.done():
                        peer.task.cancel()
                await asyncio.gather(*(peer.task for peer in peers), return_exceptions=True)
                if process is not None:
                    await stop_process(process)
        expected = [seconds * 15, math.ceil((seconds - 0.25) * 48000 / 1024)]
        if counts != expected or len(peers) != subscribers:
            raise ValueError("unmatched resource workload: missing source packets/subscribers")
        summaries = [peer.summary() for peer in peers]
        if any(item["received"] != sum(counts) or item["first_sequence"] != 0 or
               item["terminal"] != "source_failed" for item in summaries):
            raise ValueError("unmatched resource workload: incomplete consumer")
        costs["consumer_hash_release_cpu_seconds"] = sum(peer.hash_release_cpu for peer in peers)
        return {"native": native, "packets": counts, "consumers": summaries, "publisher_subsets": costs}


async def sample(stop, fixture_pid, processes, expected_workers):
    result = dict(enabled=True, samples=0, full_worker_samples=0, publisher_rss_sample_peak_bytes=0,
                  fixture_rss_sample_peak_bytes=0, worker_rss_sum_sample_peak_bytes=0)
    while not stop.is_set():
        workers = [process.pid for process in processes if process.returncode is None]
        rows = await rss_snapshot([os.getpid(), fixture_pid, *workers])
        if os.getpid() not in rows or fixture_pid not in rows:
            raise ValueError("resource sampler lost publisher/fixture")
        result["samples"] += 1
        if len(workers) == expected_workers and all(pid in rows for pid in workers):
            result["full_worker_samples"] += 1
        result["publisher_rss_sample_peak_bytes"] = max(result["publisher_rss_sample_peak_bytes"], rows[os.getpid()])
        result["fixture_rss_sample_peak_bytes"] = max(result["fixture_rss_sample_peak_bytes"], rows[fixture_pid])
        result["worker_rss_sum_sample_peak_bytes"] = max(result["worker_rss_sum_sample_peak_bytes"],
                                                       sum(rows.get(pid, 0) for pid in workers))
        try:
            await asyncio.wait_for(stop.wait(), 0.25)
        except asyncio.TimeoutError:
            pass
    if not result["full_worker_samples"]:
        raise ValueError("no RSS snapshot covered all native workers")
    return result


async def run(worker, mode, subscribers, seconds, sample_memory=True):
    integer(subscribers, 1, 8)
    integer(seconds, 3, 20)
    if mode not in ("shared", "independent"):
        raise ValueError("invalid acquisition mode")
    fixture_process = await asyncio.create_subprocess_exec(sys.executable, "-B", str(Path(__file__).resolve()),
        "--fixture", "--seconds", str(seconds), stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, start_new_session=True)
    tasks, processes = [], []
    sampler = None
    stop = asyncio.Event()
    try:
        ready = json.loads(await asyncio.wait_for(fixture_process.stdout.readline(), 30))
        port = integer(ready["port"], 1, 65535)
        workers = 1 if mode == "shared" else subscribers
        start_cpu, start_wall = time.process_time(), time.monotonic()
        tasks = [asyncio.create_task(acquire(worker, port, i + 1,
                 subscribers if mode == "shared" else 1, seconds, processes)) for i in range(workers)]
        if sample_memory:
            sampler = asyncio.create_task(sample(stop, fixture_process.pid, processes, workers))
        results = await asyncio.gather(*tasks)
        stop.set()
        rss = await sampler if sampler is not None else {"enabled": False, "reason": "explicitly disabled"}
        publisher_cpu, elapsed = time.process_time() - start_cpu, time.monotonic() - start_wall
        fixture_process.stdin.write(b"stop\n")
        await fixture_process.stdin.drain()
        fixture_stats = json.loads(await asyncio.wait_for(fixture_process.stdout.readline(), 5))
        await asyncio.wait_for(fixture_process.wait(), 5)
        if fixture_process.returncode != 0 or fixture_stats["connections"] != workers or fixture_stats["played"] != workers:
            raise ValueError("fixture did not run the matched number of acquisitions")
        fingerprints = {peer["workload_sha256"] for result in results for peer in result["consumers"]}
        if len(fingerprints) != 1:
            raise ValueError("acquisitions did not deliver identical codec/epoch/payload/clock workloads")
        return {"mode": mode, "subscribers": subscribers, "seconds": seconds, "workers": workers,
                "workload_sha256": fingerprints.pop(),
                "environment": {"system": platform.system(), "machine": platform.machine(),
                                "python": platform.python_version()},
                "native_cpu_seconds": sum(item["native"]["user_cpu_seconds"] + item["native"]["system_cpu_seconds"] for item in results),
                "native_individual_peak_rss_bytes": [item["native"]["peak_rss_bytes"] for item in results],
                "publisher_cpu_seconds": publisher_cpu, "fixture": fixture_stats,
                "publisher_subsets": {key: sum(item["publisher_subsets"][key] for item in results)
                                      for key in results[0]["publisher_subsets"]},
                "elapsed_seconds": elapsed, "rss": rss, "sources": results,
                "scope": "synthetic matched work; CPU components overlap with publisher total; sampled RSS is not unique memory or true peak"}
    finally:
        stop.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if sampler is not None:
            if not sampler.done():
                sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
        if fixture_process.returncode != 0:
            # The isolated fixture session also owns input-generation FFmpeg children.
            # Its leader may already have failed while a generator is still alive.
            # Stop them on startup failure/cancellation rather than orphaning them.
            try:
                os.killpg(fixture_process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        await stop_process(fixture_process)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker", type=Path)
    parser.add_argument("--mode", choices=("shared", "independent"), default="shared")
    parser.add_argument("--subscribers", type=int, default=2)
    parser.add_argument("--seconds", type=int, default=6)
    parser.add_argument("--no-rss", action="store_true", help="explicit CPU-only mode for restricted build sandboxes")
    args = parser.parse_args()
    integer(args.seconds, 3, 20)
    if args.fixture:
        asyncio.run(fixture(args.seconds))
    else:
        if args.worker is None:
            parser.error("--worker is required")
        print(json.dumps(asyncio.run(run(args.worker.resolve(), args.mode, args.subscribers, args.seconds, not args.no_rss)), indent=2))


if __name__ == "__main__":
    main()
