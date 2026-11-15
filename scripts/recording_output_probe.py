#!/usr/bin/env python3
"""Concurrent native recording mux/delivery with cooperative cancellation/fault controls."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import tempfile

from av_output_probe import decoded_video
from ingest_probe import inspect
from recording_cut_probe import case
from recording_output import RecordingOutput
from rtsp_fixture import make_media


class Receiver:
    def __init__(self, mode, reference):
        self.mode, self.reference = mode, reference
        self.hash = hashlib.sha256()
        self.bytes = self.calls = self.finish_calls = 0
        self.completed = False
        self.job = self.status = None
        self.first_write_status = self.completed_status = None

    async def write(self, chunk):
        if self.first_write_status is None:
            self.first_write_status = self.status()
        self.hash.update(chunk)
        self.calls += 1
        self.bytes += len(chunk)
        if self.mode == "cancel-write":
            self.job.cancel("privacy")
            await asyncio.sleep(0)
        if self.mode in ("timeout", "source-failure"):
            await asyncio.Event().wait()
        if self.mode == "write-error":
            raise OSError("synthetic receiver failure")
        await asyncio.sleep(0.005)  # bounded cooperative backpressure, no packet accumulation

    async def finish(self):
        self.finish_calls += 1
        if self.mode == "cancel-finish":
            self.job.cancel("privacy")
            await asyncio.sleep(0)
        expected = hashlib.sha256()
        with self.job.path.open("rb") as output:
            while True:
                chunk = output.read(65536)
                if not chunk:
                    break
                expected.update(chunk)
        if self.hash.digest() != expected.digest() or self.bytes != self.job.path.stat().st_size:
            raise ValueError("delivered bytes differ from owned mux file")
        info = await inspect(self.job.path)
        counts = [sum(p["stream_index"] == index for p in info["packets"]) for index in (0, 1)]
        if counts != [9, 30] or await decoded_video(self.job.path) != self.reference:
            raise ValueError("concurrent recording output changed selected media")
        self.completed_status = self.status()
        if (not all(self.completed_status[1:]) or
                self.completed_status[0] <= self.first_write_status[0]):
            raise ValueError("source did not progress while output was delivered/decoded")
        self.completed = True


async def run(worker, sink):
    with tempfile.TemporaryDirectory(prefix="amcrust-output-proof-") as temporary:
        root = Path(temporary)
        media = await make_media(root, 320, 180, profile="main")
        reference = (await decoded_video(root / "320.h264"))[15:24]
        results = []
        for generation, mode in enumerate(("success", "cancel-write", "cancel-finish", "timeout",
                                           "write-error", "file-limit", "source-failure"), 1):
            receiver = Receiver(mode, reference)
            def factory(recorder, status):
                receiver.status = status
                receiver.job = RecordingOutput(recorder, sink, receiver, chunk_bytes=4096,
                    timeout=0.5 if mode == "timeout" else 3,
                    max_output_bytes=64 if mode == "file-limit" else 8 * 1024 * 1024)
                return receiver.job
            result = await case(worker, sink, root, media, generation, 0.25, "commit", output_factory=factory,
                                send_seconds=2.2 if mode == "source-failure" else 3.2)
            job = receiver.job
            if (job.input_bytes or not job.worker_reaped or job.path.exists() or job.path.parent.exists() or
                    job._receiver is not None or job._task is not None or
                    result["later_source_packets"] != (37 if mode == "source-failure" else 99)):
                raise ValueError("concurrent output cleanup/continued ingest failed")
            expected_reason = {"success": None, "cancel-write": "privacy", "cancel-finish": "privacy",
                               "timeout": "timeout", "write-error": "output_failed", "file-limit": "output_failed",
                               "source-failure": "source_failed"}[mode]
            if job.reason != expected_reason or (job.state == "complete") != (mode == "success"):
                raise ValueError("unexpected concurrent output outcome")
            if receiver.completed != (mode == "success"):
                raise ValueError("cancelled/failed output reported receiver completion")
            if mode not in ("success", "cancel-finish") and receiver.finish_calls:
                raise ValueError("failed output invoked finish")
            if mode == "source-failure" and receiver.calls != 1:
                raise ValueError("source failure did not cancel a genuinely pending receiver write")
            if mode == "file-limit" and receiver.bytes:
                raise ValueError("failed mux delivered partial bytes")
            result.update(mode=mode, accepted_bytes=receiver.bytes, acknowledged_bytes=job.bytes_sent,
                          write_calls=receiver.calls, finish_calls=receiver.finish_calls,
                          receiver_completed=receiver.completed, output_worker_reaped=job.worker_reaped,
                          managed_input_bytes=job.input_bytes, output_removed=True,
                          first_write_source_status=receiver.first_write_status,
                          completed_source_status=receiver.completed_status)
            results.append(result)
        return {"cases": results,
                "scope": "concurrent loopback mux and cooperative receiver only; no HDS/iCloud, alias erasure or Apple acceptance"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--sink", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.worker.resolve(), args.sink.resolve())), indent=2))


if __name__ == "__main__":
    main()
