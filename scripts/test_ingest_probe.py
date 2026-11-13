"""Fast contract tests; no FFmpeg or camera needed."""

import asyncio
import os
from pathlib import Path
import sys
import tempfile
import unittest

from ingest_probe import CHUNK_BYTES, Subscriber, command, compare_packets
from ingest_baseline import descendants, local_metrics_url


class FanoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_subscriber_detaches_without_affecting_fast_subscriber(self):
        fast, slow = Subscriber(2), Subscriber(1)
        chunks = [b"a" * CHUNK_BYTES, b"b" * CHUNK_BYTES]
        for chunk in chunks:
            fast.offer(chunk)
            slow.offer(chunk)
        self.assertTrue(slow.detached)
        self.assertFalse(fast.detached)
        self.assertEqual(slow.queued_bytes, 0)
        self.assertLessEqual(slow.peak_bytes, CHUNK_BYTES)
        self.assertEqual(await fast.receive(), chunks[0])
        self.assertEqual(await fast.receive(), chunks[1])
        self.assertEqual(fast.queued_bytes, 0)
        slow.offer(b"ignored")
        self.assertTrue(slow.queue.empty())

    async def test_queue_reuses_capacity_after_receive(self):
        subscriber = Subscriber(1)
        subscriber.offer(b"one")
        self.assertEqual(await subscriber.receive(), b"one")
        subscriber.offer(b"two")
        self.assertFalse(subscriber.detached)
        self.assertEqual(await subscriber.receive(), b"two")

    async def test_unbounded_capacity_is_rejected(self):
        for capacity in (0, -1):
            with self.assertRaises(ValueError):
                Subscriber(capacity)

    async def test_oversized_chunk_is_rejected(self):
        with self.assertRaises(ValueError):
            Subscriber(1).offer(b"x" * (CHUNK_BYTES + 1))


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_worker_returns_error(self):
        with self.assertRaisesRegex(RuntimeError, "worker failed"):
            await command(sys.executable, "-c", "import sys; sys.stderr.write('worker failed'); sys.exit(1)")

    async def test_timeout_reaps_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            pidfile = Path(directory) / "pid"
            code = "import os, pathlib, sys, time; pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(60)"
            with self.assertRaises(asyncio.TimeoutError):
                await command(sys.executable, "-c", code, str(pidfile), timeout=1)
            pid = int(pidfile.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)


class BaselineTests(unittest.TestCase):
    def test_process_tree_excludes_unrelated_processes(self):
        rows = [{"pid": 3, "ppid": 2}, {"pid": 2, "ppid": 1},
                {"pid": 1, "ppid": 0}, {"pid": 4, "ppid": 0}]
        self.assertEqual({r["pid"] for r in descendants(rows, 1)}, {1, 2, 3})

    def test_metrics_url_requires_local_no_credentials(self):
        import argparse
        self.assertEqual(local_metrics_url("http://127.0.0.1:9000/metrics"),
                         "http://127.0.0.1:9000/metrics")
        for url in ("http://example.com/metrics", "http://user:secret@localhost/metrics",
                    "http://localhost/metrics?secret=yes", "file:///metrics"):
            with self.assertRaises(argparse.ArgumentTypeError):
                local_metrics_url(url)


class ComparisonTests(unittest.TestCase):
    def fixture(self, shift=0, missing_dts=False, audio_shift=None):
        streams = [{"index": 0, "codec_type": "video"},
                   {"index": 1, "codec_type": "audio"}]
        packets = []
        for stream in range(2):
            offset = audio_shift if stream == 1 and audio_shift is not None else shift
            for n in range(2):
                packet = {"stream_index": stream, "data_hash": f"{stream}-{n}",
                          "pts_time": str(n + offset), "duration_time": "1"}
                if not missing_dts:
                    packet["dts_time"] = str(n + offset)
                packets.append(packet)
        return {"streams": streams, "packets": packets}

    def test_common_epoch_shift_preserves_timing(self):
        report = compare_packets(self.fixture(), self.fixture(shift=5))
        self.assertTrue(report["common_av_pts_shift"])
        self.assertTrue(report["video"]["same_payloads"])
        self.assertEqual(report["audio"]["timing"]["pts_time"]["shift_seconds"], 5)

    def test_missing_dts_is_not_silently_replaced(self):
        report = compare_packets(self.fixture(), self.fixture(missing_dts=True))
        self.assertFalse(report["video"]["timing"]["dts_time"]["available_for_all_packets"])

    def test_audio_epoch_drift_is_reported(self):
        report = compare_packets(self.fixture(), self.fixture(shift=1, audio_shift=2))
        self.assertFalse(report["common_av_pts_shift"])

    def test_packet_loss_is_reported(self):
        output = self.fixture()
        output["packets"].pop()
        report = compare_packets(self.fixture(), output)
        self.assertFalse(report["audio"]["same_payloads"])
        self.assertFalse(report["common_av_pts_shift"])


if __name__ == "__main__":
    unittest.main()
