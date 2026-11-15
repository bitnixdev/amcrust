import asyncio
import gc
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from ingest_probe import stop_process

from recording_cut import RecordingCutInput
from recording_output import RecordingOutput
from test_audio_history import setup


class Receiver:
    def __init__(self, block=None, fail=False):
        self.block, self.fail = block, fail
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.received, self.finished = bytearray(), False

    async def write(self, chunk):
        self.received.extend(chunk)  # deliberately escaped copy, outside job ownership
        if self.fail:
            raise OSError("receiver failed")
        if self.block == "write":
            self.entered.set()
            await self.release.wait()

    async def finish(self):
        if self.block == "finish":
            self.entered.set()
            await self.release.wait()
        self.finished = True


class RecordingOutputTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.loop = asyncio.get_running_loop()
        self.errors = []
        self.previous_handler = self.loop.get_exception_handler()
        self.loop.set_exception_handler(lambda loop, context: self.errors.append(context))
        self.temporary = tempfile.TemporaryDirectory()
        self.sink = Path(self.temporary.name) / "sink"
        self.source = await setup()
        h = self.source.open(preceding=1)
        self.recorder = RecordingCutInput(h)
        with await h.receive() as d:
            self.recorder.feed(d)
        self.recorder.prepare_cut()

    async def asyncTearDown(self):
        self.source.close()
        self.temporary.cleanup()
        gc.collect()
        await asyncio.sleep(0)
        self.loop.set_exception_handler(self.previous_handler)
        self.assertEqual(self.errors, [], "unretrieved async pipe/task exception")

    def sink_script(self, body):
        self.sink.write_text(f"#!{sys.executable}\nimport sys, time\nfrom pathlib import Path\n" + body)
        self.sink.chmod(0o700)

    def normal_sink(self):
        self.sink_script("sys.stdin.buffer.read()\nPath(sys.argv[1]).write_bytes(b'abcd' * 1000)\n")

    def assert_clean(self, job):
        self.assertEqual(job.input_bytes, 0)
        self.assertTrue(job.worker_reaped)
        self.assertFalse(job.path.exists())
        self.assertFalse(job.path.parent.exists())
        self.assertIsNone(job._receiver)

    async def test_success_exact_chunks_and_one_use(self):
        self.normal_sink()
        receiver = Receiver()
        job = RecordingOutput(self.recorder, self.sink, receiver, chunk_bytes=123)
        await job.run()
        self.assertEqual(receiver.received, b'abcd' * 1000)
        self.assertEqual(job.bytes_sent, 4000)
        self.assertTrue(receiver.finished)
        self.assertEqual(job.state, "complete")
        self.assert_clean(job)
        job.cancel("privacy")
        self.assertEqual(job.state, "complete")
        with self.assertRaises(ValueError):
            await job.run()

    async def test_cancel_before_start_drops_input(self):
        job = RecordingOutput(self.recorder, self.sink, Receiver())
        self.assertGreater(job.input_bytes, 0)
        job.cancel("privacy")
        job.cancel("later")
        with self.assertRaises(ValueError):
            await job.run()
        self.assertEqual(job.reason, "privacy")
        self.assertEqual(job.input_bytes, 0)
        self.assertIsNone(job.path)
        self.assertIsNone(job._receiver)

    async def test_cancel_blocked_write_and_finish(self):
        self.normal_sink()
        # Each test setup has one prepared cut; exercise finish separately below.
        receiver = Receiver(block="write")
        job = RecordingOutput(self.recorder, self.sink, receiver, chunk_bytes=128)
        task = asyncio.create_task(job.run())
        await asyncio.wait_for(receiver.entered.wait(), 3)
        job.cancel("privacy")
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(receiver.received), 128)  # accepted bytes cannot be revoked
        self.assertEqual(job.writes, 0)  # callback did not acknowledge completion
        self.assertFalse(receiver.finished)
        self.assertEqual(job.reason, "privacy")
        self.assert_clean(job)

    async def test_cancel_during_finish_prevents_success_result(self):
        self.normal_sink()
        receiver = Receiver(block="finish")
        job = RecordingOutput(self.recorder, self.sink, receiver)
        task = asyncio.create_task(job.run())
        await asyncio.wait_for(receiver.entered.wait(), 3)
        task.cancel()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(receiver.finished)
        self.assertEqual(job.state, "cancelled")
        self.assert_clean(job)

    async def test_timeout_during_mux_reaps_and_removes_partial(self):
        self.sink_script("Path(sys.argv[1]).write_bytes(b'partial')\ntime.sleep(30)\n")
        job = RecordingOutput(self.recorder, self.sink, Receiver(), timeout=0.3)
        with self.assertRaises(asyncio.TimeoutError):
            await job.run()
        self.assertEqual(job.reason, "timeout")
        self.assert_clean(job)

    async def test_timeout_during_backpressure(self):
        self.normal_sink()
        receiver = Receiver(block="write")
        job = RecordingOutput(self.recorder, self.sink, receiver, timeout=0.3)
        with self.assertRaises(asyncio.TimeoutError):
            await job.run()
        self.assertFalse(receiver.finished)
        self.assertEqual(job.reason, "timeout")
        self.assert_clean(job)

    async def test_cancel_blocked_mux_input_reaps_partial(self):
        self.sink_script("Path(sys.argv[1]).write_bytes(b'partial')\ntime.sleep(30)\n")
        job = RecordingOutput(self.recorder, self.sink, Receiver())
        job._wire = bytes(1024 * 1024)  # synthetic pipe pressure, not a codec fixture
        task = asyncio.create_task(job.run())
        async def blocked():
            while (job._process is None or not job.path.exists() or
                   not job._process.stdin.transport.get_write_buffer_size()):
                await asyncio.sleep(0.005)
        await asyncio.wait_for(blocked(), 3)
        job.cancel("privacy")
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(job.reason, "privacy")
        self.assert_clean(job)

    async def test_cancellation_during_spawn_collects_process_handle(self):
        self.normal_sink()
        created, release = asyncio.Event(), asyncio.Event()
        spawn = asyncio.create_subprocess_exec
        async def held_spawn(*args, **kwargs):
            process = await spawn(*args, **kwargs)
            created.set()
            await release.wait()
            return process
        job = RecordingOutput(self.recorder, self.sink, Receiver())
        with patch("recording_output.asyncio.create_subprocess_exec", held_spawn):
            task = asyncio.create_task(job.run())
            try:
                await asyncio.wait_for(created.wait(), 3)
                job.cancel("privacy")
                await asyncio.sleep(0)
                self.assertFalse(task.done())
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                release.set()
                job.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(job.reason, "privacy")
        self.assert_clean(job)

    async def test_repeated_caller_cancellation_during_cleanup(self):
        self.normal_sink()
        entered, release = asyncio.Event(), asyncio.Event()
        async def held_cleanup(process):
            await stop_process(process)
            entered.set()
            await release.wait()
        receiver = Receiver(block="write")
        job = RecordingOutput(self.recorder, self.sink, receiver)
        with patch("recording_output.stop_process", held_cleanup):
            task = asyncio.create_task(job.run())
            await asyncio.wait_for(receiver.entered.wait(), 3)
            job.cancel("privacy")
            await asyncio.wait_for(entered.wait(), 3)
            for _ in range(2):
                task.cancel()
                await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(job.reason, "privacy")
        self.assert_clean(job)

    async def test_output_size_is_os_limited_and_not_delivered(self):
        self.normal_sink()
        receiver = Receiver()
        job = RecordingOutput(self.recorder, self.sink, receiver, max_output_bytes=64)
        with self.assertRaises((ValueError, BrokenPipeError, ConnectionResetError)):
            await job.run()
        self.assertEqual(job.reason, "output_failed")
        self.assertFalse(receiver.received)
        self.assertFalse(receiver.finished)
        self.assert_clean(job)

    async def test_receiver_failure_cleans_and_preserves_no_success(self):
        self.normal_sink()
        receiver = Receiver(fail=True)
        job = RecordingOutput(self.recorder, self.sink, receiver)
        with self.assertRaisesRegex(OSError, "receiver failed"):
            await job.run()
        self.assertFalse(receiver.finished)
        self.assertEqual(job.reason, "output_failed")
        self.assert_clean(job)

    async def test_input_budget_checked_before_transfer_or_packing(self):
        self.recorder._bytes = 4 * 1024 * 1024  # plus framing/end exceeds job allowance
        with patch("recording_output.pack_wire") as pack:
            with self.assertRaisesRegex(ValueError, "input budget"):
                RecordingOutput(self.recorder, self.sink, Receiver())
            pack.assert_not_called()
        self.assertEqual(self.recorder.state, "prepared")

    async def test_invalid_limits_do_not_commit_input(self):
        for options in ({"timeout": float("nan")}, {"timeout": True}, {"chunk_bytes": 0},
                        {"max_output_bytes": 0}):
            with self.assertRaises(ValueError):
                RecordingOutput(self.recorder, self.sink, Receiver(), **options)
        self.assertEqual(self.recorder.state, "prepared")
