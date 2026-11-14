"""Small deterministic checks for the instrumented synthetic load harness."""
import asyncio
import gc
import tracemalloc
import unittest

from fanout_load_probe import Fixture, Owners, run, validate_options
from packet_ingest_probe import SharedSource


class LoadProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_counts_isolation_sampling_and_reclamation(self):
        result = await run(packets=128, subscribers=3, payload_bytes=1024)
        self.assertEqual(result["consumer_counts"], [128] * 3)
        self.assertEqual(result["source_parses"], 131)
        self.assertEqual(result["stalled_reason"], "overflow")
        self.assertEqual(result["stalled_limit"], "media")
        self.assertTrue(all(value == 0 for value in result["final_owners"].values()))
        self.assertLessEqual(len(result["samples"]), 16)
        self.assertTrue(any(sample["views"] == 1 and sample["queued_wire_bytes"] == 0
                            for sample in result["samples"]))
        late = [sample for sample in result["samples"] if sample["packets"] > 68]
        self.assertTrue(all(sample["frames"] == 0 and sample["views"] == 0 and
                            sample["ranges"] == 1 for sample in late))
        self.assertFalse(tracemalloc.is_tracing())

    async def test_cancel_restores_tracer_and_reaps_consumers(self):
        before = asyncio.all_tasks()
        task = asyncio.create_task(run(packets=100000, subscribers=2, payload_bytes=256))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(tracemalloc.is_tracing())
        self.assertFalse(asyncio.all_tasks() - before)

    async def test_does_not_take_over_existing_tracer(self):
        tracemalloc.start()
        try:
            with self.assertRaises(ValueError):
                await run(packets=64)
            self.assertTrue(tracemalloc.is_tracing())
        finally:
            tracemalloc.stop()

    def test_tracking_does_not_retain_publications_or_ranges(self):
        source, owners, fixture = SharedSource(), Owners(), Fixture(256)
        for raw in fixture.startup():
            frame = source.accept(raw)
            owners.publish(frame)
        frame = source.accept(fixture.packet(0))
        owners.publish(frame)
        view = frame.metadata["data"].view()
        owners.view(view)
        del frame, raw, source
        gc.collect()
        self.assertEqual(owners.snapshot()["frames"], 0)
        self.assertEqual(owners.snapshot()["ranges"], 0)
        self.assertEqual(owners.snapshot()["views"], 1)
        view.release()
        del view
        self.assertEqual(owners.snapshot()["wire_bytes"], 0)

    def test_options_are_bounded_before_allocation(self):
        for options in ((63, 1, 256, 1), (1000001, 1, 256, 1), (64, 9, 256, 1),
                        (64, 1, 262145, 1), (64, 1, 256, 5), (64, True, 256, 1)):
            with self.assertRaises(ValueError):
                validate_options(*options)
