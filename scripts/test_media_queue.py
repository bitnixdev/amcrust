"""Exact media-span accounting after clock admission; no FFmpeg dependency."""
import asyncio
from fractions import Fraction
import unittest

from packet_ingest_probe import MediaQueue, QueueClosed
from test_packet_probe import encode, hello, packet, stream


class MediaQueueTests(unittest.IsolatedAsyncioTestCase):
    async def make_queue(self, budget=Fraction(1, 2), bases=((1, 10),), **kwargs):
        queue = MediaQueue(100000, budget, **kwargs)
        queue.offer(encode({**hello(), "streams": len(bases)}))
        for index, base in enumerate(bases):
            queue.offer(encode({**stream(), "index": index, "time_base": list(base)}))
        for _ in range(len(bases) + 1):
            await queue.receive()
        return queue

    def offer(self, queue, pts, dts=None, duration=1, track=0, **overrides):
        raw = encode({**packet(), "pts": pts, "dts": pts if dts is None else dts,
                      "duration": duration, "stream": track,
                      "sequence": queue.protocol.sequence, **overrides})
        return queue.offer(raw)

    async def test_exact_fractional_boundary_and_negative_origin(self):
        queue = await self.make_queue(Fraction(2, 3), bases=((1, 3),))
        self.assertTrue(self.offer(queue, -1))
        self.assertTrue(self.offer(queue, 0))
        self.assertEqual(queue.media_duration, Fraction(2, 3))
        self.assertFalse(self.offer(queue, 1))
        self.assertEqual(queue.overflow_limit, "media")
        self.assertEqual(queue.peak_duration, Fraction(2, 3))
        self.assertEqual(queue.media_duration, 0)
        self.assertEqual(queue.bytes, 0)
        self.assertIsNone(queue.protocol)
        self.assertFalse(queue.spans)
        with self.assertRaises(QueueClosed) as closed:
            await queue.receive()
        self.assertEqual(closed.exception.reason, "overflow")

    async def test_timestamp_gap_counts_not_just_packet_durations(self):
        queue = await self.make_queue()
        self.assertTrue(self.offer(queue, 0))
        self.assertFalse(self.offer(queue, 10))  # just .2s of packets, 1.1s envelope
        self.assertEqual(queue.overflow_limit, "media")

    async def test_cross_track_offset_uses_shared_exact_seconds(self):
        queue = await self.make_queue(bases=((1, 10), (1, 100)))
        self.assertTrue(self.offer(queue, 0))
        self.assertFalse(self.offer(queue, 50, duration=10, track=1))
        self.assertEqual(queue.overflow_limit, "media")

    async def test_consumed_progress_still_bounds_late_track(self):
        queue = await self.make_queue(bases=((1, 10), (1, 10)))
        self.offer(queue, 10)
        await queue.receive()
        self.assertEqual(queue.media_duration, 0)
        self.assertFalse(self.offer(queue, 0, track=1))
        self.assertEqual(queue.overflow_limit, "media")

    async def test_reordered_pts_allowed_and_decode_lead_counts(self):
        queue = await self.make_queue()
        self.assertTrue(self.offer(queue, 0, dts=-2))
        # The second packet spans .5s alone, but retaining the first adds .1s.
        self.assertFalse(self.offer(queue, 3, dts=-1))
        self.assertEqual(queue.overflow_limit, "media")

    async def test_reordered_pts_within_budget_and_dequeue_reclaims_span(self):
        queue = await self.make_queue(Fraction(3, 5))
        self.assertTrue(self.offer(queue, 0, dts=-2))
        self.assertTrue(self.offer(queue, 3, dts=-1))
        self.assertTrue(self.offer(queue, 1, dts=0))  # PTS decreases, DTS increases
        self.assertEqual(queue.media_duration, Fraction(3, 5))
        await queue.receive()
        self.assertEqual(queue.media_duration, Fraction(1, 2))
        await queue.receive()
        self.assertEqual(queue.media_duration, Fraction(2, 5))
        await queue.receive()
        self.assertEqual(queue.media_duration, 0)
        self.assertEqual(queue.bytes, 0)
        self.assertFalse(queue.spans)

    async def test_missing_zero_and_regressing_clocks_fail_closed(self):
        for fields in ({"pts": None}, {"dts": None}, {"duration": 0}):
            queue = await self.make_queue()
            with self.assertRaises(ValueError):
                queue.offer(encode({**packet(), "pts": 0, "dts": 0, "duration": 1, **fields}))
            self.assertEqual(queue.reason, "source_failed")
            self.assertFalse(queue.spans)
        queue = await self.make_queue()
        self.offer(queue, 0)
        await queue.receive()
        with self.assertRaises(ValueError):
            self.offer(queue, 0)
        self.assertEqual(queue.reason, "source_failed")

    async def test_generation_failure_wakes_waiter_and_clears_state(self):
        queue = await self.make_queue()
        waiter = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        with self.assertRaises(ValueError):
            self.offer(queue, 0, generation=2)
        with self.assertRaises(QueueClosed):
            await asyncio.wait_for(waiter, 0.5)
        self.assertIsNone(queue.protocol)
        self.assertIsNone(queue.watermark)
        self.assertFalse(queue.last_dts)
        self.assertFalse(queue.offer(b"late"))

    async def test_cancellation_and_detach_keep_accounting_aligned(self):
        queue = await self.make_queue()
        waiter = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        self.offer(queue, 0)
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        self.assertEqual(len(queue.spans), 1)
        await queue.receive()
        self.assertFalse(queue.spans)
        self.offer(queue, 1)
        queue.detach("generation_changed")
        queue.detach("source_failed")
        self.assertEqual(queue.reason, "generation_changed")
        self.assertEqual(queue.media_duration, 0)
        self.assertFalse(queue.spans)

    async def test_byte_count_limits_and_metadata_remain_bounded(self):
        queue = await self.make_queue(max_frames=2)
        self.offer(queue, 0)
        self.offer(queue, 1)
        self.assertFalse(self.offer(queue, 2))
        self.assertEqual(queue.overflow_limit, "frames")
        queue = MediaQueue(1, 1)
        self.assertFalse(queue.offer(encode(hello())))
        self.assertEqual(queue.overflow_limit, "bytes")
        self.assertIsNone(queue.protocol)

    async def test_end_stays_a_wire_frame_not_successful_queue_closure(self):
        queue = await self.make_queue()
        self.offer(queue, 0)
        end = encode({"type": "end", "generation": 1, "packets": 1})
        self.assertTrue(queue.offer(end))
        await queue.receive()
        self.assertEqual(queue.media_duration, 0)
        self.assertEqual(await queue.receive(), end)
        self.assertFalse(queue.detached)
        queue.detach("source_complete")
        with self.assertRaises(QueueClosed) as closed:
            await queue.receive()
        self.assertEqual(closed.exception.reason, "source_complete")

    async def test_repeated_drain_does_not_accumulate_packet_accounting(self):
        queue = await self.make_queue()
        for timestamp in range(1000):
            self.assertTrue(self.offer(queue, timestamp))
            await queue.receive()
            self.assertEqual(queue.bytes, 0)
            self.assertFalse(queue.spans)
        self.assertEqual(len(queue.last_dts), 1)
        self.assertEqual(queue.peak_duration, Fraction(1, 10))
        self.assertEqual(queue.media_duration, 0)

    async def test_invalid_budgets_and_mutable_input(self):
        for budget in (0, -1, True, 0.5, float("nan"), "1"):
            with self.assertRaises(ValueError):
                MediaQueue(100, budget)
        queue = MediaQueue(1000, 1)
        with self.assertRaises(ValueError):
            queue.offer(bytearray(encode(hello())))
        self.assertEqual(queue.reason, "source_failed")
