import gc
from dataclasses import replace
from fractions import Fraction
import unittest
from unittest.mock import patch
import weakref

from audio_window import AudioWindowIndex
from packet_wire import encode_frame
from shared_admission import Admission, LateAdmission
from late_mux_input import LateMuxInput
from test_h264_reorder import descriptor
from test_packet_probe import hello, stream, packet
from test_shared_admission import feed


def setup():
    gate = LateAdmission(1)
    gate.feed(encode_frame({**hello(), "streams": 2}))
    gate.feed(encode_frame(descriptor()))
    gate.feed(encode_frame({**stream(), "index": 1, "codec": "aac", "media": "audio",
                            "time_base": [1, 48000], "extradata": "1188", "sample_rate": 48000,
                            "channels": 1, "profile": 1, "frame_size": 1024}))
    feed(gate, 0, pts=-1920, dts=-1920)
    return gate, gate.join()


async def drain(index):
    while not index.queue.queue.empty():
        with await index.queue.receive() as delivery:
            index.observe(delivery)


async def audio(gate, index, sequence, pts):
    frame = gate.feed(encode_frame({**packet(), "stream": 1, "sequence": sequence,
                                    "pts": pts, "dts": pts, "duration": 1024}))
    ref = weakref.ref(frame)
    await drain(index)
    return ref


class AudioWindowTests(unittest.IsolatedAsyncioTestCase):
    async def test_half_open_boundary_whole_units_and_no_media_retention(self):
        gate, observer = setup()
        index = AudioWindowIndex(observer)
        await drain(index)
        await audio(gate, index, 1, -1024)
        await audio(gate, index, 2, 0)
        last = await audio(gate, index, 3, 1024)
        gc.collect()
        self.assertIsNone(last())
        feed(gate, 4, pts=0, dts=0)
        await drain(index)
        late = gate.join()
        plan = index.plan(late)
        self.assertEqual([r.sequence for r in plan], [2, 3])
        self.assertEqual(plan[0].start, 0)
        self.assertEqual([r.sequence for r in index.plan(late, preceding=1)], [1, 2, 3])
        self.assertIsNone(index.plan(late, preceding=2))
        self.assertEqual(len(plan[0].sha256), 64)
        with self.assertRaises(AttributeError):
            plan[0].sequence = 999
        # A new published packet invalidates a plan even before the observer reads it.
        gate.feed(encode_frame({**packet(), "stream": 1, "sequence": 5,
                                "pts": 2048, "dts": 2048, "duration": 1024}))
        with self.assertRaises(ValueError):
            index.plan(late)
        await drain(index)
        with self.assertRaises(ValueError):
            index.plan(late)
        index.close()
        gate.close()

    async def test_budget_eviction_refuses_missing_coverage_not_retiming(self):
        for options in ({"max_records": 1}, {"max_wire_bytes": 250},
                        {"max_duration": Fraction(7, 100)}):
            gate, observer = setup()
            index = AudioWindowIndex(observer, **options)
            await drain(index)
            for seq in range(1, 7):
                await audio(gate, index, seq, (seq - 1) * 1024)
            self.assertGreater(index.evicted, 0)
            self.assertLessEqual(index.wire_bytes, index.max_wire_bytes)
            self.assertLessEqual(len(index.records), index.max_records)
            self.assertLessEqual(index._watermark - index.records[0].start, index.max_duration)
            feed(gate, 7, pts=0, dts=0)
            await drain(index)
            self.assertIsNone(index.plan(gate.join()))
            index.close()
            gate.close()

    async def test_exact_limits_accept_one_complete_unit(self):
        gate, observer = setup()
        obj = {**packet(), "stream": 1, "sequence": 1, "pts": 2048, "dts": 2048, "duration": 1024}
        size = len(encode_frame(obj))
        index = AudioWindowIndex(observer, max_records=1, max_wire_bytes=size,
                                 max_duration=Fraction(1024, 48000))
        await drain(index)
        gate.feed(encode_frame(obj))
        await drain(index)
        self.assertEqual(len(index.records), 1)
        self.assertEqual(index.wire_bytes, size)
        self.assertEqual(index._watermark - index.records[0].start, index.max_duration)
        index.close()
        self.assertIsNone(index.snapshot)
        gate.close()

    async def test_no_future_coverage_and_video_progress_evicts_old_audio(self):
        gate, observer = setup()
        index = AudioWindowIndex(observer)
        await drain(index)
        await audio(gate, index, 1, 0)
        feed(gate, 2, pts=1920, dts=1920)  # exactly the end of the only AAC interval
        await drain(index)
        self.assertIsNone(index.plan(gate.join()))
        feed(gate, 3, pts=90000, dts=90000)
        await drain(index)
        self.assertFalse(index.records)
        self.assertIsNone(index.plan(gate.join()))
        index.close()
        gate.close()

    async def test_idle_age_uses_original_feed_time_and_supervisor_expire(self):
        gate, observer = setup()
        index = AudioWindowIndex(observer, max_age=0.25)
        await drain(index)
        await audio(gate, index, 1, 0)
        feed(gate, 2, pts=0, dts=0)
        await drain(index)
        late = gate.join()
        self.assertIsNotNone(index.plan(late))
        deadline = max(index.records[0].deadline, index._candidate[-1])
        with patch.object(index.queue._loop, "time", return_value=deadline):
            index.expire()
            self.assertEqual(len(index.records), 0)
            self.assertIsNone(index.next_deadline)
            with self.assertRaises(ValueError):
                index.plan(late)
        index.close()
        gate.close()

    async def test_dequeue_does_not_reset_index_age(self):
        gate, observer = setup()
        index = AudioWindowIndex(observer, max_age=0.25)
        await drain(index)
        gate.feed(encode_frame({**packet(), "stream": 1, "sequence": 1,
                                "pts": 0, "dts": 0, "duration": 1024}))
        with await index.queue.receive() as delivery:
            born = delivery.deadline - index.queue.max_residence
            with patch.object(index.queue._loop, "time", return_value=born + 0.3):
                index.observe(delivery)
                self.assertFalse(index.records)
        index.close()
        gate.close()

    async def test_gap_overlap_and_single_oversize_fail_locally(self):
        for second_pts in (1023, 1025):
            gate, observer = setup()
            healthy = gate.join()
            index = AudioWindowIndex(observer)
            await drain(index)
            await audio(gate, index, 1, 0)
            with self.assertRaisesRegex(ValueError, "gap/overlap"):
                await audio(gate, index, 2, second_pts)
            self.assertTrue(index.closed)
            self.assertFalse(index.records)
            self.assertFalse(healthy.queue.detached)
            gate.close()
        gate, observer = setup()
        index = AudioWindowIndex(observer, max_wire_bytes=1)
        await drain(index)
        with self.assertRaisesRegex(ValueError, "single AAC"):
            await audio(gate, index, 1, 0)
        gate.close()

    async def test_retirement_foreign_source_and_receipt_gaps(self):
        gate, observer = setup()
        index = AudioWindowIndex(observer)
        await drain(index)
        other_gate, other = setup()
        with self.assertRaises(ValueError):
            index.plan(other)
        gate.close("generation_changed")
        with self.assertRaises(ValueError):
            index.expire()
        self.assertEqual(index.queue.reason, "generation_changed")
        self.assertFalse(index.records)
        other_gate.close()
        for mode in ("skip", "duplicate", "foreign", "released"):
            gate, observer = setup()
            index = AudioWindowIndex(observer)
            other = gate.join()
            delivery = await (other.queue if mode == "foreign" else index.queue).receive()
            if mode == "duplicate":
                index.observe(delivery)
            if mode == "released":
                delivery.release()
            if mode == "skip":
                delivery.release()
                with self.assertRaises(ValueError):
                    await audio(gate, index, 1, 0)
            else:
                with self.assertRaises(ValueError):
                    index.observe(delivery)
            delivery.release()
            self.assertTrue(index.closed)
            gate.close()

    async def test_reconstructed_snapshot_is_not_a_source_grant(self):
        gate, observer = setup()
        index = AudioWindowIndex(observer)
        await drain(index)
        await audio(gate, index, 1, 0)
        feed(gate, 2, pts=0, dts=0)
        await drain(index)
        late = gate.join()
        reconstructed = Admission(replace(late.snapshot), late.queue)
        with self.assertRaisesRegex(ValueError, "source-issued"):
            index.plan(reconstructed)
        self.assertIsNotNone(index.plan(late))
        with self.assertRaisesRegex(ValueError, "source-issued"):
            LateMuxInput(reconstructed)
        self.assertFalse(index.queue.detached)
        self.assertTrue(gate._epoch.active)
        index.close()
        gate.close()

    async def test_invalid_bounds_and_finite_end(self):
        for options in ({"max_records": True}, {"max_records": 257}, {"max_wire_bytes": 0},
                        {"max_duration": 0.25}, {"max_duration": Fraction(0)},
                        {"max_age": float("nan")}, {"max_age": True}):
            gate, observer = setup()
            with self.assertRaises(ValueError):
                AudioWindowIndex(observer, **options)
            self.assertTrue(observer.queue.detached)
            gate.close()
        gate, observer = setup()
        index = AudioWindowIndex(observer)
        await drain(index)
        gate.feed(encode_frame({"type": "end", "generation": 1, "packets": 1}))
        await drain(index)
        self.assertTrue(index.closed)
        self.assertFalse(index.records)
        gate.close()
