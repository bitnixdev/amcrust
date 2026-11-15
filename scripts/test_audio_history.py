import asyncio
import gc
import weakref
from fractions import Fraction
import unittest
from unittest.mock import patch

from audio_history import HistorySource, HistoryLease, Handoff
from packet_ingest_probe import QueueClosed
from packet_wire import encode_frame, decode_frame
from test_h264_reorder import descriptor, picture, wire
from test_packet_probe import hello, stream, packet


async def video(source, sequence, pts):
    return await source.feed(encode_frame({**packet(), "sequence": sequence, "pts": pts,
        "dts": pts, "duration": 6000, "data": wire(picture()).hex()}))


async def audio(source, sequence, pts):
    return await source.feed(encode_frame({**packet(), "sequence": sequence, "stream": 1,
        "pts": pts, "dts": pts, "duration": 1024}))


async def setup(**budgets):
    source = HistorySource(1, history_budgets=budgets)
    await source.feed(encode_frame({**hello(), "streams": 2}))
    await source.feed(encode_frame(descriptor()))
    await source.feed(encode_frame({**stream(), "index": 1, "codec": "aac", "media": "audio",
        "time_base": [1, 48000], "extradata": "1188", "sample_rate": 48000,
        "channels": 1, "profile": 1, "frame_size": 1024}))
    await video(source, 0, -1920)
    await audio(source, 1, 0)
    await audio(source, 2, 1024)
    await audio(source, 3, 2048)
    await video(source, 4, 1920)
    return source


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_prefix_then_original_live_and_end(self):
        source = await setup()
        handoff = source.open(preceding=1)
        self.assertIsNotNone(handoff)
        with self.assertRaises(ValueError):
            source.close("source_complete")
        with self.assertRaises(ValueError):
            handoff.close("source_complete")
        with self.assertRaises(ValueError):
            await handoff.receive()
        with handoff.history as prefix:
            self.assertEqual([decode_frame(raw)["sequence"] for raw in prefix.frames], [1, 2, 3])
            self.assertEqual(prefix.size, sum(map(len, prefix.frames)))
            with self.assertRaises(AttributeError):
                prefix.deadline = 1000000000
        prefix.release()  # idempotent
        with self.assertRaises(ValueError):
            handoff.finish()
        with await handoff.receive() as delivery:
            self.assertEqual(delivery.frame.metadata["sequence"], 4)
        await audio(source, 5, 3072)
        with await handoff.receive() as delivery:
            self.assertEqual(delivery.frame.metadata["sequence"], 5)
        await source.feed(encode_frame({"type": "end", "generation": 1, "packets": 6}))
        with self.assertRaises(ValueError):
            source.close("source_complete")
        self.assertFalse(source.history.closed)
        with await handoff.receive() as delivery:
            self.assertEqual(delivery.frame.metadata["packets"], 6)
            with self.assertRaises(ValueError):
                handoff.finish()
        handoff.finish()
        with self.assertRaises(QueueClosed) as closed:
            await handoff.receive()
        self.assertEqual(closed.exception.reason, "source_complete")
        source.close("source_complete")
        self.assertEqual(source.history.bytes, 0)
        self.assertFalse(source.history._bodies)
        self.assertFalse(source.history._handoffs)
        self.assertIsNone(source.history._timer)

    async def test_shared_pins_stay_charged_after_cache_eviction(self):
        source = await setup(max_duration=Fraction(1, 8))
        a, b = source.open(), source.open()
        raw = a.history.frames[0]
        self.assertIs(raw, b.history.frames[0])
        sequence = decode_frame(raw)["sequence"]
        del raw
        self.assertEqual(source.history._bodies[sequence].pins, 2)
        self.assertEqual(source.history.leased_bytes, a.history.size + b.history.size)
        self.assertEqual(source.history.pinned_bytes, b.history.size)
        a.history.release()
        self.assertEqual(source.history._bodies[sequence].pins, 1)
        for seq in range(5, 14):
            await audio(source, seq, (seq - 2) * 1024)
            # Keep live queues from overflowing while exercising history ownership.
            while not a.admission.queue.queue.empty():
                with await a.receive():
                    pass
        self.assertFalse(source.history._bodies[sequence].cached)
        self.assertGreaterEqual(source.history.bytes, b.history.size)
        b.history.release()
        self.assertNotIn(sequence, source.history._bodies)
        source.close()

    async def test_idle_prefix_expiry_is_local_and_does_not_reset_age(self):
        source = await setup(max_age=0.05)
        healthy = source.join()
        h = source.open()
        original = h.history.deadline
        self.assertLessEqual(original, min(r.deadline for r in h.history._records))
        await asyncio.sleep(0.1)
        self.assertTrue(h.closed)
        self.assertEqual(h.reason, "residence_expired")
        with self.assertRaises(ValueError):
            h.history.frames
        self.assertFalse(healthy.queue.detached)
        self.assertEqual(source.history.bytes, 0)
        self.assertIsNone(source.history._timer)
        source.close()

    async def test_pinned_body_pressure_fails_history_not_source(self):
        source = await setup(max_records=4)
        h, healthy = source.open(), source.join()
        for seq in range(5, 8):
            await audio(source, seq, (seq - 2) * 1024)
        self.assertEqual(source.history.reason, "overflow")
        self.assertEqual(h.reason, "overflow")
        self.assertEqual(source.history.bytes, 0)
        self.assertFalse(healthy.queue.detached)
        self.assertTrue(source._gate._epoch.active)
        source.close()

    async def test_byte_pressure_is_independent_of_count_cap(self):
        obj = {**packet(), "sequence": 3, "stream": 1, "pts": 2048, "dts": 2048, "duration": 1024}
        limit = len(encode_frame(obj)) * 4
        source = await setup(max_wire_bytes=limit)
        h = source.open()
        for seq in range(5, 8):
            await audio(source, seq, (seq - 2) * 1024)
        self.assertEqual(h.reason, "overflow")
        self.assertLessEqual(source.history.peak_bytes, limit)
        self.assertLess(source.history.peak_bodies, source.history.index.max_records)
        self.assertEqual(source.history.bytes, 0)
        source.close()

    async def test_invalid_source_and_repeated_retirement_clear_body_owners(self):
        for turn in range(8):
            source = await setup()
            h = source.open()
            owners = [weakref.ref(body) for body in source.history._bodies.values()]
            if turn == 0:
                with self.assertRaises(ValueError):
                    await audio(source, 999, 3072)
                self.assertEqual(h.reason, "source_failed")
            else:
                source.close(("generation_changed", "source_failed", "cancelled")[turn % 3])
            gc.collect()
            self.assertTrue(all(ref() is None for ref in owners))
            self.assertEqual(source.history.bytes, 0)
            self.assertEqual(source.history.leased_bytes, 0)
            self.assertIsNone(source.history._timer)

    async def test_source_retirement_invalidates_history_live_and_waiters(self):
        source = await setup()
        held, live = source.open(), source.open()
        escaped = held.history.frames[0]  # deliberately violate the alias-drop promise
        live.history.release()
        delivery = await live.receive()
        source.close("generation_changed")
        with self.assertRaises(ValueError):
            held.history.frames
        with self.assertRaises(ValueError):
            delivery.frame
        self.assertTrue(escaped)  # immutable escaped bytes cannot be revoked/erased
        self.assertEqual(source.history.bytes, 0)
        self.assertIsNone(source.history._timer)
        with self.assertRaises(QueueClosed):
            await live.receive()
        replacement = await setup()
        with self.assertRaises(ValueError):
            replacement.history.select(held.admission, 0)
        replacement.close()

    async def test_cancelling_blocked_live_receive_releases_its_handoff(self):
        source = await setup()
        h = source.open()
        h.history.release()
        with await h.receive():
            pass
        tasks = [asyncio.create_task(h.receive()) for _ in range(2)]
        await asyncio.sleep(0)
        tasks[0].cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.assertIsInstance(results[0], asyncio.CancelledError)
        self.assertIsInstance(results[1], QueueClosed)
        self.assertEqual(h.reason, "cancelled")
        self.assertFalse(source.history._handoffs)
        source.close()

    async def test_caps_factories_delayed_expiry_and_owning_copy(self):
        source = await setup(max_handoffs=1)
        h = source.open()
        with self.assertRaisesRegex(ValueError, "handoff limit"):
            source.open()
        with self.assertRaises(ValueError):
            Handoff(None, None, ())
        with self.assertRaises(ValueError):
            HistoryLease(None, ())
        timer = source.history._timer
        source.history._arm()
        self.assertIs(source.history._timer, timer)
        with patch.object(source.history.index.queue._loop, "time", return_value=h.history.deadline):
            with self.assertRaises(ValueError):
                h.history.frames
        self.assertEqual(h.reason, "residence_expired")
        frame = await audio(source, 5, 3072)
        self.assertEqual(source.history._bodies[5].raw, frame.raw)
        self.assertIsNot(source.history._bodies[5].raw, frame.raw)
        source.close()
