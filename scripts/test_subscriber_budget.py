import asyncio
from fractions import Fraction
import unittest
import weakref
import time

from packet_ingest_probe import QueueClosed
from subscriber_budget import LeasedMediaQueue
from test_shared_admission import setup, feed


class SubscriberBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_release_is_explicit_idempotent_and_accounted(self):
        gate = setup()
        frame = feed(gate, 0)
        admission = gate.join()
        delivery = await admission.queue.receive()
        self.assertIs(delivery.frame, frame)
        self.assertEqual(admission.queue.bytes, 0)
        self.assertEqual(admission.queue.inflight_bytes, len(frame))
        self.assertEqual(admission.queue.inflight_frames, 1)
        delivery.release()
        delivery.release()
        self.assertEqual(admission.queue.inflight_bytes, 0)
        self.assertIsNone(admission.queue.next_deadline)
        self.assertIsNotNone(admission.queue._timer)  # one harmless earlier wakeup is reused
        with self.assertRaises(ValueError):
            _ = delivery.frame
        gate.close()

    async def test_count_limit_is_local_and_retires_existing_deliveries(self):
        gate = setup(max_inflight_frames=1)
        feed(gate, 0)
        slow, healthy = gate.join(), gate.join()
        held = await slow.queue.receive()
        with await healthy.queue.receive():
            pass
        frame = feed(gate, 1)
        with self.assertRaises(QueueClosed) as closed:
            await slow.queue.receive()
        self.assertEqual(closed.exception.reason, "delivery_limit")
        self.assertEqual(slow.queue.inflight_bytes, 0)
        with self.assertRaises(ValueError):
            _ = held.frame
        with await healthy.queue.receive() as delivery:
            self.assertIs(delivery.frame, frame)
        gate.close()

    async def test_byte_boundary_and_byte_overflow(self):
        baseline = setup()
        size = len(feed(baseline, 0))
        second_size = len(feed(baseline, 1))
        baseline.close()
        for limit, accepted in ((size, True), (size - 1, False)):
            gate = setup(max_inflight_bytes=limit)
            feed(gate, 0)
            admission = gate.join()
            if accepted:
                with await admission.queue.receive():
                    self.assertEqual(admission.queue.inflight_bytes, size)
            else:
                with self.assertRaises(QueueClosed) as closed:
                    await admission.queue.receive()
                self.assertEqual(closed.exception.reason, "delivery_limit")
            gate.close()

        for limit, accepted in ((size + second_size, True), (size + second_size - 1, False)):
            gate = setup(max_inflight_bytes=limit)
            feed(gate, 0)
            admission = gate.join()
            first = await admission.queue.receive()
            feed(gate, 1)
            if accepted:
                with await admission.queue.receive():
                    self.assertEqual(admission.queue.inflight_bytes, limit)
                    self.assertEqual(admission.queue.inflight_frames, 2)
                first.release()
            else:
                with self.assertRaises(QueueClosed) as closed:
                    await admission.queue.receive()
                self.assertEqual(closed.exception.reason, "delivery_limit")
                self.assertEqual(admission.queue.inflight_bytes, 0)
            gate.close()

    async def test_idle_queue_and_idle_inflight_expire_without_source_progress(self):
        for consume in (False, True):
            gate = setup(max_residence=0.04)
            frame = feed(gate, 0)
            observed = weakref.ref(frame)
            admission = gate.join()
            if consume:
                held = await admission.queue.receive()
                self.assertEqual(held.deadline, gate._candidate_born + 0.04)
                reader = asyncio.create_task(admission.queue.receive())
            del frame
            await asyncio.sleep(0.07)
            self.assertTrue(admission.queue.detached)
            self.assertEqual(admission.queue.reason, "residence_expired")
            self.assertEqual(admission.queue.inflight_bytes, 0)
            self.assertIsNone(observed())
            self.assertIsNone(gate.join())
            if consume:
                with self.assertRaises(QueueClosed):
                    await asyncio.wait_for(reader, 1)
                with self.assertRaises(ValueError):
                    _ = held.frame
            gate.close()
            self.assertEqual(admission.queue.reason, "residence_expired")

    async def test_silent_candidate_expires_without_any_subscriber(self):
        gate = setup(max_residence=0.04)
        observed = weakref.ref(feed(gate, 0))
        await asyncio.sleep(0.07)
        self.assertIsNone(gate._candidate)
        self.assertIsNone(gate._candidate_timer)
        self.assertIsNone(observed())
        self.assertIsNone(gate.join())
        feed(gate, 1)
        self.assertIsNotNone(gate.join())  # local residence is not source-clock failure
        gate.close()

    async def test_residence_does_not_reset_when_dequeued(self):
        gate = setup(max_residence=1)
        feed(gate, 0)
        born = gate._candidate_born
        admission = gate.join()
        self.assertEqual(admission.queue.next_deadline, born + 1)
        await asyncio.sleep(0)
        held = await admission.queue.receive()
        self.assertEqual(held.deadline, born + 1)
        self.assertEqual(admission.queue.next_deadline, born + 1)
        gate.close("generation_changed")
        with self.assertRaises(ValueError):
            _ = held.frame
        self.assertIsNone(admission.queue._timer)

    async def test_healthy_empty_peer_survives_other_peers_residence_expiry(self):
        gate = setup(max_residence=0.04)
        feed(gate, 0)
        healthy, slow = gate.join(), gate.join()
        with await healthy.queue.receive():
            pass
        held = await slow.queue.receive()
        await asyncio.sleep(0.07)
        self.assertFalse(healthy.queue.detached)
        self.assertEqual(slow.queue.reason, "residence_expired")
        frame = feed(gate, 1)
        with await healthy.queue.receive() as delivery:
            self.assertIs(delivery.frame, frame)
        held.release()
        gate.close()

    async def test_cancellation_releases_context_and_waiter_does_not_steal_delivery(self):
        gate = setup()
        feed(gate, 0)
        admission = gate.join()
        entered = asyncio.Event()

        async def consume():
            with await admission.queue.receive():
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(consume())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(admission.queue.inflight_frames, 0)
        waiter = asyncio.create_task(admission.queue.receive())
        await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        frame = feed(gate, 1)
        with await admission.queue.receive() as delivery:
            self.assertIs(delivery.frame, frame)
        gate.close()

    async def test_multiple_waiters_cannot_bypass_inflight_cap(self):
        gate = setup(max_inflight_frames=1)
        feed(gate, 0)
        admission = gate.join()
        with await admission.queue.receive():
            pass
        waiters = [asyncio.create_task(admission.queue.receive()) for _ in range(2)]
        await asyncio.sleep(0)
        feed(gate, 1)
        feed(gate, 2)
        results = await asyncio.gather(*waiters, return_exceptions=True)
        self.assertEqual(sum(isinstance(item, QueueClosed) for item in results), 1)
        self.assertEqual(admission.queue.reason, "delivery_limit")
        self.assertEqual(admission.queue.peak_inflight_frames, 1)
        self.assertEqual(admission.queue.inflight_frames, 0)
        gate.close()

    async def test_escaped_alias_is_not_a_reclaim_or_revocation_guarantee(self):
        gate = setup()
        feed(gate, 0)
        admission = gate.join()
        delivery = await admission.queue.receive()
        escaped = delivery.frame.metadata["data"].view()
        expected = bytes(escaped)
        delivery.release()
        gate.close()
        self.assertEqual(admission.queue.inflight_bytes, 0)
        self.assertEqual(bytes(escaped), expected)  # API cannot revoke this alias
        escaped.release()

    async def test_access_and_release_enforce_expiry_even_before_timer_runs(self):
        for operation in ("frame", "release"):
            gate = setup(max_residence=0.02)
            feed(gate, 0)
            admission = gate.join()
            delivery = await admission.queue.receive()
            time.sleep(0.03)  # deliberately prevent the event loop's timer callback
            if operation == "frame":
                with self.assertRaises(ValueError):
                    _ = delivery.frame
            else:
                delivery.release()
            self.assertEqual(admission.queue.reason, "residence_expired")
            self.assertIsNone(gate.join())
            gate.close()

    async def test_deadline_and_size_are_read_only_and_timers_are_reused(self):
        gate = setup()
        feed(gate, 0)
        admission = gate.join()
        queue_timer, candidate_timer = admission.queue._timer, gate._candidate_timer
        with await admission.queue.receive() as delivery:
            for field in ("deadline", "size", "identifier"):
                with self.assertRaises(AttributeError):
                    setattr(delivery, field, 0)
        for sequence in range(1, 30):
            feed(gate, sequence)
            with await admission.queue.receive():
                pass
            self.assertIs(admission.queue._timer, queue_timer)
            self.assertIs(gate._candidate_timer, candidate_timer)
        # Emulate the original wakeup arriving before the replacement IDR's deadline.
        candidate_timer.cancel()
        gate._candidate_timeout()
        self.assertEqual(gate._candidate_timer.when(), gate._candidate_born + 2)
        self.assertIsNotNone(gate.join())
        queue_timer.cancel()
        admission.queue._on_timer()
        self.assertIsNone(admission.queue._timer)
        self.assertFalse(admission.queue.detached)
        gate.close()

    async def test_invalid_budgets(self):
        for kwargs in ({"max_residence": 0}, {"max_residence": float("nan")},
                       {"max_residence": float("inf")}, {"max_residence": 31},
                       {"max_residence": True}, {"max_residence": 10 ** 1000},
                       {"max_inflight_frames": 0},
                       {"max_inflight_frames": 257}, {"max_inflight_bytes": 0}):
            with self.assertRaises(ValueError):
                LeasedMediaQueue(1024, Fraction(1, 2), **kwargs)
