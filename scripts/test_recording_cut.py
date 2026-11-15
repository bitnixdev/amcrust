import unittest
from unittest.mock import patch

from packet_ingest_probe import Protocol
from packet_wire import encode_frame
from recording_cut import RecordingCutInput
from test_audio_history import setup, audio


class RecordingCutTests(unittest.IsolatedAsyncioTestCase):
    async def ready(self, **budgets):
        source = await setup(**budgets)
        handoff = source.open(preceding=1)
        recorder = RecordingCutInput(handoff)
        with await handoff.receive() as delivery:
            recorder.feed(delivery)
        return source, handoff, recorder

    async def test_cut_is_local_and_excludes_newer_queued_media(self):
        source, handoff, recorder = await self.ready()
        observer = source.join()
        with await observer.queue.receive():
            pass
        self.assertEqual(recorder.prepare_cut(), 4)
        frame = await audio(source, 5, 3072)
        result = recorder.commit_cut()
        self.assertEqual([p["sequence"] for p in result if p["type"] == "packet"], list(range(4)))
        self.assertEqual(result[-1]["packets"], 4)
        protocol = Protocol(1)
        for obj in result:
            protocol.accept(encode_frame(obj))
        self.assertTrue(protocol.ended)
        self.assertTrue(source._gate._epoch.active)
        self.assertFalse(source._gate._ended)
        self.assertEqual(handoff.reason, "cancelled")
        with await observer.queue.receive() as delivery:
            self.assertIs(delivery.frame, frame)
        source.close("source_failed")
        # Transfer is explicit: later source failure does not revoke independent copies.
        self.assertEqual(recorder.state, "committed")
        self.assertEqual(result[-1]["packets"], 4)
        with self.assertRaises(ValueError):
            recorder.commit_cut()

    async def test_prepared_cut_loses_to_owner_retirement(self):
        for reason in ("cancelled", "source_failed", "generation_changed"):
            source, handoff, recorder = await self.ready()
            recorder.prepare_cut()
            source.close(reason)
            with self.assertRaisesRegex(ValueError, "retired"):
                recorder.commit_cut()
            self.assertEqual(handoff.reason, reason)
            self.assertEqual(recorder.state, "aborted")
            self.assertEqual(recorder._frames, [])
            self.assertEqual(recorder._bytes, 0)

    async def test_subscriber_cancel_and_expiry_deny_commit_locally(self):
        for cancel in (False, True):
            source, handoff, recorder = await self.ready()
            recorder.prepare_cut()
            await audio(source, 5, 3072)
            if cancel:
                recorder.abort()
            else:
                with patch.object(handoff.admission.queue._loop, "time", return_value=handoff.admission.queue.next_deadline + 1):
                    handoff.admission.queue.expire()
            with self.assertRaises(ValueError):
                recorder.commit_cut()
            self.assertEqual(recorder.state, "aborted")
            self.assertTrue(source._gate._epoch.active)
            source.close()

    async def test_source_finite_end_does_not_authorize_recording_commit(self):
        source, _, recorder = await self.ready()
        recorder.prepare_cut()
        await source.feed(encode_frame({"type": "end", "generation": 1, "packets": 5}))
        with self.assertRaisesRegex(ValueError, "source ended"):
            recorder.commit_cut()
        source.close("source_failed")

    async def test_cut_refuses_outstanding_or_missing_live_receipts(self):
        source = await setup()
        h = source.open(preceding=1)
        recorder = RecordingCutInput(h)
        with self.assertRaisesRegex(ValueError, "complete tracks"):
            recorder.prepare_cut()
        self.assertTrue(source._gate._epoch.active)
        h = source.open(preceding=1)
        recorder = RecordingCutInput(h)
        delivery = await h.receive()
        recorder.feed(delivery)
        with self.assertRaisesRegex(ValueError, "released receipts"):
            recorder.prepare_cut()
        delivery.release()
        source.close()

    async def test_foreign_receipt_and_feed_after_prepare_refused(self):
        source, _, recorder = await self.ready()
        other = source.join()
        with await other.queue.receive() as delivery:
            with self.assertRaisesRegex(ValueError, "foreign"):
                recorder.feed(delivery)
        self.assertFalse(other.queue.detached)
        h = source.open(preceding=1)
        recorder = RecordingCutInput(h)
        with await h.receive() as delivery:
            recorder.feed(delivery)
        recorder.prepare_cut()
        await audio(source, 5, 3072)
        with await h.receive() as delivery:
            with self.assertRaisesRegex(ValueError, "not open"):
                recorder.feed(delivery)
        source.close()

    async def test_live_packet_and_final_end_are_charged(self):
        source = await setup()
        h = source.open(preceding=1)
        recorder = RecordingCutInput(h, max_packets=3)
        with await h.receive() as delivery:
            with self.assertRaisesRegex(ValueError, "packet budget"):
                recorder.feed(delivery)
        self.assertEqual(recorder._bytes, 0)
        self.assertTrue(source._gate._epoch.active)
        source.close()
        source, _, recorder = await self.ready()
        recorder.prepare_cut()
        recorder._max_bytes = recorder._bytes  # no room for even the private end record
        with self.assertRaisesRegex(ValueError, "byte budget"):
            recorder.commit_cut()
        self.assertEqual(recorder._frames, [])
        self.assertTrue(source._gate._epoch.active)
        source.close()

    async def test_bounds_clear_owned_copies_and_only_cancel_own_handoff(self):
        for budgets in ({"max_packets": 2}, {"max_bytes": 1}):
            source = await setup()
            h = source.open(preceding=1)
            with self.assertRaisesRegex(ValueError, "budget"):
                RecordingCutInput(h, **budgets)
            self.assertTrue(h.closed)
            self.assertTrue(h.history.released)
            self.assertEqual(source.history.leased_bytes, 0)
            self.assertTrue(source._gate._epoch.active)
            source.close()
