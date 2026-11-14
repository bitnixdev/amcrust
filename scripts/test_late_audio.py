import struct
import unittest
from unittest.mock import patch

from late_audio_probe import adts_units, pcm_difference
from late_mux_input import LateMuxInput
from packet_wire import encode_frame
from rtsp_fixture import make_media
from test_shared_admission import setup, feed


class LateMuxInputTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_counters_only_and_original_end_count(self):
        gate = setup()
        feed(gate, 0)
        feed(gate, 1)
        first = feed(gate, 2)
        admission = gate.join()
        adapter = LateMuxInput(admission)
        with await admission.queue.receive() as delivery:
            adapter.feed(delivery)
            self.assertEqual(delivery.frame.metadata["sequence"], 2)
        gate.feed(encode_frame({"type": "end", "generation": 1, "packets": 3}))
        with await admission.queue.receive() as delivery:
            adapter.feed(delivery)
            self.assertEqual(delivery.frame.metadata["packets"], 3)
        clip = adapter.finish()
        self.assertEqual(clip[-2]["sequence"], 0)
        self.assertEqual(clip[-2]["pts"], first.metadata["pts"])
        self.assertEqual(clip[-2]["dts"], first.metadata["dts"])
        self.assertEqual(clip[-1]["packets"], 1)
        self.assertEqual(clip[1]["extradata"], bytes(admission.snapshot.streams[0]["extradata"].view()).hex())
        with self.assertRaises(ValueError):
            adapter.finish()
        gate.close()

    async def test_skipped_or_duplicate_delivery_fails_closed(self):
        for duplicate in (False, True):
            gate = setup()
            feed(gate, 0)
            admission = gate.join()
            adapter = LateMuxInput(admission)
            with await admission.queue.receive() as delivery:
                if duplicate:
                    adapter.feed(delivery)
                    with self.assertRaises(ValueError):
                        adapter.feed(delivery)
            if not duplicate:
                feed(gate, 1)
                with await admission.queue.receive() as delivery:
                    with self.assertRaises(ValueError):
                        adapter.feed(delivery)
            self.assertTrue(admission.queue.detached)
            self.assertEqual(adapter._frames, [])
            with self.assertRaises(ValueError):
                adapter.finish()
            gate.close()

    async def test_other_subscriber_lease_is_not_authorization(self):
        gate = setup()
        first = feed(gate, 0)
        own, other = gate.join(), gate.join()
        adapter = LateMuxInput(own)
        with await other.queue.receive() as delivery:
            with self.assertRaisesRegex(ValueError, "foreign"):
                adapter.feed(delivery)
            self.assertIs(delivery.frame, first)
        self.assertTrue(own.queue.detached)
        self.assertFalse(other.queue.detached)
        gate.close()

    async def test_released_retired_and_unfinished_inputs_are_refused(self):
        for mode in ("released", "retired", "unfinished"):
            gate = setup()
            feed(gate, 0)
            admission = gate.join()
            adapter = LateMuxInput(admission)
            delivery = await admission.queue.receive()
            if mode == "released":
                delivery.release()
            elif mode == "retired":
                gate.close("generation_changed")
            else:
                adapter.feed(delivery)
                delivery.release()
            if mode != "unfinished":
                with self.assertRaises(ValueError):
                    adapter.feed(delivery)
            with self.assertRaises(ValueError):
                adapter.finish()
            self.assertEqual(adapter._frames, [])
            gate.close()

    async def test_packet_and_expanded_byte_budgets_are_local(self):
        gate = setup()
        feed(gate, 0)
        admission, healthy = gate.join(), gate.join()
        with self.assertRaisesRegex(ValueError, "byte budget"):
            LateMuxInput(admission, max_bytes=1)
        self.assertFalse(healthy.queue.detached)
        adapter = LateMuxInput(healthy, max_packets=1)
        with await healthy.queue.receive() as delivery:
            adapter.feed(delivery)
        feed(gate, 1)
        with await healthy.queue.receive() as delivery:
            with self.assertRaisesRegex(ValueError, "packet budget"):
                adapter.feed(delivery)
        self.assertTrue(gate._epoch.active)
        self.assertEqual(adapter._bytes, 0)
        gate.close()

    async def test_finalization_checks_expiry_without_timer_dispatch(self):
        gate = setup()
        feed(gate, 0)
        admission = gate.join()
        adapter = LateMuxInput(admission)
        with await admission.queue.receive() as delivery:
            adapter.feed(delivery)
        gate.feed(encode_frame({"type": "end", "generation": 1, "packets": 1}))
        with await admission.queue.receive() as delivery:
            adapter.feed(delivery)
            with patch.object(admission.queue._loop, "time", return_value=delivery.deadline + 1):
                with self.assertRaises(ValueError):
                    adapter.finish()
        self.assertEqual(admission.queue.reason, "residence_expired")
        self.assertEqual(adapter._frames, [])
        gate.close()

    async def test_invalid_budgets_and_fixture_switch_are_rejected(self):
        for options in ({"max_packets": True}, {"max_packets": 0}, {"max_packets": 257},
                        {"max_bytes": False}, {"max_bytes": None}, {"max_bytes": 33 * 1024 * 1024}):
            gate = setup()
            feed(gate, 0)
            admission = gate.join()
            with self.assertRaises(ValueError):
                LateMuxInput(admission, **options)
            self.assertTrue(admission.queue.detached)
            gate.close()
        for value in (0, 1, "false", float("nan")):
            with self.assertRaisesRegex(ValueError, "PNS"):
                await make_media("unused", 320, 180, aac_pns=value)

    async def test_trailing_frame_after_end_and_retirement_before_finish(self):
        for trailing in (False, True):
            gate = setup()
            feed(gate, 0)
            admission = gate.join()
            adapter = LateMuxInput(admission)
            with await admission.queue.receive() as delivery:
                adapter.feed(delivery)
            gate.feed(encode_frame({"type": "end", "generation": 1, "packets": 1}))
            with await admission.queue.receive() as delivery:
                adapter.feed(delivery)
                if trailing:
                    with self.assertRaises(ValueError):
                        adapter.feed(delivery)
            gate.close("source_failed")
            with self.assertRaises(ValueError):
                adapter.finish()


class AudioDiagnosticTests(unittest.TestCase):
    def test_pcm_diff_is_exact_not_a_tolerance(self):
        a = struct.pack("<hhhh", -10, 20, 30, 40)
        b = struct.pack("<hhhh", -10, 25, 30, -40)
        self.assertEqual(pcm_difference(a, b), {"different_samples": 2, "first": 1,
                                               "last": 3, "max_abs_s16_error": 80})
        self.assertEqual(pcm_difference(a, a)["different_samples"], 0)
        for bad in (b"a", b"abcd"):
            with self.assertRaises(ValueError):
                pcm_difference(a, bad)

    def test_adts_preserves_headers_and_rejects_truncation(self):
        unit = bytes([255, 241, 76, 64, 1, 95, 252]) + b"aac"  # ten-byte ADTS unit
        self.assertEqual(adts_units(unit * 2), [unit, unit])
        with self.assertRaises(ValueError):
            adts_units(unit[:-1])
