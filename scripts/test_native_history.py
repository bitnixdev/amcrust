import asyncio
from contextlib import nullcontext
from types import SimpleNamespace
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from native_history_probe import HistoryPeer, digest, generation_case
from packet_wire import encode_frame
from rtsp_fixture import CameraFixture
from test_audio_history import setup, audio
from test_h264_reorder import picture, wire
from test_packet_probe import packet


def oracle():
    return {n: digest(encode_frame({**packet(), "sequence": n, "stream": 1,
                                    "pts": (n - 1) * 1024, "dts": (n - 1) * 1024,
                                    "duration": 1024})) for n in (1, 2, 3)}


class NativeHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_peer_hashes_original_history_then_live(self):
        source = await setup()
        peer = HistoryPeer(source.open(preceding=1), oracle())
        peer.expect(SimpleNamespace(raw=encode_frame({**packet(), "sequence": 4, "pts": 1920,
                          "dts": 1920, "duration": 6000, "data": wire(picture()).hex()})))
        await asyncio.sleep(0)
        frame = await audio(source, 5, 3072)
        peer.expect(frame)
        await asyncio.sleep(0)
        source.close("source_failed")
        await peer.task
        result = peer.summary()
        self.assertEqual(result["received"], 5)
        self.assertTrue(result["prefix_exact"])
        self.assertEqual(result["terminal"], "source_failed")

    async def test_incorrect_or_missing_source_hash_cannot_pass(self):
        source = await setup()
        handoff = source.open(preceding=1)
        with self.assertRaises(KeyError):
            HistoryPeer(handoff, {})
        handoff.close()
        bad = oracle()
        bad[1] = bytes(32)
        peer = HistoryPeer(source.open(preceding=1), bad, hold=True)
        await asyncio.sleep(0)
        source.close("source_failed")
        await peer.task
        with self.assertRaisesRegex(ValueError, "prefix differs"):
            peer.summary()

    async def test_multiple_prefix_waiters_wake_on_owner_retirement(self):
        source = await setup()
        h = source.open()
        waiters = [asyncio.create_task(h.wait_closed()) for _ in range(2)]
        await asyncio.sleep(0)
        source.close("generation_changed")
        self.assertEqual(await asyncio.wait_for(asyncio.gather(*waiters), 1),
                         ["generation_changed", "generation_changed"])
        self.assertEqual(source.history.bytes, 0)
        self.assertEqual(await h.wait_closed(), "generation_changed")

    async def test_prefix_waiters_wake_on_idle_expiry(self):
        source = await setup(max_age=0.05)
        h = source.open()
        try:
            reasons = await asyncio.wait_for(asyncio.gather(h.wait_closed(), h.wait_closed()), 1)
            self.assertEqual(reasons, ["residence_expired", "residence_expired"])
        finally:
            source.close()

    async def test_worker_cleanup_survives_retirement_failure(self):
        class Camera:
            port = 1234
            async def __aenter__(self):
                return self
            async def __aexit__(self, *exc):
                return False
        for close_error, cancel_cleanup in ((False, False), (True, False), (False, True)):
            process = SimpleNamespace(stdout=object(), returncode=None)
            source = Mock()
            source.history.bytes = 1  # deliberately broken retirement accounting
            if close_error:
                source.close.side_effect = ValueError("retirement failed")
            clock = Mock(next_deadline=time.monotonic() + 10)
            with patch("native_history_probe.CameraFixture", return_value=Camera()), \
                    patch("native_history_probe.HistorySource", return_value=source), \
                    patch("native_history_probe.ClockNormalizer", return_value=clock), \
                    patch("native_history_probe.asyncio.create_subprocess_exec", AsyncMock(return_value=process)), \
                    patch("native_history_probe.read_frame", AsyncMock(return_value=None)), \
                    patch("native_history_probe.stop_process", AsyncMock()) as stop, \
                    (patch("native_history_probe.asyncio.gather", side_effect=asyncio.CancelledError())
                     if cancel_cleanup else nullcontext()):
                error = (self.assertRaises(asyncio.CancelledError) if cancel_cleanup else
                         self.assertRaisesRegex(ValueError, "retirement"))
                with error:
                    await generation_case("worker", ([], []), 1, mode="disconnect", seconds=3,
                                          expected_error="native source EOF")
                source.close.assert_called_once_with("source_failed")
                clock._abort.assert_called_once_with()
                stop.assert_awaited_once_with(process)

    def test_parameter_change_requires_supported_idr_boundary(self):
        for value in (0, 16, 450, True, 30.0, None):
            with self.assertRaisesRegex(ValueError, "parameter-change frame"):
                CameraFixture(([], []), "parameter-change", replacement_sps=b"\x67",
                              parameter_change_frame=value)
        with self.assertRaises(ValueError):
            CameraFixture(([], []), parameter_change_frame=30)
        fixture = CameraFixture(([], []), "parameter-change", replacement_sps=b"\x67",
                                parameter_change_frame=30)
        self.assertEqual(fixture.parameter_change_frame, 30)
