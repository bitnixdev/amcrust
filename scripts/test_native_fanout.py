import asyncio
import unittest

from native_fanout_probe import Peer
from rtsp_fixture import CameraFixture, make_media
from test_shared_admission import setup, feed


class NativeFanoutUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_peer_hashes_all_wire_metadata_and_releases_deliveries(self):
        gate = setup()
        frame = feed(gate, 0)
        peer = Peer(gate.join())
        peer.expect(frame)
        await asyncio.sleep(0)
        for sequence in range(1, 6):
            frame = feed(gate, sequence)
            peer.expect(frame)
            await asyncio.sleep(0)
        gate.close("source_failed")
        await asyncio.wait_for(peer.task, 1)
        summary = peer.summary()
        self.assertEqual(summary["received"], 6)
        self.assertTrue(summary["exact_normalized_wire_hash"])
        self.assertEqual(summary["terminal"], "source_failed")
        self.assertEqual(summary["peak_inflight_frames"], 1)

    async def test_peer_detects_missing_expected_delivery(self):
        gate = setup()
        frame = feed(gate, 0)
        peer = Peer(gate.join())
        peer.expect(frame)
        peer.expect(frame)
        await asyncio.sleep(0)
        gate.close()
        await peer.task
        with self.assertRaisesRegex(ValueError, "lost or changed"):
            peer.summary()

    async def test_held_peer_expires_while_healthy_peer_keeps_receiving(self):
        gate = setup(max_residence=0.03)
        frame = feed(gate, 0)
        held, healthy = Peer(gate.join(), hold=True), Peer(gate.join())
        held.expect(frame)
        healthy.expect(frame)
        await asyncio.sleep(0.06)
        await asyncio.wait_for(held.task, 1)
        self.assertEqual(held.summary()["terminal"], "residence_expired")
        frame = feed(gate, 1)
        healthy.expect(frame)
        await asyncio.sleep(0)
        gate.close("generation_changed")
        await healthy.task
        self.assertEqual(healthy.summary()["received"], 2)

    async def test_fixture_duration_bounds(self):
        for invalid in (0, -1, True, float("nan"), float("inf"), 31):
            with self.assertRaises(ValueError):
                CameraFixture(([], []), send_seconds=invalid)
            with self.assertRaises(ValueError):
                await make_media("unused", 320, 180, duration=invalid)
        self.assertEqual(CameraFixture(([], [])).send_seconds, 2.2)
