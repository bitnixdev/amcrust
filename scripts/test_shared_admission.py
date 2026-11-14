"""Conservative admission/cursor bootstrap tests; decoded real video is in Nix."""
import asyncio
from dataclasses import FrozenInstanceError
import unittest

from packet_ingest_probe import QueueClosed, SharedSource
from packet_wire import encode_frame
from shared_admission import LateAdmission
from test_h264_reorder import descriptor, picture, pps, sps, wire
from test_packet_probe import hello, packet


def setup(**kwargs):
    gate = LateAdmission(1, **kwargs)
    gate.feed(encode_frame(hello()))
    gate.feed(encode_frame(descriptor()))
    return gate


def feed(gate, sequence, *, units=None, flags=1, **overrides):
    return gate.feed(encode_frame({**packet(), "sequence": sequence,
        "pts": sequence * 6000, "dts": sequence * 6000, "duration": 6000,
        "flags": flags, "data": wire(*(units or [picture()])).hex(), **overrides}))


class LateAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_idr_not_flag_and_preserved_global_counters(self):
        intra = setup()
        feed(intra, 0, units=[b"\x41" + picture()[1:]], flags=1)
        self.assertIsNone(intra.join())  # I slice in a non-IDR NAL is not enough
        intra.close()
        gate = setup()
        feed(gate, 0, units=[picture(0)], flags=1)
        self.assertIsNone(gate.join())  # false keyframe flag on P slice
        feed(gate, 1, units=[picture()], flags=0)
        self.assertIsNone(gate.join())  # conservative flag + actual IDR requirement
        first = feed(gate, 2)
        admission = gate.join()
        self.assertEqual(admission.snapshot.first_sequence, 2)
        self.assertEqual(admission.snapshot.first_ordinal, first.ordinal)
        self.assertEqual(admission.snapshot.first_video_pts, 12000)
        self.assertIs(admission.snapshot.origin, first.origin)
        self.assertIs(await admission.queue.receive(), first)
        next_frame = feed(gate, 3, units=[picture(0)], flags=0)
        self.assertIs(await admission.queue.receive(), next_frame)
        end = gate.feed(encode_frame({"type": "end", "generation": 1, "packets": 4}))
        self.assertIs(await admission.queue.receive(), end)
        self.assertTrue(admission.queue.protocol.ended)
        with self.assertRaises(ValueError):
            gate.join()
        gate.close("source_complete")

    async def test_old_idr_is_not_a_replay_buffer(self):
        gate = setup()
        feed(gate, 0)
        feed(gate, 1, units=[picture(0)], flags=0)
        self.assertIsNone(gate.join())
        first = feed(gate, 2)
        admission = gate.join()
        self.assertIs(await admission.queue.receive(), first)
        with self.assertRaises(FrozenInstanceError):
            admission.snapshot.first_sequence = 0
        with self.assertRaises(TypeError):
            admission.snapshot.streams[0]["width"] = 1
        gate.close()
        with self.assertRaises(ValueError):
            gate.join()

    async def test_parameter_change_before_admission_and_after_admission_fails(self):
        for admitted in (False, True):
            gate = setup()
            feed(gate, 0)
            admission = gate.join() if admitted else None
            if admission:
                await admission.queue.receive()
                reader = asyncio.create_task(admission.queue.receive())
                await asyncio.sleep(0)
            with self.assertRaisesRegex(ValueError, "parameter sets changed"):
                feed(gate, 1, units=[sps(profile=100), pps(), picture()])
            with self.assertRaises(ValueError):
                gate.join()
            if admission:
                with self.assertRaises(QueueClosed) as closed:
                    await asyncio.wait_for(reader, 0.5)
                self.assertEqual(closed.exception.reason, "source_failed")

    async def test_wrong_pps_mixed_vcl_and_unknown_clocks_do_not_publish(self):
        for changes in ({"units": [picture(pps_id=1)]},
                        {"units": [b"\x05" + picture()[1:]]},
                        {"units": [picture(), picture(0)]}, {"pts": None},
                        {"pts": 1, "dts": 0}, {"duration": 0}):
            gate = setup()
            with self.assertRaises(ValueError):
                feed(gate, 0, **changes)
            with self.assertRaises(ValueError):
                gate.join()

    async def test_unsupported_configuration_refused_instead_of_inferred(self):
        for config in (descriptor(profile=77, reorder=2),
                       {**descriptor(), "video_delay": 1},
                       {**descriptor(), "profile": 578},
                       {**descriptor(), "extradata": "0164001f"}):
            gate = LateAdmission(1)
            gate.feed(encode_frame(hello()))
            with self.assertRaises(ValueError):
                gate.feed(encode_frame(config))
            with self.assertRaises(ValueError):
                gate.join()

    async def test_overflow_is_local_and_live_failure_wakes_other_peer(self):
        gate = setup()
        first = feed(gate, 0)
        fast, slow = gate.join(), gate.join()
        self.assertIs(await fast.queue.receive(), first)
        for sequence in range(1, 10):
            current = feed(gate, sequence, units=[picture(0)], flags=0)
            self.assertIs(await fast.queue.receive(), current)
        self.assertEqual(slow.queue.reason, "overflow")
        self.assertFalse(fast.queue.detached)
        reader = asyncio.create_task(fast.queue.receive())
        await asyncio.sleep(0)
        gate.close("source_failed")
        with self.assertRaises(QueueClosed) as closed:
            await asyncio.wait_for(reader, 0.5)
        self.assertEqual(closed.exception.reason, "source_failed")
        self.assertEqual(slow.queue.reason, "overflow")

    async def test_admission_limits_do_not_retire_source(self):
        gate = setup(max_subscribers=1)
        first = feed(gate, 0)
        admission = gate.join()
        with self.assertRaisesRegex(ValueError, "subscriber limit"):
            gate.join()
        self.assertIs(await admission.queue.receive(), first)
        admission.queue.detach()
        replacement = gate.join()
        self.assertIs(await replacement.queue.receive(), first)
        gate.close()
        gate = setup(max_bytes=1)
        feed(gate, 0)
        with self.assertRaises(QueueClosed):
            gate.join()
        # Overflow belongs to the attempted subscriber, not the source generation.
        feed(gate, 1, units=[picture(0)], flags=0)
        self.assertIsNone(gate.join())
        gate.close()

    async def test_reconnect_retires_old_snapshot_and_wakes_old_receiver(self):
        old = setup()
        old_frame = feed(old, 0)
        admission = old.join()
        await admission.queue.receive()
        reader = asyncio.create_task(admission.queue.receive())
        await asyncio.sleep(0)
        old.close("generation_changed")
        with self.assertRaises(QueueClosed) as closed:
            await asyncio.wait_for(reader, 0.5)
        self.assertEqual(closed.exception.reason, "generation_changed")
        new = LateAdmission(2)
        new.feed(encode_frame({**hello(), "generation": 2}))
        new.feed(encode_frame({**descriptor(profile=100), "generation": 2}))
        new_frame = feed(new, 0, generation=2)
        replacement = new.join()
        self.assertEqual(admission.snapshot.streams[0]["profile"], 77)
        self.assertEqual(replacement.snapshot.streams[0]["profile"], 100)
        self.assertIs(await replacement.queue.receive(), new_frame)
        with self.assertRaises(ValueError):
            replacement.queue.offer(old_frame)
        with self.assertRaises(ValueError):
            old.join()
        new.close()

    async def test_foreign_origin_and_generation_cannot_use_bootstrap(self):
        gate = setup()
        feed(gate, 0)
        admission = gate.join()
        await admission.queue.receive()
        other = SharedSource()
        other.accept(encode_frame(hello()))
        other.accept(encode_frame(descriptor()))
        other.accept(encode_frame({**packet(), "data": wire(picture()).hex()}))
        foreign = other.accept(encode_frame({**packet(), "sequence": 1, "data": wire(picture()).hex()}))
        with self.assertRaisesRegex(ValueError, "identity"):
            admission.queue.offer(foreign)
        self.assertEqual(admission.queue.reason, "source_failed")
        with self.assertRaises(ValueError):
            feed(gate, 1, generation=2)
