"""Deterministic protocol/admission/cleanup tests; no FFmpeg needed."""
import asyncio
import hashlib
from packet_wire import decode_frame, encode_frame
import sys
import unittest

from ingest_probe import stop_process
from packet_ingest_probe import Protocol, QueueClosed
from rtsp_fixture import NONCE, REALM, access_units, authorized, fragments
from rtsp_ingest_probe import LiveAdmission, SourceSupervisor, nal_types
from test_packet_probe import hello, packet, stream


PARAMETERS = "000000016742001e0000000168ce06e2"


def config(generation=1):
    return {**stream(), "generation": generation, "extradata": PARAMETERS}


def frame(nal="65", generation=1, key=True):
    return {**packet(), "generation": generation, "flags": int(key), "data": "00000001" + nal}


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_skip_interframes_audio_and_false_keyframe_flags(self):
        gate = LiveAdmission()
        audio = {**config(), "index": 1, "codec": "aac", "media": "audio", "extradata": "1188"}
        gate.reset(1, {0: config(), 1: audio})
        for obj in (frame("41", key=False), frame("41"), {**frame(), "stream": 1}):
            self.assertFalse(gate.offer(obj))
        self.assertEqual(gate.skipped, 3)
        self.assertTrue(gate.queue.queue.empty())
        self.assertTrue(gate.offer(frame()))
        self.assertEqual(gate.queue.queue.qsize(), 3)  # two configs then the IDR
        first = decode_frame(await gate.queue.receive())
        self.assertEqual(first["type"], "stream")
        self.assertTrue(gate.offer(frame("41", key=False)))
        self.assertTrue(gate.offer({**frame(), "stream": 1}))

    async def test_requires_current_parameter_sets(self):
        gate = LiveAdmission()
        gate.reset(1, {0: {**config(), "extradata": ""}})
        self.assertFalse(gate.offer(frame()))
        self.assertFalse(gate.offer(frame("6742001e0000000168ce06e2", key=False)))
        # Discarded parameter-set packets cannot initialize a future decoder.
        self.assertFalse(gate.offer(frame()))
        self.assertTrue(gate.offer(frame("6742001e0000000168ce06e20000000165")))

    async def test_generation_reset_clears_old_queue_and_idr_state(self):
        gate = LiveAdmission()
        gate.reset(1, {0: config()})
        self.assertTrue(gate.offer(frame()))
        old_queue = gate.queue
        gate.reset(2, {0: config(2)})
        self.assertTrue(old_queue.detached)
        self.assertEqual(old_queue.bytes, 0)
        self.assertTrue(old_queue.queue.empty())
        with self.assertRaises(ValueError):
            gate.offer(frame())
        self.assertFalse(gate.offer(frame("41", generation=2, key=False)))
        self.assertTrue(gate.offer(frame(generation=2)))
        gate.invalidate()
        self.assertFalse(gate.active)
        self.assertIsNone(gate.video)
        self.assertFalse(gate.streams)
        self.assertFalse(gate.parameters)
        self.assertFalse(gate.offer(frame(generation=2)))

    async def test_generation_and_source_failure_wake_retired_receivers(self):
        gate = LiveAdmission()
        gate.reset(1, {0: config()})
        old = gate.queue
        reader = asyncio.create_task(old.receive())
        await asyncio.sleep(0)
        gate.reset(2, {0: config(2)})
        with self.assertRaises(QueueClosed) as closed:
            await asyncio.wait_for(reader, 0.5)
        self.assertEqual(closed.exception.reason, "generation_changed")
        self.assertFalse(gate.queue.detached)
        reader = asyncio.create_task(gate.queue.receive())
        await asyncio.sleep(0)
        gate.invalidate("source_failed")
        with self.assertRaises(QueueClosed) as closed:
            await asyncio.wait_for(reader, 0.5)
        self.assertEqual(closed.exception.reason, "source_failed")
        self.assertFalse(gate.ready)

    async def test_reject_stale_config_and_unknown_stream(self):
        gate = LiveAdmission()
        with self.assertRaises(ValueError):
            gate.reset(2, {0: config()})
        gate.reset(1, {0: config()})
        with self.assertRaises(ValueError):
            gate.offer({**frame(), "stream": 5})

    async def test_overflow_detaches_without_false_ready_state(self):
        gate = LiveAdmission()
        gate.reset(1, {0: config()})
        gate.queue.max_bytes = 1
        self.assertFalse(gate.offer(frame()))
        self.assertTrue(gate.queue.detached)
        self.assertFalse(gate.active)
        self.assertFalse(gate.ready)

    async def test_protocol_accepts_only_supervisor_assigned_epoch(self):
        state = Protocol(2)
        with self.assertRaises(ValueError):
            state.accept(encode_frame(hello()))
        state.accept(encode_frame({**hello(), "generation": 2}))
        state.accept(encode_frame(config(2)))
        with self.assertRaises(ValueError):
            state.accept(encode_frame(frame()))
        state.accept(encode_frame(frame(generation=2)))


class FixtureTests(unittest.TestCase):
    def test_digest_binds_method_uri_user_and_nonce(self):
        md5 = lambda text: hashlib.md5(text.encode()).hexdigest()
        uri = "rtsp://127.0.0.1:1234/camera"
        digest = md5(f"{md5('fixture:' + REALM + ':fixture')}:{NONCE}:{md5('DESCRIBE:' + uri)}")
        header = f'Digest username="fixture", realm="{REALM}", nonce="{NONCE}", uri="{uri}", response="{digest}"'
        self.assertTrue(authorized(header, "DESCRIBE", uri))
        self.assertFalse(authorized(header, "PLAY", uri))
        self.assertFalse(authorized(header, "DESCRIBE", uri + "/other"))
        self.assertFalse(authorized(header.replace('username="fixture"', 'username="wrong"'), "DESCRIBE", uri))
        self.assertFalse(authorized("", "DESCRIBE", uri))

    def test_fu_a_fragments_reconstruct_original_nal(self):
        nal = b"\x65" + bytes(range(256)) * 12
        parts = fragments(nal)
        self.assertGreater(len(parts), 2)
        self.assertTrue(parts[0][1] & 0x80)
        self.assertTrue(parts[-1][1] & 0x40)
        self.assertTrue(all(len(part) <= 1000 for part in parts))
        reconstructed = bytes([(parts[0][0] & 0xE0) | (parts[0][1] & 31)]) + b"".join(p[2:] for p in parts)
        self.assertEqual(reconstructed, nal)

    def test_aud_units_and_annex_b_detection(self):
        data = bytes.fromhex("0000000109000000016501000001090000014102")
        units = access_units(data)
        self.assertEqual(len(units), 2)
        self.assertEqual([n[0] & 31 for n in units[0]], [9, 5])
        self.assertEqual(nal_types(data), {9, 5, 1})
        self.assertFalse(nal_types(b"\x65"))
        with self.assertRaises(ValueError):
            access_units(bytes.fromhex("0000000167"))


class CleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_reap_drains_paused_stdout_after_kill(self):
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "import os,time; os.write(1, b'x' * 1048576); time.sleep(60)",
            stdout=asyncio.subprocess.PIPE, limit=1024)
        try:
            # Enough buffered data to pause the reader; writer blocks on pipe capacity.
            await asyncio.wait_for(process.stdout.readexactly(1), 2)
            await asyncio.sleep(0.05)
            await asyncio.wait_for(stop_process(process), 2)
            self.assertIsNotNone(process.returncode)
        finally:
            await stop_process(process)

    async def test_overlapping_source_attempts_rejected(self):
        supervisor = SourceSupervisor("not-used")
        supervisor.running = True
        with self.assertRaises(ValueError):
            await supervisor.attempt(1234)


if __name__ == "__main__":
    unittest.main()
