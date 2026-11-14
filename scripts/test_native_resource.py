import asyncio
import json
import signal
import struct
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from native_resource_probe import MeteredPeer, acquire, resource_report, rss_rows, run
from packet_wire import encode_frame
from shared_admission import LateAdmission
from test_h264_reorder import descriptor
from test_packet_probe import hello
from test_shared_admission import feed
from rtsp_fixture import CameraFixture


def report(**changes):
    values = dict(user_cpu_seconds=0.125, system_cpu_seconds=0.025, peak_rss_bytes=1048576)
    values.update(changes)
    return b"packet worker: input read failed\nAMC_RESOURCE " + json.dumps(values).encode() + b"\n"


class ResourceParsingTests(unittest.TestCase):
    def test_native_report_preserves_units(self):
        self.assertEqual(resource_report(report())["peak_rss_bytes"], 1048576)
        self.assertEqual(resource_report(report())["user_cpu_seconds"], 0.125)

    def test_bad_reports_are_not_zero_filled_or_silently_accepted(self):
        for data in (b"", report() + report(), b"extra error\n" + report(), b"x" * 65537,
                     report(user_cpu_seconds=-1), report(user_cpu_seconds=float("nan")),
                     report(system_cpu_seconds=True), report(peak_rss_bytes=0), report(extra=1)):
            with self.assertRaises(ValueError):
                resource_report(data)

    def test_rss_rows_are_pid_only_and_kib_to_bytes(self):
        self.assertEqual(rss_rows(" 12 1024\n 34 2048\n", {12, 34}), {12: 1048576, 34: 2097152})
        self.assertEqual(rss_rows("12 1\n", {12, 34}), {12: 1024})  # exited PID may be absent
        for text in ("12 1 command", "12 -1", "99 12", "12 1\n12 2", "bad row"):
            with self.assertRaises(ValueError):
                rss_rows(text, {12, 34})


class ResourceCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_workload_fingerprint_ignores_generation_but_not_clocks(self):
        async def fingerprint(generation, timestamp):
            gate = LateAdmission(generation)
            gate.feed(encode_frame({**hello(), "generation": generation}))
            gate.feed(encode_frame({**descriptor(), "generation": generation}))
            frame = feed(gate, 0, generation=generation, pts=timestamp, dts=timestamp)
            peer = MeteredPeer(gate.join())
            peer.expect(frame)
            await asyncio.sleep(0)
            gate.close("source_failed")
            await peer.task
            return peer.summary()["workload_sha256"]
        first = await fingerprint(1, 0)
        self.assertEqual(first, await fingerprint(2, 0))
        self.assertNotEqual(first, await fingerprint(3, 6000))

    async def test_eof_retires_gate_before_exit_report_collection(self):
        gate = LateAdmission(1)
        async def exited():
            self.assertFalse(gate._epoch.active)
            return 1
        process = SimpleNamespace(stdout=object(), returncode=1, wait=AsyncMock(side_effect=exited))
        with patch("native_resource_probe.LateAdmission", return_value=gate), \
                patch("native_resource_probe.asyncio.create_subprocess_exec", AsyncMock(return_value=process)), \
                patch("native_resource_probe.read_frame", AsyncMock(return_value=None)), \
                patch("native_resource_probe.stop_process", AsyncMock()) as stop:
            with self.assertRaisesRegex(ValueError, "resource report"):
                await acquire("worker", 1234, 1, 1, 3, [])  # missing report remains a failure
            process.wait.assert_awaited_once()
            stop.assert_awaited_once_with(process)

    async def test_cancelled_native_read_reaps_its_worker(self):
        process = SimpleNamespace(stdout=object(), returncode=None)
        processes = []
        with patch("native_resource_probe.asyncio.create_subprocess_exec", AsyncMock(return_value=process)), \
                patch("native_resource_probe.read_frame", AsyncMock(side_effect=asyncio.CancelledError)), \
                patch("native_resource_probe.stop_process", AsyncMock()) as stop:
            with self.assertRaises(asyncio.CancelledError):
                await acquire("worker", 1234, 1, 1, 3, processes)
            stop.assert_awaited_once_with(process)
        self.assertEqual(processes, [process])

    async def test_failed_fixture_startup_stops_its_isolated_process_group(self):
        for returncode in (None, -9):
            process = SimpleNamespace(stdout=SimpleNamespace(readline=AsyncMock(return_value=b"bad json")),
                                      pid=12345, returncode=returncode)
            with patch("native_resource_probe.asyncio.create_subprocess_exec", AsyncMock(return_value=process)) as spawn, \
                    patch("native_resource_probe.os.killpg") as kill, \
                    patch("native_resource_probe.stop_process", AsyncMock()) as stop:
                with self.assertRaises(ValueError):
                    await run("worker", "shared", 1, 3)
                self.assertTrue(spawn.call_args.kwargs["start_new_session"])
                kill.assert_called_once_with(12345, signal.SIGKILL)
                stop.assert_awaited_once_with(process)


class FixtureCounterTests(unittest.IsolatedAsyncioTestCase):
    async def test_each_connection_starts_its_own_rtcp_counters(self):
        class Writer:
            def __init__(self):
                self.frames = []
            def write(self, frame):
                self.frames.append(frame)
            async def drain(self):
                pass
            def close(self):
                pass

        camera = CameraFixture(([[b"\x65abc"]], [b"aac"]), clocked=True, send_seconds=0.01)
        for _ in range(2):
            writer = Writer()
            await camera._send(writer, {0: 0, 1: 2})
            reports = [frame for frame in writer.frames if frame[1] in (1, 3)]
            self.assertEqual(len(reports), 2)
            self.assertEqual([struct.unpack("!I", frame[24:28])[0] for frame in reports], [0, 0])
        self.assertEqual(camera.sent, [2, 2])  # fixture diagnostics still aggregate
