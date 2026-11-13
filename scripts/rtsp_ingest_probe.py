#!/usr/bin/env python3
"""Loopback-only RTSP fault/generation/late-join experiment; no production cameras."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import time

from ingest_probe import command, stop_process
from packet_ingest_probe import PacketQueue, Protocol, integer, read_frame
from rtsp_fixture import CameraFixture, make_media, nals


def nal_types(data):
    # This fixture uses Annex B from libavformat's RTSP demuxer, not AVCC samples.
    if not data.startswith((b"\x00\x00\x01", b"\x00\x00\x00\x01")):
        return set()
    return {nal[0] & 31 for nal in nals(data)}


class LiveAdmission:
    """Generation-scoped Annex-B IDR admission, with no stale queue/cache reuse."""
    def __init__(self):
        self.generation = None
        self.queue = PacketQueue(8 * 1024 * 1024)
        self.active = False
        self.ready = False
        self.skipped = 0

    def reset(self, generation, streams):
        self.invalidate("generation_changed")
        integer(generation, 1, (1 << 31) - 1)
        if not streams or any(s["generation"] != generation for s in streams.values()):
            raise ValueError("stale stream descriptors")
        video = [s for s in streams.values() if s["codec"] == "h264"]
        if len(video) != 1:
            raise ValueError("fixture expects one H.264 stream")
        self.video = video[0]
        self.generation = generation
        self.streams = streams.copy()
        self.parameters = nal_types(bytes.fromhex(self.video["extradata"])) & {7, 8}
        self.queue = PacketQueue(8 * 1024 * 1024)
        self.active = True
        self.ready = False
        self.skipped = 0

    def invalidate(self, reason="cancelled"):
        self.queue.detach(reason)
        self.active = False
        self.ready = False
        self.parameters = set()
        self.streams = {}
        self.video = None
        self.skipped = 0

    def offer(self, packet):
        if packet["generation"] != self.generation:
            raise ValueError("stale packet generation")
        if not self.active or self.queue.detached:
            return False
        if packet["stream"] not in self.streams:
            raise ValueError("unknown packet stream")
        is_video = packet["stream"] == self.video["index"]
        if not self.ready:
            types = nal_types(bytes.fromhex(packet["data"])) if is_video else set()
            # Parameter sets must accompany the admitted IDR or its descriptor;
            # do not remember a parameter-set-only packet that we discarded.
            if not (is_video and packet["flags"] & 1 and 5 in types
                    and {7, 8} <= (self.parameters | types)):
                self.skipped += 1
                return False
            for descriptor in self.streams.values():
                self.queue.offer(json.dumps(descriptor).encode())
            self.ready = True
        self.queue.offer(json.dumps(packet).encode())
        if self.queue.detached:
            self.active = False
            self.ready = False
            return False
        return True


class SourceSupervisor:
    """Bounded, explicitly reopened source attempts for this fault experiment.

    No production retry/backoff or privacy lifecycle yet. Native workers are reaped
    and consumers invalidated before a new generation is assigned. PTS fallback is
    only valid here because the synthetic H.264 source deliberately has no B-frames.
    """
    def __init__(self, worker):
        self.worker = worker
        self.generation = 0
        self.admission = LiveAdmission()
        self.running = False

    async def attempt(self, port):
        if self.running:
            raise ValueError("source attempt already running")
        self.running = True
        self.generation += 1
        epoch = self.generation
        protocol = Protocol(epoch)
        video_count = 0
        audio_count = 0
        audio_hashes = []
        previous = {}
        admitted = []
        first_admitted = None
        stale_sample = None
        reason = None
        process = None
        started = time.monotonic()
        descriptor = None
        close_reason = "source_failed"
        self.admission.invalidate("generation_changed")
        try:
            with tempfile.TemporaryFile() as errors:
                process = await asyncio.create_subprocess_exec(
                    str(self.worker), "--rtsp-fixture", str(port), str(epoch),
                    stdout=asyncio.subprocess.PIPE, stderr=errors)
                while True:
                    try:
                        raw = await asyncio.wait_for(read_frame(process.stdout), 4 if protocol.hello is None else 2.5)
                    except asyncio.TimeoutError:
                        reason = "supervisor_timeout"
                        break
                    if raw is None:
                        await asyncio.wait_for(process.wait(), 2)
                        if process.returncode == 0:
                            raise ValueError("live EOF incorrectly reported success")
                        reason = "worker_failure"
                        break
                    obj = protocol.accept(raw)
                    if obj["type"] == "end":
                        raise ValueError("live stream emitted finite-media end marker")
                    if obj["type"] != "packet":
                        continue
                    stale_sample = obj
                    stream = protocol.streams[obj["stream"]]
                    timestamp = obj["dts"] if obj["dts"] is not None else obj["pts"]
                    if timestamp is not None:
                        if obj["stream"] in previous and timestamp < previous[obj["stream"]]:
                            reason = "timestamp_rollback"
                            break
                        previous[obj["stream"]] = timestamp
                    if stream["media"] == "video":
                        video_count += 1
                    else:
                        audio_count += 1
                        audio_hashes.append(hashlib.sha256(bytes.fromhex(obj["data"])).hexdigest())
                        if stream["time_base"] != [1, 48000]:
                            raise ValueError("unexpected AAC clock")
                    # Join well after the initial IDR, while the source is running.
                    if video_count == 7 and not self.admission.active:
                        self.admission.reset(epoch, protocol.streams)
                        descriptor = self.admission.video
                    if self.admission.active and self.admission.offer(obj):
                        # Drain a healthy viewer immediately. Stalled-subscriber
                        # bounds are exercised separately by the fanout unit suite.
                        while not self.admission.queue.queue.empty():
                            await self.admission.queue.receive()
                        if stream["media"] == "video":
                            if first_admitted is None:
                                first_admitted = obj
                            admitted.append(bytes.fromhex(obj["data"]))
                return {"generation": epoch, "reason": reason, "video_packets": video_count,
                        "audio_packets": audio_count, "audio_hashes": audio_hashes, "elapsed": time.monotonic() - started,
                        "descriptor": descriptor, "admitted": admitted, "first_admitted": first_admitted,
                        "skipped_before_idr": self.admission.skipped, "stale_sample": stale_sample}
        except asyncio.CancelledError:
            close_reason = "cancelled"
            raise
        finally:
            # Wake old consumers and clear media before waiting on worker cleanup.
            self.admission.invalidate(close_reason)
            if process is not None:
                await stop_process(process)
            self.running = False


async def check_media(result, directory, width, audio):
    if (result["video_packets"] < 20 or result["audio_packets"] < 30
            or not result["admitted"] or not result["skipped_before_idr"]):
        raise ValueError("fixture did not exercise audio/video and late admission")
    expected_audio = [hashlib.sha256(frame).hexdigest() for frame in audio[:result["audio_packets"]]]
    if result["audio_hashes"] != expected_audio:
        raise ValueError("AAC payload changed during RTP depacketization")
    if result["descriptor"]["width"] != width:
        raise ValueError("stale/wrong codec descriptor")
    if 5 not in nal_types(bytes.fromhex(result["first_admitted"]["data"])):
        raise ValueError("late viewer did not start at IDR")
    path = directory / f"admitted-{result['generation']}.h264"
    path.write_bytes(bytes.fromhex(result["descriptor"]["extradata"]) + b"".join(result["admitted"]))
    await command("ffmpeg", "-nostdin", "-v", "error", "-xerror", "-f", "h264",
                  "-i", str(path), "-f", "null", "-")


def report(result):
    return {key: value for key, value in result.items()
            if key not in ("descriptor", "admitted", "first_admitted", "stale_sample", "audio_hashes")}


async def run(worker):
    with tempfile.TemporaryDirectory(prefix="amcrust-rtsp-") as temp:
        directory = Path(temp)
        small = await make_media(directory, 320, 180)
        large = await make_media(directory, 640, 360)
        supervisor = SourceSupervisor(worker)
        results = []
        async with CameraFixture(small) as camera:
            first = await supervisor.attempt(camera.port)
            await check_media(first, directory, 320, small[1])
            if first["reason"] != "worker_failure":
                raise ValueError("disconnect did not terminate native acquisition")
            if supervisor.admission.active or not supervisor.admission.queue.detached:
                raise ValueError("disconnect left consumer active")
            # Same endpoint, new encoding/SPS/PPS and a live-socket media stall.
            camera.video, camera.audio = large
            camera.mode = "stall"
            second = await supervisor.attempt(camera.port)
            await check_media(second, directory, 640, large[1])
            if (second["generation"] != first["generation"] + 1
                    or first["descriptor"]["extradata"] == second["descriptor"]["extradata"]):
                raise ValueError("reconnect did not replace codec generation")
            if second["reason"] not in ("worker_failure", "supervisor_timeout") or second["elapsed"] > 7:
                raise ValueError("silent media stall did not terminate promptly")
            supervisor.admission.reset(second["generation"], {s["index"]: s for s in [second["descriptor"]]})
            try:
                supervisor.admission.offer(first["stale_sample"])
            except ValueError:
                pass
            else:
                raise ValueError("accepted previous-generation packet")
            supervisor.admission.invalidate()
            if camera.connections != 2 or camera.played != 2 or camera.challenges < 2 or not camera.authenticated:
                raise ValueError("RTSP authentication/reconnect path not exercised")
            results += [report(first), report(second)]
        async with CameraFixture(small, "rollback") as camera:
            rollback = await supervisor.attempt(camera.port)
            if rollback["reason"] != "timestamp_rollback":
                raise ValueError("RTP timestamp rollback was hidden/ignored")
            results.append(report(rollback))
        for mode in ("silent", "reject"):
            async with CameraFixture(small, mode) as camera:
                failed = await supervisor.attempt(camera.port)
                if failed["video_packets"] or failed["audio_packets"] or failed["elapsed"] > 7:
                    raise ValueError("failed source produced media or exceeded deadline")
                if mode == "silent" and camera.played != 1:
                    raise ValueError("silent PLAY was not exercised")
                if mode == "reject" and (camera.played or not camera.challenges):
                    raise ValueError("authentication rejection not exercised")
                results.append({"mode": mode, **report(failed)})
        return {"attempts": results, "reconnect_codec_change": True, "late_idr_video_decode": True,
                "scope": "loopback RTSP H264/AAC; no real camera/HomeKit or A/V sync proof"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(asyncio.wait_for(run(args.worker.resolve()), 60)), indent=2))
