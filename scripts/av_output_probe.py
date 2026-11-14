#!/usr/bin/env python3
"""Synthetic packet IPC -> fragmented MP4 proof; no cameras or production changes."""
import argparse
import asyncio
from dataclasses import dataclass
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import struct
import tempfile

from ingest_probe import command, inspect, stop_process
from packet_ingest_probe import MAX_FRAME, Protocol, read_frame
from packet_wire import decode_frame, encode_frame
from packet_wire_faults import malformed_bodies
from rtsp_fixture import CameraFixture, make_media


@dataclass(frozen=True)
class InitSegment:
    generation: int
    data: bytes

    @property
    def identity(self):
        return self.generation, hashlib.sha256(self.data).digest()


@dataclass(frozen=True)
class Fragment:
    owner: tuple
    sequence: int
    data: bytes


def boxes(data):
    offset = 0
    while offset < len(data):
        if len(data) - offset < 8:
            raise ValueError("truncated MP4 box header")
        size, kind = struct.unpack_from("!I4s", data, offset)
        header = 8
        if size == 1:
            if len(data) - offset < 16:
                raise ValueError("truncated extended MP4 box")
            size = struct.unpack_from("!Q", data, offset + 8)[0]
            header = 16
        if size < header or size > MAX_FRAME or offset + size > len(data):
            raise ValueError("invalid/oversized MP4 box")
        yield kind, data[offset:offset + size]
        offset += size


def split_recording(generation, data):
    init_parts, fragments = [], []
    seen = []
    init = None
    pending = None
    trailer = False
    for kind, box in boxes(data):
        if kind == b"moof":
            if pending is not None or trailer:
                raise ValueError("invalid fragment boundary")
            if init is None:
                if seen.count(b"ftyp") != 1 or seen.count(b"moov") != 1 or seen[0] != b"ftyp":
                    raise ValueError("missing/duplicate initialization")
                init = InitSegment(generation, b"".join(init_parts))
            pending = box
        elif kind == b"mdat":
            if pending is None:
                raise ValueError("media without fragment header")
            if len(pending) + len(box) > MAX_FRAME:
                raise ValueError("fragment limit exceeded")
            fragments.append(Fragment(init.identity, len(fragments) + 1, pending + box))
            pending = None
        elif init is None and kind in (b"ftyp", b"moov", b"free"):
            init_parts.append(box)
            seen.append(kind)
            if sum(map(len, init_parts)) > MAX_FRAME:
                raise ValueError("initialization limit exceeded")
        elif kind == b"mfra" and pending is None and fragments and not trailer:
            trailer = True  # optional seek index, not part of HDS media fragments
        else:
            raise ValueError("unexpected MP4 boundary")
    if init is None or pending is not None or not fragments:
        raise ValueError("incomplete fragmented recording")
    return init, fragments


def assemble(init, fragments):
    if not fragments:
        raise ValueError("empty recording")
    if fragments[0].sequence < 1:
        raise ValueError("invalid fragment sequence")
    for sequence, fragment in enumerate(fragments, fragments[0].sequence):
        if fragment.owner != init.identity or fragment.sequence != sequence:
            raise ValueError("mixed generation/init or missing/reordered fragment")
    return init.data + b"".join(fragment.data for fragment in fragments)


def unpack_wire(data, generation):
    protocol = Protocol(generation)
    frames = []
    offset = 0
    while offset < len(data):
        if len(data) - offset < 4:
            raise ValueError("truncated IPC")
        size = struct.unpack_from("!I", data, offset)[0]
        offset += 4
        if not 0 < size <= MAX_FRAME or offset + size > len(data):
            raise ValueError("invalid IPC length")
        frame = data[offset:offset + size]
        protocol.accept(frame)
        frames.append(decode_frame(frame))
        offset += size
    if not protocol.ended:
        raise ValueError("missing IPC end")
    return frames


def pack_wire(frames):
    result = []
    for obj in frames:
        raw = encode_frame(obj)
        if len(raw) > MAX_FRAME:
            raise ValueError("IPC length limit")
        result += [struct.pack("!I", len(raw)), raw]
    return b"".join(result)


async def mux(sink, output, wire, success=True, failure_contains=None):
    existing = output.read_bytes() if output.exists() else None
    process = await asyncio.create_subprocess_exec(
        str(sink), str(output), stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        _, errors = await asyncio.wait_for(process.communicate(wire), 15)
        if success and process.returncode:
            raise ValueError("native mux failed: " + errors.decode(errors="replace")[:4096])
        if not success:
            if not process.returncode:
                raise ValueError("invalid input accepted")
            if failure_contains is not None and failure_contains not in errors.decode(errors="replace"):
                raise ValueError("unexpected rejection reason: " + errors.decode(errors="replace")[:4096])
            if existing is None and output.exists():
                raise ValueError("partial output retained")
            if existing is not None and output.read_bytes() != existing:
                raise ValueError("existing output modified")
    finally:
        await stop_process(process)


async def decoded_video(path):
    video = await command("ffmpeg", "-nostdin", "-v", "error", "-xerror", "-i", str(path),
                          "-map", "0:v:0", "-f", "framemd5", "-")
    # Frame hashes/sizes verify image content/order; timing is checked separately.
    video = [tuple(line.split(",")[-2:]) for line in video.decode().splitlines()
             if line and not line.startswith("#")]
    return video


async def decoded_audio(path):
    return await command("ffmpeg", "-nostdin", "-v", "error", "-xerror", "-i", str(path),
                         "-map", "0:a:0", "-c:a", "pcm_s16le", "-f", "s16le", "-")


async def decoded(path):
    return await decoded_video(path), await decoded_audio(path)


def compare_timing(source, output):
    worst = Fraction(0)
    if len(source["streams"]) != len(output["streams"]):
        raise ValueError("stream count changed")
    for src_stream, dst_stream in zip(source["streams"], output["streams"]):
        if src_stream["codec_name"] != dst_stream["codec_name"]:
            raise ValueError("codec changed")
        src = [p for p in source["packets"] if p["stream_index"] == src_stream["index"]]
        dst = [p for p in output["packets"] if p["stream_index"] == dst_stream["index"]]
        if len(src) != len(dst):
            raise ValueError(f"{src_stream['codec_type']} packet count changed: {len(src)} -> {len(dst)}")
        a, b = Fraction(src_stream["time_base"]), Fraction(dst_stream["time_base"])
        for index, (left, right) in enumerate(zip(src, dst)):
            if left["data_hash"] != right["data_hash"]:
                raise ValueError("encoded packet payload changed")
            for field in ("pts", "dts", "duration"):
                if field not in left or field not in right:
                    raise ValueError("missing timestamp/duration after mux")
                delta = abs(left[field] * a - right[field] * b)
                worst = max(worst, delta)
                if delta > max(a, b):
                    raise ValueError(f"{src_stream['codec_type']} packet {index} {field} changed: "
                                     f"{left[field] * a} -> {right[field] * b}")
    return float(worst)


def pcm_difference(reference, actual):
    if len(reference) != len(actual) or len(reference) % 2:
        raise ValueError("PCM sample count changed")
    first = last = None
    count = maximum = 0
    for offset in range(0, len(reference), 2):
        if reference[offset:offset + 2] != actual[offset:offset + 2]:
            first = offset // 2 if first is None else first
            last = offset // 2
            count += 1
            maximum = max(maximum, abs(int.from_bytes(reference[offset:offset + 2], "little", signed=True) -
                                       int.from_bytes(actual[offset:offset + 2], "little", signed=True)))
    return {"different_samples": count, "first": first, "last": last, "max_abs_s16_error": maximum}



def compare_audio(source_audio, output_audio, reference, channels):
    """Permit only the known terminal AAC discard samples, never drift/front loss.

    Streaming fMP4 may omit final discard-padding signaling even when trun sample
    durations remain exact. Keep this limitation visible, with a direct mux control.
    """
    audio_index = next(s["index"] for s in reference["streams"] if s["codec_type"] == "audio")
    packets = [p for p in reference["packets"] if p["stream_index"] == audio_index]
    if not packets or any(side.get("discard_padding", 0) for p in packets[:-1]
                          for side in p.get("side_data_list", [])):
        raise ValueError("unsupported nonterminal AAC discard metadata")
    padding = sum(side.get("discard_padding", 0) for side in packets[-1].get("side_data_list", []))
    extra = len(output_audio) - len(source_audio)
    if output_audio[:len(source_audio)] != source_audio or extra not in (0, padding * channels * 2):
        raise ValueError(f"decoded PCM differs beyond declared tail padding: {len(source_audio)} -> {len(output_audio)} bytes, padding={padding}")
    return extra // (channels * 2)


async def fixture(directory, generation, width, bframes, rate, channels, offset):
    path = directory / f"source-{generation}.mp4"
    await command("ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
                  f"testsrc2=size={width}x{width * 9 // 16}:rate=15", "-itsoffset", str(offset),
                  "-f", "lavfi", "-i", f"sine=frequency=1000:sample_rate={rate}", "-t", "3",
                  "-c:v", "libx264", "-preset", "ultrafast", "-g", "15", "-bf", str(bframes),
                  "-pix_fmt", "yuv420p", "-c:a", "aac", "-ac", str(channels), str(path))
    return path


async def reject_invalid_inputs(sink, directory, frames):
    def changed(kind, field, value, occurrence=0):
        copy = json.loads(json.dumps(frames))
        targets = [obj for obj in copy if obj["type"] == kind]
        targets[occurrence][field] = value
        return pack_wire(copy)

    packets = [obj for obj in frames if obj["type"] == "packet"]
    video = [i for i, obj in enumerate(packets) if obj["stream"] == 0]
    variants = {
        "mixed": changed("packet", "generation", 2, 5),
        "missing-end": pack_wire(frames[:-1]),
        "truncated": pack_wire(frames)[:-1],
        "oversized": struct.pack("!I", MAX_FRAME + 1),
        "sequence": changed("packet", "sequence", 0, 5),
        "end-count": changed("end", "packets", len(packets) + 1),
        "rollback": changed("packet", "dts", packets[video[0]]["dts"], video[1]),
        "missing-pts": changed("packet", "pts", None),
        "unsigned-overflow": changed("packet", "pts", 1 << 63),
        "fractional-pts": changed("packet", "pts", 1.5),
        "non-key-start": changed("packet", "flags", 0, video[0]),
        "bad-clock": changed("stream", "time_base", [1, 0]),
    }
    # Corrupt a packet after output creation so rejection also exercises removal
    # of partial output, not only refusal before opening the destination.
    target = next(i for i, frame in enumerate(frames)
                  if frame["type"] == "packet" and frame["sequence"] == 5)
    prefix, suffix = pack_wire(frames[:target]), pack_wire(frames[target + 1:])
    for name, raw in malformed_bodies(encode_frame(frames[target])).items():
        variants[f"binary-{name}"] = prefix + struct.pack("!I", len(raw)) + raw + suffix
    for name, wire in variants.items():
        await mux(sink, directory / f"invalid-{name}.mp4", wire, success=False)
    existing = directory / "existing.mp4"
    existing.write_bytes(b"do not replace")
    await mux(sink, existing, pack_wire(frames), success=False)
    return len(variants) + 1


async def rtsp_timing_gate(worker, sink, directory):
    media = await make_media(directory, 320, 180)
    frames, counts = [], [0, 0]
    protocol = Protocol(3)
    # A deliberate finite clip boundary while the camera session is still active,
    # not converting a failed or disconnected source into a successful recording.
    async with CameraFixture(media, "stall") as camera:
        process = await asyncio.create_subprocess_exec(
            str(worker), "--rtsp-fixture", str(camera.port), "3",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            while counts[0] < 30 or counts[1] < 94:
                raw = await asyncio.wait_for(read_frame(process.stdout), 4)
                if raw is None:
                    raise ValueError("RTSP source ended before deliberate clip boundary")
                obj = protocol.accept(raw)
                frames.append(obj)
                if obj["type"] == "packet":
                    counts[obj["stream"]] += 1
            if process.returncode is not None:
                raise ValueError("RTSP worker exited before clip boundary")
        finally:
            await stop_process(process)
    frames.append({"type": "end", "generation": 3, "packets": protocol.sequence})
    packets = [f for f in frames if f["type"] == "packet"]
    if any(p["pts"] is None or p["dts"] is None or p["duration"] <= 0 for p in packets):
        details = {index: {"missing_pts": sum(p["pts"] is None for p in packets if p["stream"] == index),
                           "missing_dts": sum(p["dts"] is None for p in packets if p["stream"] == index),
                           "durations": sorted({p["duration"] for p in packets if p["stream"] == index}),
                           "first": [(p["pts"], p["dts"], p["duration"]) for p in packets if p["stream"] == index][:3]}
                   for index in protocol.streams}
        await mux(sink, directory / "unsafe-rtsp.mp4", pack_wire(frames), success=False,
                  failure_contains="missing packet timestamp")
        return {"status": "blocked_missing_timestamps", "tracks": details,
                "unsafe_mux_rejected": True,
                "next": "explicit timestamp normalization with a common A/V epoch"}
    raise ValueError("RTSP fixture no longer exercises missing timestamps; re-evaluate its clock policy")


async def run(worker, sink):
    with tempfile.TemporaryDirectory(prefix="amcrust-av-") as temp:
        directory = Path(temp)
        results, recordings, captures = [], [], []
        for generation, width, bframes, rate, channels, offset in (
                (1, 320, 0, 48000, 1, 0), (2, 640, 2, 32000, 2, 0.25)):
            source = await fixture(directory, generation, width, bframes, rate, channels, offset)
            wire = await command(str(worker), "--file-generation", str(generation), str(source))
            frames = unpack_wire(wire, generation)
            captures.append(frames)
            path = directory / f"output-{generation}.mp4"
            await mux(sink, path, wire)
            original = await inspect(source)
            output = await inspect(path)
            worst = compare_timing(original, output)
            source_video, source_audio = await decoded(source)
            video, audio = await decoded(path)
            if video != source_video:
                raise ValueError("decoded video changed")
            tail_samples = compare_audio(source_audio, audio, original, channels)
            # Independent direct-copy control distinguishes muxer semantics from
            # information lost crossing our IPC/packet reconstruction boundary.
            clock = math.lcm(*(Fraction(s["time_base"]).denominator for s in original["streams"]))
            control = directory / f"control-{generation}.mp4"
            await command("ffmpeg", "-nostdin", "-v", "error", "-i", str(source), "-map", "0:v:0",
                          "-map", "0:a:0", "-c", "copy", "-movflags",
                          "frag_keyframe+delay_moov+default_base_moof", "-use_editlist", "1",
                          "-movie_timescale", str(clock), "-avoid_negative_ts", "disabled", str(control))
            if await decoded(control) != (video, audio):
                raise ValueError("IPC mux differs from independent direct-copy control")
            compare_timing(output, await inspect(control))
            init, fragments = split_recording(generation, path.read_bytes())
            recordings.append((init, fragments))
            if len(fragments) < 2:
                raise ValueError("fixture did not produce multiple fragments")
            independently_decoded = []
            for fragment in fragments:
                standalone = directory / f"fragment-{generation}-{fragment.sequence}.mp4"
                standalone.write_bytes(assemble(init, [fragment]))
                fragment_info = await inspect(standalone)
                first_video = next(p for p in fragment_info["packets"] if p["stream_index"] == 0)
                if "K" not in first_video["flags"]:
                    raise ValueError("fragment does not begin with a video keyframe")
                fragment_video, _ = await decoded(standalone)
                independently_decoded.extend(fragment_video)
            if independently_decoded != video:
                raise ValueError("independent fragment video decoding differs from full clip")
            rebuilt = directory / f"rebuilt-{generation}.mp4"
            rebuilt.write_bytes(assemble(init, fragments))
            if await decoded(rebuilt) != (video, audio):
                raise ValueError("fragment reconstruction changed decoded media")
            results.append({"generation": generation, "bframes": bframes, "width": width,
                            "sample_rate": rate, "channels": channels, "audio_offset": offset,
                            "fragments": len(fragments), "video_frames": len(video),
                            "pcm_samples_per_channel": len(audio) // (2 * channels),
                            "maximum_timestamp_error_seconds": worst,
                            "video_equal_and_audio_prefix_exact": True,
                            "direct_copy_control_equal": True,
                            "each_fragment_video_independently_decodable": True,
                            "unrepresented_terminal_aac_padding_samples": tail_samples})
        try:
            assemble(recordings[0][0], recordings[1][1])
        except ValueError:
            pass
        else:
            raise ValueError("mixed initialization/fragments accepted")
        rejected = await reject_invalid_inputs(sink, directory, captures[0])
        rtsp_result = await rtsp_timing_gate(worker, sink, directory)
        return {"cases": results, "rtsp": rtsp_result, "native_rejection_cases": rejected, "mixed_generation_rejected": True,
                "incomplete_output_removed": True, "scope": "synthetic A/V packet-copy fMP4; not HomeKit acceptance"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--sink", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(asyncio.wait_for(run(args.worker.resolve(), args.sink.resolve()), 90)), indent=2))
