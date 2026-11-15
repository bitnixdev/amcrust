#!/usr/bin/env python3
"""Compare exact common-origin translations with native mux edit lists and decoded output."""
import argparse
import asyncio
from fractions import Fraction
import json
from pathlib import Path
import struct
import tempfile

from av_output_probe import boxes, decoded_audio, decoded_video, mux, pack_wire, pcm_difference
from clip_presentation import translate
from ingest_probe import inspect
from recording_cut_probe import case
from rtsp_fixture import make_media


def payload(box):
    return box[16:] if box[:4] == b"\0\0\0\1" else box[8:]


def children(box):
    return list(boxes(payload(box)))


def one(items, kind):
    found = [b for k, b in items if k == kind]
    if len(found) != 1:
        raise ValueError("missing/duplicate presentation box")
    return found[0]


def field(box, version0, version1):
    data = payload(box)
    if not data or data[0] not in (0, 1):
        raise ValueError("unsupported/truncated presentation full box")
    offset = version0 if data[0] == 0 else version1
    if len(data) < offset + 4:
        raise ValueError("truncated presentation field")
    value = struct.unpack_from("!I", data, offset)[0]
    if not value:
        raise ValueError("zero track identity/timescale")
    return value


def edit_entries(box):
    data = payload(box)
    if len(data) < 8 or data[0] not in (0, 1) or data[1:4] != bytes(3):
        raise ValueError("unsupported/truncated edit list")
    count = struct.unpack_from("!I", data, 4)[0]
    size, fmt = (12, "!Iihh") if data[0] == 0 else (20, "!Qqhh")
    if not 0 < count <= 8 or len(data) != 8 + count * size:
        raise ValueError("invalid edit-list length/count")
    result = []
    for index in range(count):
        duration, media_time, rate, fraction = struct.unpack_from(fmt, data, 8 + index * size)
        if (rate, fraction) != (1, 0) or media_time < -1:
            raise ValueError("unsupported edit rate/media time")
        result.append({"duration": duration, "media_time": media_time})
    return result


def edits(path):
    moov = children(one(list(boxes(path.read_bytes())), b"moov"))
    movie_scale = field(one(moov, b"mvhd"), 12, 20)
    tracks = []
    for kind, box in moov:
        if kind != b"trak":
            continue
        track = children(box)
        identity = field(one(track, b"tkhd"), 12, 20)
        scale = field(one(children(one(track, b"mdia")), b"mdhd"), 12, 20)
        containers = [b for k, b in track if k == b"edts"]
        if len(containers) > 1:
            raise ValueError("duplicate edit container")
        entries = edit_entries(one(children(containers[0]), b"elst")) if containers else []
        tracks.append({"track_id": identity, "timescale": scale, "edits": entries})
    return {"movie_timescale": movie_scale, "tracks": tracks}


def difference(a, b):
    return pcm_difference(a, b) if len(a) == len(b) else {"sample_counts": [len(a) // 2, len(b) // 2]}


async def study(sink, directory, artifact, baseline, continuous):
    base_info, base_pcm = await inspect(baseline), await decoded_audio(baseline)
    base_video = await decoded_video(baseline)
    first = {i: next(p for p in artifact if p["type"] == "packet" and p["stream"] == i) for i in (0, 1)}
    lead = Fraction(first[0]["pts"], 90000) * 48000 - first[1]["pts"]
    if lead.denominator != 1 or lead < 0:
        raise ValueError("unsupported analytical video/audio boundary")
    cut = int(lead) * 2
    results = []
    for policy in ("earliest", "video"):
        presentation = translate(artifact, policy)
        output = directory / f"{policy}.mp4"
        await mux(sink, output, pack_wire(presentation.frames))
        info = await inspect(output)
        if await decoded_video(output) != base_video:
            raise ValueError("presentation changed decoded video")
        tracks = []
        for index in (0, 1):
            before = [p for p in base_info["packets"] if p["stream_index"] == index]
            after = [p for p in info["packets"] if p["stream_index"] == index]
            target = [p for p in presentation.frames if p["type"] == "packet" and p["stream"] == index]
            if len(before) != len(after) or [p["data_hash"] for p in before] != [p["data_hash"] for p in after]:
                raise ValueError("presentation changed selected encoded packets")
            tb = Fraction(info["streams"][index]["time_base"])
            source_tb = Fraction(1, 90000 if index == 0 else 48000)
            deltas = {str(b[key] * tb - a[key] * source_tb) for a, b in zip(target, after)
                      for key in ("pts", "dts")}
            if any(a["duration"] * source_tb != b["duration"] * tb for a, b in zip(target, after)):
                raise ValueError("presentation changed packet duration")
            if deltas != {"0"}:
                raise ValueError("mux rounded or otherwise changed translated clocks")
            tracks.append({"index": index, "packets": len(after), "first_pts": after[0]["pts"],
                           "time_base": str(tb), "mux_timestamp_deltas": sorted(deltas),
                           "first_packet_side_data": after[0].get("side_data_list", []),
                           "discard_packet_indices": [i for i, p in enumerate(after) if "D" in p.get("flags", "")]})
        pcm = await decoded_audio(output)
        expected_pcm = base_pcm if policy == "earliest" else base_pcm[cut:]
        if pcm != expected_pcm:
            raise ValueError("presentation PCM differs from declared whole-window/edit control")
        layout = edits(output)
        if layout["movie_timescale"] != 720000 or len(layout["tracks"]) != 2:
            raise ValueError("unexpected exact-clock movie layout")
        for index, track in enumerate(layout["tracks"]):
            rate = 90000 if index == 0 else 48000
            pts = tracks[index]["first_pts"]
            expected_edits = ([{"duration": pts * (720000 // rate), "media_time": -1}]
                              if pts > 0 else []) + [{"duration": 0, "media_time": max(0, -pts)}]
            if track != {"track_id": index + 1, "timescale": rate, "edits": expected_edits}:
                raise ValueError("mux edit list differs from declared common-origin policy")
        audio_side = tracks[1]["first_packet_side_data"]
        expected_side = ([] if policy == "earliest" else [{"side_data_type": "Skip Samples",
            "skip_samples": int(lead), "discard_padding": 0, "skip_reason": 0, "discard_reason": 0}])
        if audio_side != expected_side:
            raise ValueError("unexpected decoder skip/discard signaling")
        results.append({"policy": policy, "origin_seconds": str(presentation.origin), "tracks": tracks,
                        "edits": layout, "decoded_samples": len(pcm) // 2,
                        "versus_fresh_whole_window": difference(base_pcm, pcm),
                        "versus_fresh_from_video": difference(base_pcm[cut:], pcm),
                        "versus_continuous_from_video": difference(continuous[cut:], pcm)})
    return {"baseline_edits": edits(baseline), "leading_samples": int(lead), "alternatives": results}


async def run(worker, sink):
    with tempfile.TemporaryDirectory(prefix="amcrust-presentation-") as temporary:
        results = []
        for generation, (name, profile, offset, pns) in enumerate((
                ("main-delayed", "main", 0.25, None), ("high-aligned", "high", 0, None),
                ("main-delayed-no-pns", "main", 0.25, False)), 1):
            directory = Path(temporary) / name
            directory.mkdir()
            media = await make_media(directory, 320, 180, profile=profile, aac_pns=pns)
            result = await case(worker, sink, directory, media, generation, offset, "commit", presentation_check=study)
            result["name"] = name
            results.append(result)
        return {"cases": results, "scope": "private common-origin/edit diagnostics, not a production or Apple presentation policy"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--sink", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.worker.resolve(), args.sink.resolve())), indent=2))


if __name__ == "__main__":
    main()
