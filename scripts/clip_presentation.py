"""Private diagnostic common-origin translations; never a shared-source publication."""
from dataclasses import dataclass
from fractions import Fraction

from packet_ingest_probe import Protocol
from packet_wire import decode_frame, encode_frame


@dataclass(frozen=True)
class Presentation:
    origin: Fraction
    frames: tuple


def translate(frames, policy):
    if policy not in ("earliest", "video"):
        raise ValueError("unknown presentation origin")
    if not frames or len(frames) > 266:
        raise ValueError("bounded finite recording required")
    protocol = Protocol(frames[0]["generation"])
    owned, size = [], 0
    for obj in frames:
        raw = encode_frame(obj)
        size += len(raw) + 4
        if size > 4 * 1024 * 1024:
            raise ValueError("presentation input budget")
        protocol.accept(raw)
        owned.append(decode_frame(raw))  # independent diagnostic metadata/payload representation
    if not protocol.ended:
        raise ValueError("presentation requires private finite end")
    streams = {s["index"]: s for s in owned if s["type"] == "stream"}
    packets = [p for p in owned if p["type"] == "packet"]
    videos = [s for s in streams.values() if s["media"] == "video"]
    if len(videos) != 1 or not packets or set(p["stream"] for p in packets) != set(streams):
        raise ValueError("presentation requires one video and all declared tracks")
    clocks = {i: Fraction(*s["time_base"]) for i, s in streams.items()}
    if any(p["pts"] is None or p["dts"] is None for p in packets):
        raise ValueError("presentation requires known clocks")
    first_video = next(p for p in packets if p["stream"] == videos[0]["index"])
    if not first_video["flags"] & 1:
        raise ValueError("presentation video must begin at admitted keyframe")
    origin = (min(min(p["pts"], p["dts"]) * clocks[p["stream"]] for p in packets)
              if policy == "earliest" else first_video["pts"] * clocks[first_video["stream"]])
    shifts = {i: origin / clock for i, clock in clocks.items()}
    if any(v.denominator != 1 for v in shifts.values()):
        raise ValueError("common origin is not exactly representable on every track")
    for packet in packets:
        for key in ("pts", "dts"):
            packet[key] -= int(shifts[packet["stream"]])
            if not -(2**63) < packet[key] < 2**63:
                raise ValueError("translated timestamp outside native range")
    check = Protocol(frames[0]["generation"])
    for obj in owned:
        check.accept(encode_frame(obj))
    return Presentation(origin, tuple(owned))
