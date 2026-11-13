"""Conservative Main/High no-reordering evidence, not a general AVC decoder.

Only Annex-B, one SPS/PPS, progressive 8-bit 4:2:0, POC types 0/2 and an explicit
VUI num_reorder_frames=0 are admitted. Unsupported/absent evidence grants no repair.
"""
from dataclasses import dataclass
import re
from typing import Optional


class Bits:
    def __init__(self, data):
        self.data, self.offset = data, 0

    def read(self, count=1):
        if not 0 <= count <= 32 or self.offset + count > len(self.data) * 8:
            raise ValueError("truncated H264 syntax")
        value = 0
        for _ in range(count):
            value = (value << 1) | ((self.data[self.offset // 8] >> (7 - self.offset % 8)) & 1)
            self.offset += 1
        return value

    def ue(self, maximum=(1 << 31) - 1):
        zeros = 0
        while not self.read():
            zeros += 1
            if zeros > 31:
                raise ValueError("oversized Exp-Golomb code")
        value = (1 << zeros) - 1 + self.read(zeros)
        if value > maximum:
            raise ValueError("H264 syntax value out of range")
        return value

    def se(self):
        value = self.ue()
        return (value + 1) // 2 if value & 1 else -(value // 2)

    def finish(self):
        remaining = len(self.data) * 8 - self.offset
        if not 1 <= remaining <= 8 or self.read() != 1:
            raise ValueError("invalid SPS trailing bits")
        while self.offset < len(self.data) * 8:
            if self.read():
                raise ValueError("invalid SPS trailing padding")


def rbsp(nal, limit=None):
    if not nal or nal[0] & 0x80:
        raise ValueError("invalid NAL header")
    result = bytearray()
    zeros, index = 0, 1
    while index < len(nal) and (limit is None or len(result) < limit):
        byte = nal[index]
        if zeros == 2:
            if byte == 3:
                if index + 1 == len(nal) or nal[index + 1] > 3:
                    raise ValueError("invalid emulation prevention")
                zeros = 0
                index += 1
                continue
            if byte < 3:
                raise ValueError("unescaped start-code bytes")
        result.append(byte)
        zeros = zeros + 1 if byte == 0 else 0
        index += 1
    return bytes(result)


def annexb(data):
    matches = re.finditer(b"\x00\x00(?:\x00)?\x01", data)
    previous = next(matches, None)
    if previous is None or previous.start() != 0:
        raise ValueError("Annex-B required for reorder evidence")
    result = []
    for following in matches:
        result.append(data[previous.end():following.start()].rstrip(b"\0"))
        if len(result) >= 256:
            raise ValueError("NAL count limit")
        previous = following
    result.append(data[previous.end():].rstrip(b"\0"))
    if any(not nal or nal[0] & 0x80 for nal in result):
        raise ValueError("invalid NAL unit")
    return result


def hrd(bits):
    count = bits.ue(31) + 1
    bits.read(8)
    for _ in range(count):
        bits.ue()
        bits.ue()
        bits.read()
    bits.read(20)


def vui(bits):
    if bits.read():  # aspect_ratio_info_present_flag
        if bits.read(8) == 255:
            if not bits.read(16) or not bits.read(16):
                raise ValueError("invalid extended SAR")
    if bits.read():
        bits.read()  # overscan
    if bits.read():
        bits.read(4)  # video_format + full_range
        if bits.read():
            bits.read(24)  # colour description
    if bits.read():
        bits.ue(5)
        bits.ue(5)
    if bits.read():
        if not bits.read(32) or not bits.read(32):
            raise ValueError("invalid VUI timing")
        bits.read()
    nal_hrd = bits.read()
    if nal_hrd:
        hrd(bits)
    vcl_hrd = bits.read()
    if vcl_hrd:
        hrd(bits)
    if nal_hrd or vcl_hrd:
        bits.read()
    bits.read()  # pic_struct_present_flag
    if not bits.read():
        return None, None
    bits.read()  # motion_vectors_over_pic_boundaries_flag
    for _ in range(4):
        bits.ue(16)
    reorder, buffering = bits.ue(16), bits.ue(16)
    if reorder > buffering:
        raise ValueError("contradictory reorder/DPB bounds")
    return reorder, buffering


@dataclass(frozen=True)
class SPS:
    identifier: int
    profile: int
    width: int
    height: int
    reorder: Optional[int]


def parse_sps(nal):
    if not 4 <= len(nal) <= 4096 or nal[0] & 31 != 7 or not nal[0] & 0x60:
        raise ValueError("invalid/oversized SPS")
    bits = Bits(rbsp(nal))
    profile, constraints = bits.read(8), bits.read(8)
    if profile not in (77, 100) or constraints & 3:
        raise ValueError("unsupported SPS profile/constraints")
    bits.read(8)  # level_idc, not used to infer a reorder bound
    identifier = bits.ue(31)
    if profile == 100:
        if bits.ue(3) != 1 or bits.ue(6) != 0 or bits.ue(6) != 0:
            raise ValueError("only 8-bit 4:2:0 supported")
        bits.read()  # transform bypass
        if bits.read():
            for index in range(8):
                if bits.read():
                    last = next_scale = 8
                    for _ in range(16 if index < 6 else 64):
                        if next_scale:
                            delta = bits.se()
                            if not -128 <= delta <= 127:
                                raise ValueError("invalid scaling delta")
                            next_scale = (last + delta) % 256
                        last = next_scale or last
    bits.ue(12)  # log2_max_frame_num_minus4
    poc = bits.ue(2)
    if poc == 0:
        bits.ue(12)
    elif poc != 2:
        raise ValueError("POC type 1 unsupported for repair")
    references = bits.ue(16)
    bits.read()  # gaps_in_frame_num_value_allowed_flag
    width, height = (bits.ue(511) + 1) * 16, (bits.ue(511) + 1) * 16
    if bits.read() != 1:
        raise ValueError("interlaced SPS unsupported for repair")
    bits.read()  # direct_8x8_inference_flag
    if bits.read():
        left, right, top, bottom = (bits.ue(4096) for _ in range(4))
        width -= 2 * (left + right)
        height -= 2 * (top + bottom)
        if width <= 0 or height <= 0:
            raise ValueError("invalid SPS cropping")
    reorder, buffering = vui(bits) if bits.read() else (None, None)
    if buffering is not None and buffering < references:
        raise ValueError("DPB smaller than reference bound")
    bits.finish()
    return SPS(identifier, profile, width, height, reorder)


class ReorderEvidence:
    def __init__(self, stream):
        if len(stream["extradata"]) > 32768:
            raise ValueError("configuration size limit")
        data = bytes.fromhex(stream["extradata"])
        units = annexb(data)
        if len(units) != 2:
            raise ValueError("exactly one SPS/PPS required")
        self.sps, self.pps = units
        sps = parse_sps(self.sps)
        if (stream["profile"] != sps.profile or type(stream.get("video_delay")) is not int or
                stream["video_delay"] != 0 or
                (stream["width"], stream["height"]) != (sps.width, sps.height) or sps.reorder != 0):
            raise ValueError("no consistent explicit zero-reorder evidence")
        if not 2 <= len(self.pps) <= 4096 or self.pps[0] & 31 != 8 or not self.pps[0] & 0x60:
            raise ValueError("invalid PPS")
        bits = Bits(rbsp(self.pps))
        self.pps_id = bits.ue(255)
        if bits.ue(31) != sps.identifier:
            raise ValueError("PPS references unknown SPS")

    def check_packet(self, data):
        picture = False
        for nal in annexb(data):
            kind = nal[0] & 31
            if kind in (7, 8):
                if nal != (self.sps if kind == 7 else self.pps):
                    raise ValueError("parameter sets changed within clock generation")
            elif kind in (1, 5):
                # Only the bounded slice-header prefix is needed, not slice data.
                bits = Bits(rbsp(nal, limit=32))
                bits.ue()
                slice_type = bits.ue(9) % 5
                if (slice_type not in (0, 2) or (kind == 5 and slice_type != 2) or
                        bits.ue(255) != self.pps_id):
                    raise ValueError("slice contradicts no-reordering configuration")
                picture = True
            elif kind not in (6, 9, 12):
                raise ValueError("unsupported NAL type for repair")
        if not picture:
            raise ValueError("expected complete picture access unit")


def evidence_for(stream):
    try:
        return ReorderEvidence(stream)
    except (ValueError, KeyError, TypeError):
        return None
