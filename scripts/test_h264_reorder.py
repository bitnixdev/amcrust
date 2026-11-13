"""Synthetic syntax tests plus clock-policy wiring; real x264 media is in the Nix probe."""
import unittest

from h264_reorder import Bits, ReorderEvidence, annexb, evidence_for, parse_sps, rbsp
from rtsp_clock import ClockNormalizer
from test_packet_probe import hello, stream
import test_rtsp_clock as clock_tests
from test_rtsp_clock import EPOCH, feed, pkt, sr


class Writer:
    def __init__(self):
        self.bits = ""

    def put(self, value, count=1):
        self.bits += format(value, f"0{count}b")

    def ue(self, value):
        word = bin(value + 1)[2:]
        self.bits += "0" * (len(word) - 1) + word

    def nal(self, header):
        bits = self.bits + "1"
        bits += "0" * (-len(bits) % 8)
        raw = bytes(int(bits[i:i+8], 2) for i in range(0, len(bits), 8))
        result, zeros = bytearray([header]), 0
        for byte in raw:
            if zeros == 2 and byte <= 3:
                result.append(3)
                zeros = 0
            result.append(byte)
            zeros = zeros + 1 if byte == 0 else 0
        return bytes(result)


def sps(profile=77, reorder=0, restriction=True, has_vui=True, extended=False,
        scaling=False, frame_only=True, poc=2, identifier=0):
    w = Writer()
    w.put(profile, 8)
    w.put(0, 8)
    w.put(12, 8)
    w.ue(identifier)
    if profile == 100:
        w.ue(1)
        w.ue(0)
        w.ue(0)
        w.put(0)
        w.put(int(scaling))
        if scaling:
            for i in range(8):
                w.put(1)
                for _ in range(16 if i < 6 else 64):
                    w.ue(0)  # signed delta=0
    w.ue(0)
    w.ue(poc)
    if poc == 0:
        w.ue(0)
    w.ue(1)
    w.put(0)
    w.ue(19)
    w.ue(11)
    w.put(int(frame_only))
    w.put(1)
    w.put(1)
    for value in (0, 0, 0, 6):
        w.ue(value)
    w.put(int(has_vui))
    if has_vui:
        w.put(int(extended))
        if extended:
            w.put(255, 8)
            w.put(1, 16)
            w.put(1, 16)
        w.put(int(extended))
        if extended:
            w.put(0)
        w.put(int(extended))
        if extended:
            w.put(10, 4)
            w.put(1)
            w.put(0x010101, 24)
        w.put(int(extended))
        if extended:
            w.ue(0)
            w.ue(0)
        w.put(1)
        w.put(1, 32)
        w.put(30, 32)
        w.put(1)
        for _ in range(2):
            w.put(int(extended))
            if extended:
                w.ue(0)
                w.put(0, 8)
                w.ue(0)
                w.ue(0)
                w.put(0)
                w.put(0, 20)
        if extended:
            w.put(0)
        w.put(0)
        w.put(int(restriction))
        if restriction:
            w.put(1)
            for value in (0, 0, 8, 8, reorder, max(1, reorder)):
                w.ue(value)
    return w.nal(0x67)


def pps(identifier=0, sps_id=0):
    w = Writer()
    w.ue(identifier)
    w.ue(sps_id)
    return w.nal(0x68)


def picture(slice_type=2, pps_id=0):
    w = Writer()
    w.ue(0)
    w.ue(slice_type)
    w.ue(pps_id)
    return w.nal(0x65 if slice_type == 2 else 0x41)


def wire(*units):
    return b"".join(b"\0\0\0\1" + nal for nal in units)


def descriptor(profile=77, **syntax):
    return {**stream(), "profile": profile, "width": 320, "height": 180, "video_delay": 0,
            "extradata": wire(sps(profile=profile, **syntax), pps()).hex()}


class SyntaxTests(unittest.TestCase):
    def test_explicit_reorder_bounds_and_optional_vui_paths(self):
        for profile in (77, 100):
            for poc in (0, 2):
                for extended in (False, True):
                    nal = sps(profile=profile, poc=poc, extended=extended, scaling=(profile == 100))
                    parsed = parse_sps(nal)
                    self.assertEqual((parsed.profile, parsed.width, parsed.height, parsed.reorder),
                                     (profile, 320, 180, 0))
        self.assertEqual(parse_sps(sps(reorder=2)).reorder, 2)
        self.assertIsNone(parse_sps(sps(restriction=False)).reorder)
        self.assertIsNone(parse_sps(sps(has_vui=False)).reorder)

    def test_truncation_and_excess_golomb_never_authorize_repair(self):
        nal = sps(extended=True)
        for length in range(len(nal)):
            with self.assertRaises(ValueError):
                parse_sps(nal[:length])
        with self.assertRaises(ValueError):
            Bits(bytes(8)).ue()
        for bad in (nal + b"\x80", nal + b"\0", sps(frame_only=False), sps(poc=1)):
            with self.assertRaises(ValueError):
                parse_sps(bad)

    def test_emulation_prevention_and_annexb_bounds(self):
        self.assertEqual(rbsp(b"\x67\0\0\3\1"), b"\0\0\1")
        for nal in (b"\x67\0\0\3", b"\x67\0\0\3\4", b"\x67\0\0\2", b"\xe7\x80"):
            with self.assertRaises(ValueError):
                rbsp(nal)
        for data in (b"", b"\x01\x64\0\x1f", wire(b""), wire(*([b"\x09\x10"] * 257))):
            with self.assertRaises(ValueError):
                annexb(data)

    def test_absent_conflicting_or_unsupported_configuration_grants_no_repair(self):
        for item in (descriptor(reorder=2), descriptor(restriction=False), descriptor(has_vui=False),
                     {**descriptor(), "video_delay": 1}, {**descriptor(), "video_delay": False},
                     {**descriptor(), "extradata": "00" * 16385}, {**descriptor(), "width": 640},
                     {**descriptor(), "profile": 100}, {**descriptor(), "extradata": "0164001f"},
                     {**descriptor(), "extradata": wire(sps(), pps(sps_id=1)).hex()},
                     {**descriptor(), "extradata": wire(sps(), sps(identifier=1), pps()).hex()}):
            self.assertIsNone(evidence_for(item))

    def test_parameter_set_and_slice_identity_are_checked(self):
        evidence = ReorderEvidence(descriptor())
        evidence.check_packet(wire(evidence.sps, evidence.pps, picture()))
        for data in (wire(sps(reorder=2), picture()), wire(pps(identifier=1), picture()),
                     wire(picture(slice_type=1)), wire(picture(pps_id=1)), wire(b"\x74\x80"),
                     wire(b"\x06\x80")):
            with self.assertRaises(ValueError):
                evidence.check_packet(data)


class PolicyTests(unittest.TestCase):
    assert_closed = clock_tests.ClockTests.assert_closed

    def clock(self, config):
        clock = ClockNormalizer(1, now=0)
        feed(clock, {**hello(), "streams": 2, "start_time_realtime": EPOCH})
        feed(clock, config)
        feed(clock, {**stream(), "index": 1, "media": "audio", "codec": "aac", "time_base": [1, 48000]})
        return clock

    def test_main_and_high_repair_uses_actual_configuration(self):
        for profile in (77, 100):
            clock = self.clock(descriptor(profile))
            feed(clock, {**pkt(0, pts=None, dts=None, report=sr()), "data": wire(picture()).hex()})
            feed(clock, {**pkt(1, pts=6000, dts=None), "data": wire(picture(0)).hex()})
            result = feed(clock, pkt(2, track=1, report=sr(1)))
            self.assertEqual([(p["pts"], p["dts"]) for p in result if p["type"] == "packet"],
                             [(0, 0), (6000, 6000), (0, 0)])
            self.assertEqual((clock.repaired_pts, clock.repaired_dts), (1, 2))
            self.assertTrue(clock.ready)

    def test_missing_evidence_or_contradictory_slice_closes_generation(self):
        for config, payload in ((descriptor(reorder=2), picture()),
                                (descriptor(restriction=False), picture()),
                                (descriptor(), picture(slice_type=1))):
            clock = self.clock(config)
            with self.assertRaises(ValueError):
                feed(clock, {**pkt(0, pts=None, dts=None, report=sr()), "data": wire(payload).hex()})
            self.assert_closed(clock)

    def test_known_composition_offset_is_rejected_not_overwritten(self):
        clock = self.clock(descriptor())
        known = {**pkt(0, pts=6000, dts=0, report=sr()), "data": wire(picture()).hex()}
        feed(clock, known)
        with self.assertRaisesRegex(ValueError, "composition offset"):
            feed(clock, pkt(1, track=1, report=sr(1)))
        self.assertEqual((known["pts"], known["dts"]), (6000, 0))
        self.assert_closed(clock)

    def test_in_session_parameter_change_invalidates_admitted_evidence(self):
        clock = self.clock(descriptor())
        feed(clock, {**pkt(0, report=sr()), "data": wire(picture()).hex()})
        feed(clock, pkt(1, track=1, report=sr(1)))
        self.assertTrue(clock.ready)
        with self.assertRaisesRegex(ValueError, "parameter sets changed"):
            feed(clock, {**pkt(2, pts=6000, dts=6000), "data": wire(sps(reorder=2), picture(0)).hex()})
        self.assert_closed(clock)


if __name__ == "__main__":
    unittest.main()
