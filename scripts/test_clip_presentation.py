from fractions import Fraction
import struct
import unittest

from clip_presentation import translate
from packet_wire import encode_frame
from presentation_probe import edit_entries, field
from recording_cut import RecordingCutInput
from test_audio_history import setup


def elst(version=0, duration=0, media_time=0, rate=1, count=1):
    body = bytes([version, 0, 0, 0]) + struct.pack("!I", count)
    body += struct.pack("!Iihh" if version == 0 else "!Qqhh", duration, media_time, rate, 0)
    return struct.pack("!I4s", len(body) + 8, b"elst") + body


class PresentationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.source = await setup()
        handoff = self.source.open(preceding=1)
        recorder = RecordingCutInput(handoff)
        with await handoff.receive() as delivery:
            recorder.feed(delivery)
        recorder.prepare_cut()
        self.frames = recorder.commit_cut()

    async def asyncTearDown(self):
        self.source.close()

    def test_one_exact_origin_preserves_offsets_payloads_and_counters(self):
        before = tuple(encode_frame(f) for f in self.frames)
        for policy, origin in (("earliest", Fraction(0)), ("video", Fraction(8, 375))):
            shifted = translate(self.frames, policy)
            self.assertEqual(shifted.origin, origin)
            for original, current in zip(self.frames, shifted.frames):
                if original["type"] != "packet":
                    self.assertEqual(original, current)
                    continue
                clock = Fraction(1, 90000 if original["stream"] == 0 else 48000)
                for key in original:
                    if key in ("pts", "dts"):
                        self.assertEqual((original[key] - current[key]) * clock, origin)
                    else:
                        self.assertEqual(original[key], current[key])
            shifted.frames[0]["generation"] = 99  # copied diagnostics are not immutable grants
        self.assertEqual(before, tuple(encode_frame(f) for f in self.frames))
        self.assertTrue(self.source._gate._epoch.active)

    def test_nonrepresentable_origin_refuses_rounding(self):
        for obj in self.frames:
            if obj["type"] == "packet" and obj["stream"] == 1:
                obj["pts"] += 1
                obj["dts"] += 1
        with self.assertRaisesRegex(ValueError, "exactly representable"):
            translate(self.frames, "earliest")

    def test_shift_overflow_is_refused(self):
        for obj in self.frames:
            if obj["type"] == "packet":
                delta = -(2**62) if obj["stream"] == 1 else 2**62
                obj["pts"] += delta
                obj["dts"] += delta
        with self.assertRaisesRegex(ValueError, "native range"):
            translate(self.frames, "earliest")

    def test_requires_finite_known_clocks_and_explicit_policy(self):
        with self.assertRaisesRegex(ValueError, "unknown"):
            translate(self.frames, "independent-zero")
        with self.assertRaisesRegex(ValueError, "finite end"):
            translate(self.frames[:-1], "video")
        packet = next(f for f in self.frames if f["type"] == "packet")
        packet["dts"] = None
        with self.assertRaisesRegex(ValueError, "known clocks"):
            translate(self.frames, "video")

    def test_no_implicit_unbounded_recording(self):
        with self.assertRaisesRegex(ValueError, "bounded finite"):
            translate(self.frames * 100, "video")
        with self.assertRaises(ValueError):
            translate((), "video")

    def test_wire_budget_is_enforced(self):
        for packet in self.frames:
            if packet["type"] == "packet":
                packet["data"] = "ab" * (1024 * 1024)
        with self.assertRaisesRegex(ValueError, "input budget"):
            translate(self.frames, "video")

    def test_nonkey_video_start_is_refused(self):
        packet = next(f for f in self.frames if f["type"] == "packet" and f["stream"] == 0)
        packet["flags"] = 0
        with self.assertRaisesRegex(ValueError, "keyframe"):
            translate(self.frames, "video")

    def test_edit_versions_and_signed_media_time(self):
        self.assertEqual(edit_entries(elst(media_time=-1, duration=123)),
                         [{"duration": 123, "media_time": -1}])
        self.assertEqual(edit_entries(elst(1, 2**33, 1184)),
                         [{"duration": 2**33, "media_time": 1184}])
        for malformed in (elst(count=9), elst(count=0), elst(rate=0), elst(media_time=-2), elst()[:-1]):
            with self.assertRaises(ValueError):
                edit_entries(malformed)
        bad = bytearray(elst())
        bad[8] = 2
        with self.assertRaises(ValueError):
            edit_entries(bytes(bad))

    def test_field_rejects_truncation_and_zero_scale(self):
        for box in (b"\0" * 8, b"\0" * 20, b"\0" * 32):
            with self.assertRaises(ValueError):
                field(box, 12, 20)
