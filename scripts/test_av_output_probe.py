"""Fragment ownership, timing, and AAC-tail policy tests; no native tools needed."""
import struct
import unittest

from av_output_probe import (Fragment, InitSegment, assemble, boxes, compare_audio,
                             compare_timing, split_recording)
from packet_ingest_probe import MAX_FRAME


def box(kind, payload=b""):
    return struct.pack("!I4s", len(payload) + 8, kind) + payload


def movie():
    return (box(b"ftyp") + box(b"moov") + box(b"moof") + box(b"mdat", b"one")
            + box(b"moof") + box(b"mdat", b"two") + box(b"mfra"))


class FragmentTests(unittest.TestCase):
    def test_contiguous_fragments_and_late_clip(self):
        init, fragments = split_recording(1, movie())
        self.assertEqual(len(fragments), 2)
        self.assertEqual(assemble(init, fragments), movie()[:-8])
        self.assertEqual(assemble(init, fragments[1:]), init.data + fragments[1].data)

    def test_reject_other_epoch_or_initialization(self):
        init, fragments = split_recording(1, movie())
        for other in (InitSegment(2, init.data), InitSegment(1, init.data + box(b"free"))):
            with self.assertRaises(ValueError):
                assemble(other, fragments)

    def test_reject_gap_duplicate_reorder_and_invalid_sequence(self):
        init, fragments = split_recording(1, movie())
        for bad in ([], fragments[::-1], fragments[:1] * 2,
                    [fragments[0], Fragment(init.identity, 3, fragments[1].data)],
                    [Fragment(init.identity, 0, fragments[0].data)]):
            with self.assertRaises(ValueError):
                assemble(init, bad)

    def test_invalid_box_limits_and_truncation(self):
        for data in (b"x", struct.pack("!I4s", 0, b"mdat"),
                     struct.pack("!I4s", MAX_FRAME + 1, b"mdat"),
                     struct.pack("!I4s", 1, b"mdat"),
                     struct.pack("!I4sQ", 1, b"mdat", 15), box(b"mdat", b"x")[:-1]):
            with self.assertRaises(ValueError):
                list(boxes(data))
        self.assertEqual(list(boxes(struct.pack("!I4sQ", 1, b"free", 16)))[0][0], b"free")

    def test_incomplete_or_mixed_fragment_layout(self):
        for data in (box(b"mdat"), box(b"moov") + box(b"moof") + box(b"mdat"),
                     box(b"ftyp") + box(b"moov") + box(b"moof"),
                     movie() + box(b"moof") + box(b"mdat"),
                     box(b"ftyp") * 2 + movie()[8:],
                     box(b"ftyp") + box(b"moov") + box(b"moof") * 2 + box(b"mdat")):
            with self.assertRaises(ValueError):
                split_recording(1, data)


class TimingTests(unittest.TestCase):
    def reference(self, time_base="1/48000", pts=0, duration=1024):
        return {"streams": [{"index": 0, "codec_type": "audio", "codec_name": "aac",
                             "time_base": time_base}],
                "packets": [{"stream_index": 0, "pts": pts, "dts": pts, "duration": duration,
                             "data_hash": "SHA256:unchanged"}]}

    def test_exact_rational_rescaling(self):
        self.assertEqual(compare_timing(self.reference(pts=48000),
                                        self.reference("1/96000", pts=96000, duration=2048)), 0)

    def test_detect_timeline_shift_payload_loss_and_duration_change(self):
        original = self.reference()
        changed = self.reference()
        changed["packets"][0]["data_hash"] = "SHA256:changed"
        for output in (self.reference(pts=1024), self.reference(duration=2048), changed,
                       {"streams": [], "packets": []}):
            with self.assertRaises(ValueError):
                compare_timing(original, output)

    def test_nonterminal_padding_is_not_treated_as_tail_padding(self):
        reference = self.reference()
        reference["packets"][0]["side_data_list"] = [{"discard_padding": 1}]
        reference["packets"].append({"stream_index": 0})
        with self.assertRaises(ValueError):
            compare_audio(b"abcd", b"abcdxx", reference, 1)

    def test_only_declared_terminal_padding_is_tolerated(self):
        reference = self.reference()
        reference["packets"][0]["side_data_list"] = [{"discard_padding": 1}]
        self.assertEqual(compare_audio(b"abcd", b"abcd", reference, 1), 0)
        self.assertEqual(compare_audio(b"abcd", b"abcdxx", reference, 1), 1)
        for output in (b"abcexx", b"xxabcd", b"abcdxxxx", b"ab"):
            with self.assertRaises(ValueError):
                compare_audio(b"abcd", output, reference, 1)


if __name__ == "__main__":
    unittest.main()
