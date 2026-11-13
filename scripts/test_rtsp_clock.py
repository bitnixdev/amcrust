"""Clock policy and compound RTCP fixture tests; no native libraries required."""
from packet_wire import encode_frame
import struct
import unittest

from rtsp_clock import ClockNormalizer, NTP_UNIX_OFFSET
from rtsp_fixture import sender_report
from test_packet_probe import hello, stream, packet

NTP = 3900000000 << 32
EPOCH = (3900000000 - NTP_UNIX_OFFSET) * 1000000


def sr(track=0, at=0, drift=0):
    rate = 90000 if track == 0 else 48000
    return {"ssrc": 10 + track, "ntp": NTP + int(at * (1 << 32)),
            "rtp": (0xFFFF0000 + int(at * rate) + drift) & 0xFFFFFFFF}


def feed(clock, frame):
    return clock.feed(encode_frame(frame), now=0)


def setup(profile=578, delay=0, **limits):
    clock = ClockNormalizer(1, now=0, **limits)
    feed(clock, {**hello(), "streams": 2, "start_time_realtime": EPOCH})
    feed(clock, {**stream(), "profile": profile, "video_delay": delay})
    feed(clock, {**stream(), "index": 1, "media": "audio", "codec": "aac", "profile": 1,
                 "sample_rate": 48000, "time_base": [1, 48000]})
    return clock


def pkt(seq, track=0, pts=0, dts=0, report=None):
    result = {**packet(), "sequence": seq, "stream": track, "pts": pts, "dts": dts,
              "duration": 6000 if track == 0 else 1024}
    if report is not None:
        result["rtcp_sr"] = report
    return result


def admitted():
    clock = setup()
    feed(clock, pkt(0, report=sr()))
    feed(clock, pkt(1, track=1, report=sr(1)))
    return clock


class ClockTests(unittest.TestCase):
    def assert_closed(self, clock):
        self.assertTrue(clock.failed)
        self.assertEqual(clock.pending, [])
        self.assertEqual(clock.state, {})
        self.assertEqual(clock.protocol.streams, {})
        self.assertEqual(clock.bytes, 0)
        with self.assertRaises(ValueError):
            feed(clock, pkt(clock.protocol.sequence))

    def test_bounded_prefix_repair_retains_shared_epoch_and_offset(self):
        clock = setup()
        first = pkt(0, pts=None, dts=None, report=sr())
        self.assertEqual(feed(clock, first), [])
        self.assertEqual(feed(clock, pkt(1, pts=6000, dts=None)), [])
        output = feed(clock, pkt(2, track=1, pts=12000, dts=12000, report=sr(1)))
        packets = [f for f in output if f["type"] == "packet"]
        self.assertEqual([(p["pts"], p["dts"]) for p in packets], [(0, 0), (6000, 6000), (12000, 12000)])
        self.assertIsNone(first["pts"])  # caller's raw frame was not mutated
        self.assertEqual((clock.repaired_pts, clock.repaired_dts), (1, 2))
        self.assertTrue(clock.ready)
        self.assertEqual(clock.bytes, 0)

    def test_complete_b_frame_timestamps_pass_unchanged(self):
        clock = setup(profile=77, delay=2)
        first = pkt(0, pts=12000, dts=0, report=sr())
        feed(clock, first)
        output = feed(clock, pkt(1, track=1, report=sr(1)))
        self.assertEqual(output[-2], first)
        for seq, pts, dts in ((2, 30000, 6000), (3, 18000, 12000), (4, 24000, 18000)):
            obj = pkt(seq, pts=pts, dts=dts)
            self.assertEqual(feed(clock, obj), [obj])

    def test_missing_reordered_unknown_or_delayed_video_is_rejected(self):
        for profile, delay in ((77, 2), (100, 0), (-99, 0), (578, 1)):
            clock = setup(profile=profile, delay=delay)
            with self.assertRaisesRegex(ValueError, "unsupported/reordered"):
                feed(clock, pkt(0, pts=12000, dts=None, report=sr()))
            self.assert_closed(clock)

    def test_no_epoch_or_out_of_window_first_report_is_rejected(self):
        for epoch in (None, True, -1):
            clock = ClockNormalizer(1, now=0)
            with self.assertRaises(ValueError):
                feed(clock, {**hello(), "start_time_realtime": epoch})
            self.assert_closed(clock)
        clock = setup()
        with self.assertRaisesRegex(ValueError, "epoch window"):
            feed(clock, pkt(0, report=sr(at=0.75)))
        self.assert_closed(clock)

    def test_staggered_report_before_first_media_is_safe_but_late_evidence_is_not(self):
        clock = setup()
        feed(clock, pkt(0, report=sr()))
        feed(clock, pkt(1, track=1, pts=12000, dts=12000, report=sr(1, at=0.25)))
        self.assertTrue(clock.ready)
        clock = setup()
        with self.assertRaisesRegex(ValueError, "lacks sender-clock evidence"):
            feed(clock, pkt(0))
        self.assert_closed(clock)

    def test_missing_dts_prefix_has_a_hard_limit_after_admission(self):
        clock = setup()
        feed(clock, pkt(0, dts=None, report=sr()))
        feed(clock, pkt(1, track=1, report=sr(1)))
        self.assertTrue(clock.ready)
        for i in range(1, 8):
            feed(clock, pkt(i + 1, pts=i * 6000, dts=None))
        with self.assertRaisesRegex(ValueError, "bounded startup prefix"):
            feed(clock, pkt(9, pts=48000, dts=None))
        self.assert_closed(clock)

    def test_fresh_generation_rejects_old_descriptors(self):
        fresh = ClockNormalizer(2, now=0)
        feed(fresh, {**hello(), "generation": 2, "start_time_realtime": EPOCH})
        with self.assertRaises(ValueError):
            feed(fresh, stream())
        self.assert_closed(fresh)

    def test_sender_report_wrap_and_clock_faults(self):
        clock = admitted()
        # One second of RTP ticks wraps the fixture's origin, not its timeline.
        clock._report(sr(at=1), clock.protocol.streams[0], clock.state[0])
        self.assertEqual(clock.state[0]["report"]["rtp"], (0xFFFF0000 + 90000) & 0xFFFFFFFF)
        for report in (sr(at=1, drift=9000), {**sr(at=1), "ssrc": 99}, sr(at=0)):
            clock = admitted()
            with self.assertRaisesRegex(ValueError, "discontinuity"):
                feed(clock, pkt(2, pts=6000, dts=6000, report=report))
            self.assert_closed(clock)

    def test_midstream_missing_gap_and_dts_rollback_fail_closed(self):
        for pts, dts in ((None, None), (6000, None), (12000, 12000), (6000, 0)):
            clock = admitted()
            with self.assertRaises(ValueError):
                feed(clock, pkt(2, pts=pts, dts=dts))
            self.assert_closed(clock)

    def test_media_count_byte_and_deadline_limits(self):
        clock = setup()
        # Missing second-track report cannot retain media indefinitely.
        with self.assertRaisesRegex(ValueError, "media-duration"):
            for i in range(8):
                feed(clock, pkt(i, pts=i * 6000, dts=i * 6000, report=sr() if i == 0 else None))
        self.assert_closed(clock)
        for limits, message in (({"max_frames": 3}, "buffer"), ({"max_bytes": 1500}, "buffer")):
            clock = setup(**limits)
            with self.assertRaisesRegex(ValueError, message):
                feed(clock, {**pkt(0, report=sr()), "data": "aa" * 800})
            self.assert_closed(clock)
        clock = setup()
        feed(clock, pkt(0, pts=None, dts=None, report=sr()))
        with self.assertRaisesRegex(ValueError, "timed out"):
            clock.expire(now=5)
        self.assert_closed(clock)

    def test_startup_budget_counts_expanded_proof_objects(self):
        clock = setup()
        raw = encode_frame({**pkt(0, report=sr()), "data": "aa" * 1000})
        clock.max_bytes = clock.bytes + len(raw) + 1
        with self.assertRaisesRegex(ValueError, "startup buffer limit"):
            clock.feed(raw, now=0)
        self.assert_closed(clock)

    def test_generation_end_and_ambiguous_prefix(self):
        for frame in ({**pkt(0), "generation": 2}, {"type": "end", "generation": 1, "packets": 0},
                      pkt(0, pts=None, dts=1, report=sr())):
            clock = setup()
            with self.assertRaises(ValueError):
                feed(clock, frame)
                feed(clock, pkt(1, pts=6000, dts=6000))
            self.assert_closed(clock)
        clock = admitted()
        end = {"type": "end", "generation": 1, "packets": 2}
        self.assertEqual(feed(clock, end), [end])
        with self.assertRaises(ValueError):
            feed(clock, pkt(2))
        self.assert_closed(clock)
        self.assertFalse(setup().ready)  # fresh generation has no inherited clock admission


class FreshnessTests(unittest.TestCase):
    assert_closed = ClockTests.assert_closed

    def test_idle_expiry_after_admission_clears_state(self):
        clock = admitted()
        self.assertEqual(clock.next_deadline, 2)
        clock.expire(now=1.99)
        with self.assertRaisesRegex(ValueError, "freshness expired \\(wall clock\\)"):
            clock.expire(now=2)
        self.assert_closed(clock)

    def test_first_track_can_expire_while_waiting_for_other_track(self):
        clock = setup()
        feed(clock, pkt(0, pts=None, dts=None, report=sr()))
        self.assertEqual(clock.next_deadline, 2)  # earlier than startup's 4s deadline
        with self.assertRaisesRegex(ValueError, "freshness expired"):
            clock.expire(now=2)
        self.assert_closed(clock)

    def test_reports_renew_only_their_own_track(self):
        clock = admitted()
        clock.feed(encode_frame(pkt(2, pts=6000, dts=6000, report=sr(at=0.5))), now=0.5)
        self.assertEqual(clock.next_deadline, 2)  # audio did not renew
        with self.assertRaisesRegex(ValueError, "freshness expired"):
            clock.expire(now=2)
        self.assert_closed(clock)
        clock = admitted()
        clock.feed(encode_frame(pkt(2, pts=6000, dts=6000, report=sr(at=0.5))), now=0.5)
        clock.feed(encode_frame(pkt(3, track=1, pts=1024, dts=1024, report=sr(1, at=0.5))), now=0.5)
        self.assertEqual(clock.next_deadline, 2.5)
        clock.expire(now=2.49)
        self.assertTrue(clock.ready)

    def test_media_burst_cannot_keep_another_tracks_old_mapping_alive(self):
        clock = admitted()
        with self.assertRaisesRegex(ValueError, "freshness expired \\(media clock\\)"):
            for i in range(1, 31):
                at = {8: 0.5, 15: 1, 23: 1.5, 30: 2}.get(i)
                # Video SRs stay current; the silent audio track does not.
                feed(clock, pkt(i + 1, pts=i * 6000, dts=i * 6000,
                                report=sr(at=at) if at is not None else None))
        self.assertEqual(clock.now, 0)  # no receiver wall time elapsed
        self.assert_closed(clock)

    def test_late_report_does_not_revive_expired_mapping(self):
        clock = admitted()
        with self.assertRaisesRegex(ValueError, "freshness expired \\(wall clock\\)"):
            clock.feed(encode_frame(pkt(2, pts=6000, dts=6000, report=sr(at=2))), now=2)
        self.assert_closed(clock)
        clock = admitted()
        with self.assertRaisesRegex(ValueError, "freshness expired \\(report interval\\)"):
            feed(clock, pkt(2, pts=6000, dts=6000, report=sr(at=2)))
        self.assert_closed(clock)

    def test_timeout_policy_and_monotonic_inputs_are_bounded(self):
        for value in (0, -1, True, 31, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                ClockNormalizer(1, report_timeout=value)
        for value in (-1, float("nan"), float("inf")):
            clock = admitted()
            with self.assertRaisesRegex(ValueError, "monotonic time"):
                clock.expire(now=value)
            self.assert_closed(clock)


class SenderReportTests(unittest.TestCase):
    def test_compound_sr_sdes_lengths_and_shared_epoch(self):
        for track, rate in ((0, 90000), (1, 48000)):
            wire = sender_report(track, 0xFFFF0000, 1, 4, 100)
            version, kind, length, ssrc, ntp, rtp, packets, octets = struct.unpack("!BBHIQIII", wire[:28])
            self.assertEqual((version, kind, length, ssrc), (0x80, 200, 6, 0x10203040 + track))
            self.assertEqual((ntp, rtp, packets, octets), (NTP + (1 << 32), (0xFFFF0000 + rate) & 0xFFFFFFFF, 4, 100))
            self.assertEqual(wire[28:30], bytes([0x81, 202]))
            self.assertEqual((struct.unpack("!H", wire[30:32])[0] + 1) * 4, len(wire) - 28)
            self.assertIn(b"shared-fixture-clock", wire[32:])


if __name__ == "__main__":
    unittest.main()
