"""Experimental, fail-closed clock admission for early-SR H264/AAC sources.

Libavformat owns RTP/RTCP mapping. We never zero tracks independently. Only the
initial missing timestamps of proven non-reordering H264 profiles are repaired.
This excludes first SRs arriving after media and missing reordered DTS.
"""
from fractions import Fraction
import math
import time

from packet_ingest_probe import Protocol, integer

NTP_UNIX_OFFSET = 2208988800


class ClockNormalizer:
    def __init__(self, generation, *, now=None, max_bytes=2 * 1024 * 1024,
                 max_frames=64, max_media=Fraction(1, 2), timeout=4,
                 report_timeout=Fraction(2)):
        try:
            self.report_timeout = Fraction(report_timeout)
        except (TypeError, ValueError, OverflowError, ZeroDivisionError) as error:
            raise ValueError("invalid report timeout") from error
        if isinstance(report_timeout, bool) or not 0 < self.report_timeout <= 30:
            raise ValueError("invalid report timeout")
        self.protocol = Protocol(generation)
        self.now = time.monotonic() if now is None else now
        if not math.isfinite(self.now):
            raise ValueError("invalid monotonic time")
        self.deadline = self.now + timeout
        self.horizon = None
        self.max_bytes, self.max_frames, self.max_media = max_bytes, max_frames, max_media
        self.pending, self.bytes = [], 0
        self.state = {}
        self.ready = self.failed = False
        self.repaired_pts = self.repaired_dts = 0

    def _abort(self):
        self.failed, self.ready = True, False
        self.pending.clear()
        self.state.clear()
        self.protocol.streams.clear()
        self.bytes = 0
        self.horizon = None

    @property
    def next_deadline(self):
        """Caller must arm an idle timer; feed() alone cannot observe silence."""
        deadlines = [s["report_deadline"] for s in self.state.values() if s["report"] is not None]
        if not self.ready:
            deadlines.append(self.deadline)
        return min(deadlines)

    def expire(self, now=None):
        now = time.monotonic() if now is None else now
        if not math.isfinite(now) or now < self.now:
            self._abort()
            raise ValueError("invalid/regressing monotonic time")
        self.now = now
        if self.failed or (not self.ready and now >= self.deadline):
            self._abort()
            raise ValueError("clock admission closed/timed out")
        if any(s["report"] is not None and now >= s["report_deadline"] for s in self.state.values()):
            self._abort()
            raise ValueError("sender report freshness expired (wall clock)")

    def feed(self, raw, *, now=None):
        try:
            self.expire(now)
            return self._feed(raw)
        except (ValueError, KeyError, TypeError) as error:
            self._abort()
            raise ValueError(f"clock admission failed: {error}") from error

    def _feed(self, raw):
        if not self.ready and (self.bytes + len(raw) > self.max_bytes or len(self.pending) >= self.max_frames):
            raise ValueError("startup buffer limit")
        frame = self.protocol.accept(raw)
        kind = frame["type"]
        if kind == "hello":
            self.epoch = integer(frame.get("start_time_realtime"), 0)
        elif kind == "stream":
            self.state[frame["index"]] = dict(report=None, missing=[], pts_seen=False,
                dts_seen=False, missing_dts=0, media=Fraction(0), last=None)
        elif kind == "packet":
            self._packet(frame)
        elif kind == "end" and not self.ready:
            raise ValueError("source ended before clock admission")
        if self.ready:
            self._validate(frame)
            return [frame]
        self.pending.append(frame)
        self.bytes += len(raw)
        if (len(self.state) == self.protocol.hello["streams"] and
                all(s["report"] is not None and s["pts_seen"] and not s["missing"] for s in self.state.values())):
            for item in self.pending:
                self._validate(item)
            released, self.pending = self.pending, []
            self.bytes = 0
            self.ready = True
            return released
        return []

    def _report(self, report, stream, state):
        ssrc = integer(report["ssrc"], 0, (1 << 32) - 1)
        ntp = integer(report["ntp"], 0, (1 << 64) - 1)
        rtp = integer(report["rtp"], 0, (1 << 32) - 1)
        previous = state["report"]
        if previous is None:
            unix_us = ntp * 1000000 // (1 << 32) - NTP_UNIX_OFFSET * 1000000
            if not 0 <= Fraction(unix_us - self.epoch, 1000000) <= self.max_media:
                raise ValueError("first sender report lies outside the admitted epoch window")
        else:
            delta_ntp = Fraction(ntp - previous["ntp"], 1 << 32)
            delta_rtp = (rtp - previous["rtp"] + (1 << 31)) % (1 << 32) - (1 << 31)
            tick = Fraction(*stream["time_base"])
            if (ssrc != previous["ssrc"] or not 0 < delta_ntp <= 30 or
                    abs(delta_rtp * tick - delta_ntp) > tick):
                raise ValueError("sender clock/SSRC discontinuity")
            if delta_ntp >= self.report_timeout:
                raise ValueError("sender report freshness expired (report interval)")
        state["report"] = {"ssrc": ssrc, "ntp": ntp, "rtp": rtp}
        state["report_deadline"] = self.now + float(self.report_timeout)
        state["report_media_time"] = (Fraction(ntp, 1 << 32) - NTP_UNIX_OFFSET -
                                      Fraction(self.epoch, 1000000))

    def _packet(self, packet):
        stream = self.protocol.streams[packet["stream"]]
        state = self.state[packet["stream"]]
        duration = integer(packet["duration"], 1)
        if not self.ready:
            state["media"] += duration * Fraction(*stream["time_base"])
            if state["media"] > self.max_media:
                raise ValueError("startup media-duration limit")
        if state["report"] is None and "rtcp_sr" not in packet:
            raise ValueError("first packet lacks sender-clock evidence")
        if "rtcp_sr" in packet:
            self._report(packet["rtcp_sr"], stream, state)
        baseline = (stream["codec"] == "h264" and stream["profile"] in (66, 578)
                    and stream.get("video_delay") == 0)
        if packet["pts"] is None or packet["dts"] is None:
            if not baseline:
                raise ValueError("missing timestamp on unsupported/reordered track")
        if packet["dts"] is None:
            state["missing_dts"] += 1
            if state["dts_seen"] or state["missing_dts"] > 8:
                raise ValueError("DTS repair limited to bounded startup prefix")
            packet["dts"] = packet["pts"]
            self.repaired_dts += 1
        else:
            state["dts_seen"] = True
        if packet["pts"] is None:
            if state["pts_seen"] or len(state["missing"]) >= 8:
                raise ValueError("PTS repair limited to bounded startup prefix")
            state["missing"].append(packet)
        else:
            cursor = packet["pts"]
            for previous in reversed(state["missing"]):
                cursor -= previous["duration"]
                integer(cursor, -(1 << 63) + 1)
                if previous["dts"] not in (None, cursor):
                    raise ValueError("inferred PTS contradicts known DTS")
                previous["pts"] = cursor
                if previous["dts"] is None:
                    previous["dts"] = cursor
                self.repaired_pts += 1
            state["missing"].clear()
            state["pts_seen"] = True

    def _validate(self, packet):
        if packet["type"] != "packet":
            return
        stream = self.protocol.streams[packet["stream"]]
        state = self.state[packet["stream"]]
        pts, dts = packet["pts"], packet["dts"]
        integer(pts, -(1 << 63) + 1)
        integer(dts, -(1 << 63) + 1)
        baseline = stream["codec"] == "h264" and stream["profile"] in (66, 578)
        if baseline and pts != dts:
            raise ValueError("baseline video has composition reordering")
        last = state["last"]
        if last is not None:
            if dts <= last[1]:
                raise ValueError("nonmonotonic decode clock")
            if (baseline or stream["codec"] == "aac") and pts != last[0] + last[2]:
                raise ValueError("non-reordering track has a timestamp gap/rollback")
        end = (pts + packet["duration"]) * Fraction(*stream["time_base"])
        horizon = end if self.horizon is None else max(self.horizon, end)
        # Shared presentation progress ages every track's clock evidence, including
        # a track that has stopped delivering packets. Receipt bursts cannot refresh
        # an old source clock simply by arriving quickly on the IPC pipe.
        for other in self.state.values():
            if horizon - other["report_media_time"] >= self.report_timeout:
                raise ValueError("sender report freshness expired (media clock)")
        self.horizon = horizon
        state["last"] = (pts, dts, packet["duration"])
