"""Bounded metadata-only AAC window planner; not a media buffer or replay grant.

One packet is one AU under the synthetic fixture contract, not general AAC parsing.
Records contain scalars/hashes, never packet bodies or shared frame references.
Plans describe complete AUs covering a current admitted video IDR. Copied plans are
inert diagnostic evidence; source retirement cannot revoke copies and they must
never authorize subsequent output. The owning supervisor drives idle expire().
"""
from collections import deque
from dataclasses import dataclass
from fractions import Fraction
import hashlib

from packet_ingest_probe import byte_field, integer
from subscriber_budget import Delivery, residence_seconds
from shared_admission import require_admission


@dataclass(frozen=True)
class AudioRecord:
    __slots__ = ("sequence", "ordinal", "start", "end", "wire_bytes", "sha256", "deadline")
    sequence: int
    ordinal: int
    start: Fraction
    end: Fraction
    wire_bytes: int
    sha256: str
    deadline: float


class AudioWindowIndex:
    def __init__(self, admission, *, max_duration=Fraction(1, 4), max_age=0.5,
                 max_records=64, max_wire_bytes=2 * 1024 * 1024):
        self.queue = admission.queue
        self.snapshot = admission.snapshot
        self.records = deque()
        self.wire_bytes = self.peak_records = self.peak_wire_bytes = 0
        self.evicted = 0
        self.closed = False
        self._ordinal = self.snapshot.first_ordinal
        self._sequence = self.snapshot.first_sequence
        self._audio_end = self._watermark = self._candidate = None
        try:
            require_admission(admission)
            self.max_records = integer(max_records, 1, 256)
            self.max_wire_bytes = integer(max_wire_bytes, 1, 32 * 1024 * 1024)
            self.max_age = residence_seconds(max_age)
            if not isinstance(max_duration, Fraction) or not 0 < max_duration <= 1:
                raise ValueError("invalid audio window duration")
            self.max_duration = max_duration
            streams = self.snapshot.streams
            audio = [s for s in streams if s["media"] == "audio"]
            video = [s for s in streams if s["media"] == "video"]
            if len(audio) != 1 or len(video) != 1 or audio[0]["codec"] != "aac":
                raise ValueError("audio window requires one AAC and one video track")
            self._audio = audio[0]["index"]
            self._clocks = {s["index"]: Fraction(*s["time_base"]) for s in streams}
            self._video_clock = self._clocks[video[0]["index"]]
            self.expire()
        except ValueError:
            self.close()
            raise

    def close(self):
        self.closed = True
        self.records.clear()
        self.wire_bytes = 0
        self._candidate = None
        self.snapshot = None
        self.queue.detach("cancelled")

    def _pop(self):
        self.wire_bytes -= self.records.popleft().wire_bytes
        self.evicted += 1

    def expire(self):
        self.queue.expire()
        if self.closed or self.queue.detached or not self.queue._epoch.active:
            self.close()
            raise ValueError("retired audio window")
        now = self.queue._loop.time()
        while self.records and self.records[0].deadline <= now:
            self._pop()
        if self._candidate is not None and self._candidate[-1] <= now:
            self._candidate = None

    @property
    def next_deadline(self):
        deadlines = [self.records[0].deadline] if self.records else []
        if self._candidate is not None:
            deadlines.append(self._candidate[-1])
        return min(deadlines) if deadlines else None

    def observe(self, delivery):
        try:
            self.expire()
            if not isinstance(delivery, Delivery) or delivery._owner is not self.queue:
                raise ValueError("foreign/released window delivery")
            frame = delivery.frame
            obj = frame.metadata
            if (frame.origin is not self.snapshot.origin or frame.ordinal != self._ordinal or
                    obj["generation"] != self.snapshot.generation):
                raise ValueError("window source identity/ordinal/generation changed")
            self._candidate = None
            if obj["type"] == "end":
                if obj["packets"] != self._sequence:
                    raise ValueError("window end count changed")
                self.close()
                return
            if obj["type"] != "packet" or obj["sequence"] != self._sequence:
                raise ValueError("window source sequence changed")
            self._ordinal += 1
            self._sequence += 1
            clock = self._clocks[obj["stream"]]
            start, end = obj["pts"] * clock, (obj["pts"] + obj["duration"]) * clock
            if obj["pts"] != obj["dts"] or end <= start:
                raise ValueError("window requires known non-reordered clocks")
            deadline = min(delivery.deadline,
                           delivery.deadline - self.queue.max_residence + self.max_age)
            self._watermark = max(end, self._watermark) if self._watermark is not None else end
            if obj["stream"] == self._audio:
                if self._audio_end is not None and start != self._audio_end:
                    raise ValueError("AAC gap/overlap")
                self._audio_end = end
                size = len(frame.raw)
                if size > self.max_wire_bytes or end - start > self.max_duration:
                    raise ValueError("single AAC unit exceeds window budget")
                record = AudioRecord(obj["sequence"], frame.ordinal, start, end, size,
                                     hashlib.sha256(byte_field(obj["data"])).hexdigest(), deadline)
                self.records.append(record)
                self.wire_bytes += size
            else:
                # Actual IDR authorization comes from the matching admission in plan().
                self._candidate = (frame.ordinal, obj["sequence"], obj["pts"], deadline)
            while self.records and (len(self.records) > self.max_records or
                    self.wire_bytes > self.max_wire_bytes or
                    self._watermark - self.records[0].start > self.max_duration):
                self._pop()
            self.expire()
            self.peak_records = max(self.peak_records, len(self.records))
            self.peak_wire_bytes = max(self.peak_wire_bytes, self.wire_bytes)
        except ValueError:
            self.close()
            raise

    def plan(self, admission, *, preceding=0):
        """Return immutable scalar/hash records, or None when coverage is unavailable.

        'preceding' is an explicit diagnostic choice, never a decoder-preroll promise.
        IDR boundaries use half-open AAC intervals: start <= IDR < end.
        """
        self.expire()
        integer(preceding, 0, 8)
        require_admission(admission)
        snapshot = admission.snapshot
        if (not self.queue.queue.empty() or admission.queue.detached or
                admission.queue._epoch is not self.queue._epoch or
                snapshot.origin is not self.snapshot.origin or snapshot.generation != self.snapshot.generation or
                self._candidate is None or self._candidate[:3] !=
                (snapshot.first_ordinal, snapshot.first_sequence, snapshot.first_video_pts)):
            raise ValueError("window plan needs current same-source IDR admission")
        boundary = snapshot.first_video_pts * self._video_clock
        records = tuple(self.records)
        for index, record in enumerate(records):
            if record.start <= boundary < record.end:
                if index < preceding:
                    return None
                return records[index - preceding:]
        return None
