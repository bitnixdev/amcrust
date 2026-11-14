"""Experimental shared late-IDR admission after common-clock normalization.

Deliberately restricted to the existing Annex-B, explicit zero-reorder Main/High
SPS/PPS evidence. This is not a general AVC parser or production subscription API.
"""
from dataclasses import dataclass
from fractions import Fraction

from h264_reorder import ReorderEvidence, annexb
from packet_ingest_probe import (MAX_FRAME, SharedSource, SharedCursor, SharedMediaQueue,
                                 QueueClosed, byte_field, integer)
from packet_wire import inspect_metadata


@dataclass(frozen=True)
class Snapshot:
    __slots__ = ("generation", "origin", "hello", "streams", "first_ordinal",
                 "first_sequence", "first_video_pts")
    generation: int
    origin: object
    hello: object
    streams: tuple
    first_ordinal: int
    first_sequence: int
    first_video_pts: int


class _Epoch:
    def __init__(self):
        self.active = True


_KEY = object()


class _AdmittedQueue(SharedMediaQueue):
    def __init__(self, snapshot, epoch, max_bytes, max_duration, key):
        if key is not _KEY or not epoch.active:
            raise ValueError("late queues require current source admission")
        super().__init__(max_bytes, max_duration, generation=snapshot.generation)
        cursor = SharedCursor(snapshot.generation)
        # This is source-issued bootstrap state, not subscriber-selected counters.
        cursor.origin, cursor.hello = snapshot.origin, snapshot.hello
        cursor.streams = {stream["index"]: stream for stream in snapshot.streams}
        cursor.ordinal, cursor.sequence = snapshot.first_ordinal, snapshot.first_sequence
        self.protocol, self._epoch = cursor, epoch

    def _decode(self, frame):
        if not self._epoch.active:
            raise ValueError("admission generation retired")
        return super()._decode(frame)


@dataclass(frozen=True)
class Admission:
    __slots__ = ("snapshot", "queue")
    snapshot: Snapshot
    queue: _AdmittedQueue


class LateAdmission:
    """Packet-driven admission: join() returns None until the current frame is IDR.

    No historical IDR buffer: the next source frame expires the candidate. Existing
    queues continue on original global ordinals/sequences and unmodified clocks.
    Live failure, clock expiry or reconnect must call close(); finite end may drain
    before close(). This gate does not replace the supervisor's idle/freshness timers.
    """
    def __init__(self, generation, *, max_subscribers=8, max_bytes=MAX_FRAME * 2,
                 max_duration=Fraction(1, 2)):
        integer(max_subscribers, 1, 8)
        # Validate budgets before any admission can be attempted.
        check = SharedMediaQueue(max_bytes, max_duration, generation=generation)
        check.detach()
        self._source = SharedSource(generation)
        self._generation = generation
        self._epoch = _Epoch()
        self._queues = []
        self._candidate = self._evidence = self._video = None
        self._last_dts = {}
        self._ended = False
        self._max_subscribers, self._max_bytes = max_subscribers, max_bytes
        self._max_duration = max_duration

    def close(self, reason="cancelled"):
        # Validate first, so a caller typo cannot partially retire the generation.
        if reason not in SharedMediaQueue.CLOSE_REASONS:
            raise ValueError("unknown subscriber close reason")
        self._epoch.active = False
        for queue in self._queues:
            queue.detach(reason)
        self._queues.clear()
        self._candidate = self._evidence = self._video = self._source = None
        self._last_dts.clear()

    def feed(self, raw):
        if not self._epoch.active:
            raise ValueError("admission generation retired")
        self._candidate = None
        try:
            frame = self._source.accept(raw)
            metadata = frame.metadata
            kind = metadata["type"]
            if kind == "stream" and len(self._source.streams) == self._source.hello["streams"]:
                streams = tuple(self._source.streams.values())
                videos = [stream for stream in streams if stream["media"] == "video"]
                if (len(videos) != 1 or len(streams) > 2 or
                        any(stream["media"] == "audio" and stream["codec"] != "aac" for stream in streams)):
                    raise ValueError("late admission requires one H264 video and optional AAC")
                video = videos[0]
                if video["codec"] != "h264":
                    raise ValueError("unsupported late-admission video codec")
                self._video = video["index"]
                self._evidence = ReorderEvidence(inspect_metadata(video))
            elif kind == "packet":
                pts = integer(metadata["pts"], -(1 << 63) + 1)
                dts = integer(metadata["dts"], -(1 << 63) + 1)
                integer(metadata["duration"], 1)
                track = metadata["stream"]
                if pts != dts or (track in self._last_dts and dts <= self._last_dts[track]):
                    raise ValueError("late admission requires known non-reordering clocks")
                self._last_dts[track] = dts
                if track == self._video:
                    data = bytes(byte_field(metadata["data"]))
                    self._evidence.check_packet(data)  # every packet, not just candidate IDRs
                    units = annexb(data)
                    if any(nal[0] & 31 == 5 and not nal[0] & 0x60 for nal in units):
                        raise ValueError("IDR must be a reference picture")
                    pictures = {nal[0] & 31 for nal in units if nal[0] & 31 in (1, 5)}
                    if pictures == {1, 5}:
                        raise ValueError("mixed IDR/non-IDR access unit")
                    if pictures == {5} and metadata["flags"] & 1:
                        self._candidate = frame
            elif kind == "end":
                self._ended = True
            self._queues = [queue for queue in self._queues if not queue.detached]
            for queue in self._queues:
                queue.offer(frame)
            return frame
        except ValueError:
            self.close("source_failed")
            raise

    def join(self):
        if not self._epoch.active or self._ended:
            raise ValueError("admission generation closed")
        if self._candidate is None:
            return None
        self._queues = [queue for queue in self._queues if not queue.detached]
        if len(self._queues) >= self._max_subscribers:
            raise ValueError("subscriber limit reached")
        first = self._candidate
        snapshot = Snapshot(self._generation, first.origin, self._source.hello,
                            tuple(self._source.streams.values()), first.ordinal,
                            first.metadata["sequence"], first.metadata["pts"])
        queue = _AdmittedQueue(snapshot, self._epoch, self._max_bytes, self._max_duration, _KEY)
        if not queue.offer(first):
            raise QueueClosed(queue.reason)
        self._queues.append(queue)
        return Admission(snapshot, queue)
