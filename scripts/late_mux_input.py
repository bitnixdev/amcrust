"""Bounded finite diagnostic adapter, not a live recording or preroll implementation.

Only private sink IPC counters are rebased. Published frames and media clocks are
never changed. Input must come from this admission's active delivery leases.
"""
import json

from packet_ingest_probe import Protocol, integer
from packet_wire import encode_frame, inspect_metadata
from subscriber_budget import Delivery


class LateMuxInput:
    def __init__(self, admission, *, max_bytes=2 * 1024 * 1024, max_packets=256):
        self._queue = admission.queue
        self._frames = []
        self._closed = self._ended = False
        self._bytes = self._count = 0
        self._origin = admission.snapshot.origin
        self._generation = admission.snapshot.generation
        self._ordinal = admission.snapshot.first_ordinal
        self._sequence = admission.snapshot.first_sequence
        try:
            self._max_bytes = integer(max_bytes, 1, 32 * 1024 * 1024)
            self._max_packets = integer(max_packets, 1, 256)
            self._queue.expire()
            if self._queue.detached:
                raise ValueError("retired recording admission")
            for metadata in (admission.snapshot.hello, *admission.snapshot.streams):
                self._append(inspect_metadata(metadata))
        except ValueError:
            self.abort()
            raise

    def _append(self, obj):
        # Inspection hex can be larger than AMC2. Bound retained representation too.
        size = max(len(encode_frame(obj)), len(json.dumps(obj, separators=(",", ":")).encode()))
        if self._bytes + size > self._max_bytes:
            raise ValueError("recording input byte budget")
        self._frames.append(obj)
        self._bytes += size

    def abort(self):
        if self._queue is not None:
            self._queue.detach("cancelled")
        self._queue = None
        self._frames.clear()
        self._bytes = 0
        self._closed = True

    def feed(self, delivery):
        try:
            if self._closed or self._ended:
                raise ValueError("closed recording input")
            if not isinstance(delivery, Delivery) or delivery._owner is not self._queue:
                raise ValueError("foreign/released recording delivery")
            frame = delivery.frame  # enforces lease lifetime and generation retirement
            metadata = frame.metadata
            if (frame.origin is not self._origin or frame.ordinal != self._ordinal or
                    metadata["generation"] != self._generation):
                raise ValueError("recording source identity/ordinal/generation changed")
            obj = inspect_metadata(metadata)
            if obj["type"] == "packet":
                if obj["sequence"] != self._sequence:
                    raise ValueError("recording source sequence gap")
                if self._count >= self._max_packets:
                    raise ValueError("recording input packet budget")
                obj["sequence"] = self._count  # sink-local only; original frame stays immutable
                self._append(obj)
                self._count += 1
                self._sequence += 1
            elif obj["type"] == "end":
                if obj["packets"] != self._sequence or not self._count:
                    raise ValueError("recording source end count")
                obj["packets"] = self._count
                self._append(obj)
                self._ended = True
            else:
                raise ValueError("unexpected recording input frame")
            self._ordinal += 1
        except ValueError:
            self.abort()
            raise

    def finish(self):
        try:
            if self._queue is not None:
                self._queue.expire()
            if self._closed or not self._ended or self._queue.detached:
                raise ValueError("recording input lacks active validated finite end")
            protocol = Protocol(self._generation)
            for obj in self._frames:
                protocol.accept(encode_frame(obj))
            self._queue.expire()
            if self._queue.detached:
                raise ValueError("recording admission retired during finalization")
        except ValueError:
            self.abort()
            raise
        frames, self._frames = tuple(self._frames), []
        self._bytes = 0
        self._closed = True
        self._queue = None
        return frames
