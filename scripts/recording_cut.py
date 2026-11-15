"""Bounded experimental recording-local IPC cut, not source EOF or HDS completion.

Whole AUs and the original common epoch are retained. Preparation is revocable;
commit transfers ordinary owned diagnostic copies. Later source retirement cannot
revoke those copies or stand in for output delivery/privacy policy.
"""
import hashlib
from fractions import Fraction

from audio_history import Handoff
from late_mux_input import LateMuxInput
from packet_ingest_probe import Protocol
from packet_wire import decode_frame, encode_frame
from shared_admission import require_admission
from subscriber_budget import Delivery


class RecordingCutInput(LateMuxInput):
    def __init__(self, handoff, **budgets):
        if type(handoff) is not Handoff:
            raise ValueError("recording requires a source-issued handoff")
        self._handoff = handoff
        self._state = "open"
        self._seen = set()
        self._cut_sequence = None
        super().__init__(handoff.admission, **budgets)
        try:
            self._active()
            snapshot = handoff.admission.snapshot
            audio = next(s for s in snapshot.streams if s["media"] == "audio")
            clock = Fraction(*audio["time_base"])
            records = handoff.history._records
            with handoff.history as prefix:
                for record, raw in zip(records, prefix.frames):
                    obj = decode_frame(raw)
                    if (obj["type"] != "packet" or obj["generation"] != self._generation or
                            obj["stream"] != audio["index"] or obj["sequence"] != record.sequence or
                            record.ordinal >= self._ordinal or obj["sequence"] >= self._sequence or
                            obj["pts"] * clock != record.start or
                            (obj["pts"] + obj["duration"]) * clock != record.end or
                            hashlib.sha256(bytes.fromhex(obj["data"])).hexdigest() != record.sha256):
                        raise ValueError("recording history does not match source grant")
                    if self._count >= self._max_packets:
                        raise ValueError("recording input packet budget")
                    obj["sequence"] = self._count
                    self._append(obj)
                    self._count += 1
                    self._seen.add(obj["stream"])
            self._active()
        except ValueError:
            self.abort()
            raise

    @property
    def state(self):
        return self._state

    @property
    def cut_sequence(self):
        return self._cut_sequence

    def _active(self):
        self._handoff._owner.expire()
        if self._handoff.closed or self._handoff._owner.sealed:
            raise ValueError("recording grant retired or source ended")
        require_admission(self._handoff.admission)

    def abort(self):
        if self._state == "committed":
            return  # transferred copies are no longer this builder's property
        self._state = "aborted"
        self._handoff.close("cancelled")
        super().abort()

    def feed(self, delivery):
        try:
            if self._state != "open":
                raise ValueError("recording input is not open")
            self._active()
            if not isinstance(delivery, Delivery) or delivery._owner is not self._queue:
                raise ValueError("foreign/released recording receipt")
            obj = delivery.frame.metadata
            if obj["type"] != "packet":
                raise ValueError("source end is not a recording cut")
            super().feed(delivery)  # original source ordinal/sequence, private counter only
            self._seen.add(obj["stream"])
        except ValueError:
            self.abort()
            raise

    def prepare_cut(self):
        try:
            if self._state != "open":
                raise ValueError("recording cut is not open")
            self._active()
            snapshot = self._handoff.admission.snapshot
            if (self._queue.inflight_frames or self._sequence == snapshot.first_sequence or
                    self._seen != {s["index"] for s in snapshot.streams}):
                raise ValueError("recording cut requires complete tracks and released receipts")
            self._cut_sequence = self._sequence - 1
            self._state = "prepared"
            return self._cut_sequence
        except ValueError:
            self.abort()
            raise

    def commit_cut(self):
        try:
            if self._state != "prepared":
                raise ValueError("recording cut was not prepared")
            self._active()
            if self._queue.inflight_frames:
                raise ValueError("recording cut has outstanding receipts")
            self._append({"type": "end", "generation": self._generation, "packets": self._count})
            protocol = Protocol(self._generation)
            for obj in self._frames:
                protocol.accept(encode_frame(obj))
            self._active()  # source/lease expiry wins until this transfer point
            if not protocol.ended:
                raise ValueError("recording-local end missing")
            result, self._frames = tuple(self._frames), []
            self._bytes = 0
            self._closed = True
            self._state = "committed"
            # Later queued source media is deliberately excluded, not delivered or retimed.
            self._handoff.close("cancelled")
            self._queue = None
            return result
        except ValueError:
            self.abort()
            raise
