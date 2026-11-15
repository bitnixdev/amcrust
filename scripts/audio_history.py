"""Experimental source-owned AAC bodies and cooperative history/live handoff.

HistorySource is the lifecycle owner: close it for privacy, source loss or retirement.
Do not close its private gate independently. Escaped immutable bytes/views cannot be
revoked; releasing a prefix promises the consumer has dropped its borrowed aliases.
"""
import asyncio

from audio_window import AudioWindowIndex
from packet_ingest_probe import QueueClosed, integer
from shared_admission import LateAdmission
from subscriber_budget import Delivery, LeasedMediaQueue


_KEY = object()


class _Body:
    def __init__(self, raw):
        self.raw = raw
        self.pins = 0
        self.cached = True


class HistoryLease:
    def __init__(self, handoff, records, key=None):
        if key is not _KEY:
            raise ValueError("history leases require source selection")
        self._handoff = handoff
        self._records = records
        self._released = False
        self._deadline = min(record.deadline for record in records)
        self._size = sum(record.wire_bytes for record in records)

    @property
    def deadline(self):
        return self._deadline

    @property
    def size(self):
        return self._size

    @property
    def released(self):
        return self._released

    @property
    def frames(self):
        owner = self._handoff._owner
        owner.expire()
        if self._released or self._handoff.closed:
            raise ValueError("history lease released or retired")
        return tuple(owner._bodies[r.sequence].raw for r in self._records)

    def _release(self):
        if self._released:
            return
        self._released = True
        owner = self._handoff._owner
        for record in self._records:
            owner._bodies[record.sequence].pins -= 1
        self._records = ()
        owner._prune()

    def release(self):
        self._handoff._owner.expire()
        self._release()
        self._handoff._owner._arm()

    def __enter__(self):
        self.frames
        return self

    def __exit__(self, *exc):
        self.release()


class Handoff:
    def __init__(self, owner, admission, records, key=None):
        if key is not _KEY:
            raise ValueError("handoffs require source selection")
        self._owner, self._admission = owner, admission
        self._closed, self._reason = False, None
        self._closed_event = asyncio.Event()
        self._history = HistoryLease(self, records, _KEY)

    @property
    def admission(self):
        return self._admission

    @property
    def history(self):
        return self._history

    @property
    def closed(self):
        return self._closed

    @property
    def reason(self):
        return self._reason

    def _drained(self):
        queue = self.admission.queue
        return (self.history.released and queue.protocol is not None and queue.protocol.ended and
                queue.queue.empty() and not queue.inflight_frames)

    def close(self, reason="cancelled"):
        if self.closed:
            return
        if reason not in LeasedMediaQueue.CLOSE_REASONS:
            raise ValueError("unknown handoff close reason")
        if self.admission.queue.detached:
            reason = self.admission.queue.reason
        if reason == "source_complete" and not self.admission.queue.detached and not self._drained():
            raise ValueError("finite handoff has not drained")
        self._closed, self._reason = True, reason
        self.admission.queue.detach(reason)
        self.history._release()
        self._owner._handoffs.discard(self)
        self._closed_event.set()

    async def wait_closed(self):
        """Observe owner/handoff closure, including idle held-prefix expiry."""
        await self._closed_event.wait()
        return self.reason

    def finish(self):
        """Acknowledge a validated finite end only after all delivered leases drain."""
        self._owner.expire()
        if self.closed:
            raise QueueClosed(self.reason)
        if not self._drained():
            raise ValueError("finite handoff has not drained")
        self.close("source_complete")

    async def receive(self):
        self._owner.expire()
        if self.closed:
            raise QueueClosed(self.reason)
        if not self.history.released:
            raise ValueError("release historical prefix before receiving live media")
        try:
            delivery = await self.admission.queue.receive()
            self._owner.expire()
            if self.closed:
                delivery.release()
                raise QueueClosed(self.reason)
            return delivery
        except QueueClosed as error:
            self.close(error.reason)
            raise
        except asyncio.CancelledError:
            self.close("cancelled")
            raise


class _History:
    def __init__(self, admission, *, max_handoffs=4, **budgets):
        self.index = AudioWindowIndex(admission, **budgets)
        self.max_handoffs = integer(max_handoffs, 1, 7)
        self._bodies, self._handoffs = {}, set()
        self.closed, self.reason, self.sealed = False, None, False
        self.bytes = self.peak_bytes = self.peak_bodies = 0
        self._timer = None

    @property
    def leased_bytes(self):
        # Per-consumer charges may overlap; never add this to unique owned bytes.
        return sum(h.history.size for h in self._handoffs if not h.history.released)

    @property
    def pinned_bytes(self):
        return sum(len(body.raw) for body in self._bodies.values() if body.pins)

    def _prune(self):
        cached = {r.sequence for r in self.index.records} if not self.closed else set()
        for sequence, body in list(self._bodies.items()):
            body.cached = sequence in cached
            if not body.cached and not body.pins:
                self.bytes -= len(body.raw)
                del self._bodies[sequence]

    def close(self, reason="cancelled"):
        if self.closed:
            return
        if reason not in LeasedMediaQueue.CLOSE_REASONS:
            raise ValueError("unknown history close reason")
        if reason == "source_complete" and any(not h.admission.queue.detached and not h._drained()
                                                for h in self._handoffs):
            raise ValueError("finite history handoffs have not drained")
        self.closed, self.reason = True, reason
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        for handoff in tuple(self._handoffs):
            handoff.close(reason)
        self.index.queue.detach(reason)
        self.index.close()
        self._prune()

    def expire(self):
        if self.closed:
            return
        try:
            self.index.expire()
        except ValueError:
            self.close(self.index.queue.reason or "source_failed")
            return
        now = self.index.queue._loop.time()
        for handoff in tuple(self._handoffs):
            handoff.admission.queue.expire()
            if handoff.admission.queue.detached:
                handoff.close(handoff.admission.queue.reason)
            elif not handoff.history.released and now >= handoff.history.deadline:
                handoff.close("residence_expired")
        self._prune()

    def _arm(self):
        if self.closed:
            return
        deadlines = [h.history.deadline for h in self._handoffs if not h.history.released]
        if self.index.next_deadline is not None:
            deadlines.append(self.index.next_deadline)
        if not deadlines:
            return  # at most one harmless earlier wakeup remains
        deadline = min(deadlines)
        if self._timer is not None:
            if self._timer.when() <= deadline:
                return
            self._timer.cancel()
        self._timer = self.index.queue._loop.call_at(deadline, self._on_timer)

    def _on_timer(self):
        self._timer = None
        self.expire()
        self._arm()

    def observe(self, delivery):
        if self.closed:
            return
        try:
            if (self.sealed or not isinstance(delivery, Delivery) or
                    delivery._owner is not self.index.queue):
                raise ValueError("closed/foreign history receipt")
            frame = delivery.frame
            if frame.metadata["type"] == "end":
                # Finite input may drain existing handoffs; it never opens a new one.
                if (frame.origin is not self.index.snapshot.origin or
                        frame.ordinal != self.index._ordinal or
                        frame.metadata["packets"] != self.index._sequence):
                    raise ValueError("history end identity/count changed")
                self.sealed = True
                self.index._candidate = None
            else:
                self.index.observe(delivery)
                self.expire()
                if self.closed:
                    return
                self._prune()
                sequence = frame.metadata["sequence"]
                if any(r.sequence == sequence for r in self.index.records):
                    # This is a separately owned copy, not an alias escaping the source lease.
                    size = len(frame.raw)
                    if (len(self._bodies) + 1 > self.index.max_records or
                            self.bytes + size > self.index.max_wire_bytes):
                        self.close("overflow")  # pinned selections cannot be silently evicted
                        return
                    raw = memoryview(frame.raw).tobytes()
                    self._bodies[sequence] = _Body(raw)
                    self.bytes += len(raw)
                    self.peak_bytes = max(self.peak_bytes, self.bytes)
                    self.peak_bodies = max(self.peak_bodies, len(self._bodies))
            self._arm()
        except ValueError:
            self.close("source_failed")

    def select(self, admission, preceding):
        self.expire()
        if self.closed or self.sealed:
            raise QueueClosed(self.reason or "source_complete")
        if len(self._handoffs) >= self.max_handoffs:
            raise ValueError("history handoff limit")
        records = self.index.plan(admission, preceding=preceding)
        if records is None:
            return None
        if any(r.sequence not in self._bodies for r in records):
            raise ValueError("history body missing")
        handoff = Handoff(self, admission, records, _KEY)
        for record in records:
            self._bodies[record.sequence].pins += 1
        self._handoffs.add(handoff)
        self._arm()
        return handoff


class HistorySource:
    """Own the gate/history together so retirement invalidates history and live leases."""
    def __init__(self, generation, *, history_budgets=None, **gate_budgets):
        self._gate = LateAdmission(generation, **gate_budgets)
        self._budgets = dict(history_budgets or {})
        self.history = None

    async def feed(self, raw):
        try:
            frame = self._gate.feed(raw)
            if self.history is None and frame.metadata["type"] != "end":
                admission = self._gate.join()
                if admission is not None:
                    self.history = _History(admission, **self._budgets)
            if self.history is not None and not self.history.closed:
                queue = self.history.index.queue
                while not queue.queue.empty():
                    with await queue.receive() as delivery:
                        self.history.observe(delivery)
            return frame
        except asyncio.CancelledError:
            self.close("cancelled")
            raise
        except (ValueError, QueueClosed):
            self.close("source_failed")
            raise

    def join(self):
        return self._gate.join()  # ordinary live-only admission remains independent

    def open(self, *, preceding=0):
        if self.history is None:
            return None
        admission = self._gate.join()
        if admission is None:
            return None
        try:
            handoff = self.history.select(admission, preceding)
            if handoff is None:
                admission.queue.detach("cancelled")
            return handoff
        except (ValueError, QueueClosed):
            admission.queue.detach("cancelled")
            raise

    def close(self, reason="cancelled"):
        if reason not in LeasedMediaQueue.CLOSE_REASONS:
            raise ValueError("unknown source close reason")
        if reason == "source_complete" and not self._gate._ended:
            raise ValueError("source lacks validated finite end")
        if self.history is not None:
            self.history.close(reason)
        self._gate.close(reason)
