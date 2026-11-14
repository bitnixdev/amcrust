"""Cooperative delivery leases; bounds managed ownership, not escaped Python aliases."""
import asyncio
from collections import deque
import math

from packet_ingest_probe import MAX_FRAME, QueueClosed, SharedMediaQueue, integer


class Delivery:
    __slots__ = ("_owner", "_frame", "_deadline", "_size", "_identifier")

    def __init__(self, owner, frame, deadline, identifier):
        self._owner, self._frame = owner, frame
        self._deadline, self._size, self._identifier = deadline, len(frame), identifier

    @property
    def deadline(self):
        return self._deadline

    @property
    def size(self):
        return self._size

    @property
    def identifier(self):
        return self._identifier

    @property
    def frame(self):
        if self._owner is not None:
            self._owner.expire()
        if self._owner is None:
            raise ValueError("delivery released or retired")
        return self._frame

    def release(self):
        if self._owner is not None:
            self._owner._release(self.identifier)

    def _retire(self):
        self._owner = self._frame = None

    def __enter__(self):
        self.frame  # refuse entry after release/expiry
        return self

    def __exit__(self, *exc):
        self.release()


def residence_seconds(value):
    if type(value) not in (int, float) or not 0 < value <= 30 or not math.isfinite(value):
        raise ValueError("residence budget must be finite positive seconds, at most 30")
    return float(value)


class LeasedMediaQueue(SharedMediaQueue):
    """Single-loop queue with one offer-to-release deadline, including in-flight time.

    receive() transfers a frame to an explicit Delivery, not an unaccounted raw return.
    Deadline timers also run when the source and consumers are idle. Release promises
    that consumers no longer retain frame/view aliases; Python cannot enforce that.
    """
    CLOSE_REASONS = SharedMediaQueue.CLOSE_REASONS | {"residence_expired", "delivery_limit"}

    def __init__(self, *args, max_residence=2.0, max_inflight_frames=2,
                 max_inflight_bytes=MAX_FRAME, **kwargs):
        self.max_residence = residence_seconds(max_residence)
        self.max_inflight_frames = integer(max_inflight_frames, 1, 256)
        self.max_inflight_bytes = integer(max_inflight_bytes, 1, MAX_FRAME * 2)
        super().__init__(*args, **kwargs)
        self._loop = asyncio.get_running_loop()
        self._deadlines = deque()
        self._deliveries = {}
        self._timer = None
        self._identifier = 0
        self.inflight_bytes = self.peak_inflight_bytes = self.peak_inflight_frames = 0

    @property
    def inflight_frames(self):
        return len(self._deliveries)

    @property
    def next_deadline(self):
        values = list(self._deadlines) + [item.deadline for item in self._deliveries.values()]
        return min(values) if values else None

    def _arm(self):
        deadline = self.next_deadline
        if self._timer is not None:
            # Keep an earlier wakeup, even when temporarily empty. Cancelling and
            # allocating on every packet grows asyncio's timer heap until a yield.
            if deadline is None or self._timer.when() <= deadline:
                return
            self._timer.cancel()
            self._timer = None
        if not self.detached and deadline is not None:
            self._timer = self._loop.call_at(deadline, self._on_timer)

    def _on_timer(self):
        self._timer = None
        self.expire()
        if not self.detached:
            self._arm()  # tolerate a loop firing slightly before its deadline

    def expire(self):
        deadline = self.next_deadline
        if not self.detached and deadline is not None and self._loop.time() >= deadline:
            self.detach("residence_expired")

    def offer(self, frame, *, born=None):
        self.expire()
        if self.detached:
            return False
        now = self._loop.time()
        born = now if born is None else born
        if type(born) not in (int, float) or not math.isfinite(born) or born > now:
            raise ValueError("invalid receiver-monotonic birth time")
        deadline = born + self.max_residence
        if now >= deadline:
            self.detach("residence_expired")
            return False
        if not super().offer(frame):
            return False
        self._deadlines.append(deadline)
        self._arm()
        return True

    async def receive(self):
        self.expire()
        frame = await super().receive()
        self.expire()
        if self.detached:
            raise QueueClosed(self.reason)
        deadline = self._deadlines.popleft()
        # Recheck after the await: several waiting receivers can resume in one tick.
        if self._loop.time() >= deadline:
            self.detach("residence_expired")
            raise QueueClosed(self.reason)
        if (self.inflight_frames >= self.max_inflight_frames or
                self.inflight_bytes + len(frame) > self.max_inflight_bytes):
            self.detach("delivery_limit")
            raise QueueClosed(self.reason)
        self._identifier += 1
        delivery = Delivery(self, frame, deadline, self._identifier)
        self._deliveries[delivery.identifier] = delivery
        self.inflight_bytes += delivery.size
        self.peak_inflight_bytes = max(self.peak_inflight_bytes, self.inflight_bytes)
        self.peak_inflight_frames = max(self.peak_inflight_frames, self.inflight_frames)
        self._arm()
        return delivery

    def _release(self, identifier):
        self.expire()
        if self.detached:
            return
        delivery = self._deliveries.pop(identifier)
        self.inflight_bytes -= delivery.size
        delivery._retire()
        self._arm()

    def detach(self, reason="cancelled"):
        super().detach(reason)
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._deadlines.clear()
        for delivery in self._deliveries.values():
            delivery._retire()
        self._deliveries.clear()
        self.inflight_bytes = 0
