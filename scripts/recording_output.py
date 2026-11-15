"""Experimental owned mux/delivery job, not HDS, a privacy sandbox or cloud acknowledgment."""
import asyncio
from pathlib import Path
import sys
import tempfile

from av_output_probe import pack_wire
from ingest_probe import stop_process
from packet_ingest_probe import integer
from packet_wire import encode_frame
from recording_cut import RecordingCutInput


class RecordingOutput:
    """One-use owner; supervisor explicitly cancels on privacy/session loss.

    Receiver.write(bytes) and Receiver.finish() are cooperative async callbacks.
    Writes do not overlap. Already accepted bytes/external aliases cannot be revoked.
    """
    def __init__(self, recorder, sink, receiver, *, timeout=10, max_output_bytes=8 * 1024 * 1024,
                 chunk_bytes=64 * 1024):
        if type(recorder) is not RecordingCutInput or recorder.state != "prepared":
            raise ValueError("prepared recording input required")
        self._max_output = integer(max_output_bytes, 1, 32 * 1024 * 1024)
        self._chunk = integer(chunk_bytes, 1, 64 * 1024)
        if type(timeout) not in (float, int) or not 0 < timeout <= 30:
            raise ValueError("output timeout must be finite and at most 30 seconds")
        if not all(callable(getattr(receiver, name, None)) for name in ("write", "finish")):
            raise ValueError("async receiver write/finish callbacks required")
        # Conservative pre-transfer bound: retained inspection-or-wire accounting
        # dominates every encoded body. Include framing and the not-yet-appended end.
        end = encode_frame({"type": "end", "generation": recorder._generation, "packets": recorder._count})
        if recorder._bytes + 4 * len(recorder._frames) + len(end) + 4 > 4 * 1024 * 1024:
            raise ValueError("recording output input budget")
        self._timeout = timeout
        self._sink, self._receiver = str(Path(sink).resolve()), receiver
        self._wire = pack_wire(recorder.commit_cut())
        if len(self._wire) > 4 * 1024 * 1024:
            self._wire = None
            raise ValueError("recording output input budget")
        self._task = self._process = self._spawn = self._directory = None
        self.state, self.reason = "ready", None
        self.writes = self.bytes_sent = 0
        self.worker_reaped = False
        self.path = None

    @property
    def input_bytes(self):
        return len(self._wire) if self._wire is not None else 0

    def cancel(self, reason="cancelled"):
        if self.reason is not None or self.state == "complete":
            return
        if not isinstance(reason, str) or not reason:
            raise ValueError("nonempty output cancellation reason required")
        self.reason, self.state = reason, "cancelled"
        self._wire = None
        self._receiver = None if self._task is None else self._receiver
        if self._task is not None:
            self._task.cancel()

    def _active(self):
        if self.reason is not None:
            raise asyncio.CancelledError

    async def _execute(self):
        self._active()
        self._directory = tempfile.TemporaryDirectory(prefix="amcrust-recording-output-")
        self.path = Path(self._directory.name) / "recording.mp4"
        self.state = "muxing"
        self._spawn = asyncio.create_task(asyncio.create_subprocess_exec(
            sys.executable, "-B", str(Path(__file__).with_name("recording_output_limit.py")),
            str(self._max_output), self._sink, str(self.path),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL))
        # Cleanup always awaits this spawn and takes its handle, even if owner cancellation wins.
        self._process = await asyncio.shield(self._spawn)
        self._active()
        view = memoryview(self._wire)
        try:
            for offset in range(0, len(view), self._chunk):
                self._active()
                self._process.stdin.write(view[offset:offset + self._chunk])
                await self._process.stdin.drain()
        finally:
            view.release()
            self._wire = None
        self._process.stdin.close()
        await self._process.wait()
        self.worker_reaped = True
        self._active()
        if self._process.returncode:
            raise ValueError("recording mux failed or exceeded output limit")
        size = self.path.stat().st_size
        if not 0 < size <= self._max_output:
            raise ValueError("invalid bounded recording output")
        self.state = "delivering"
        with self.path.open("rb") as output:
            while True:
                self._active()
                chunk = output.read(self._chunk)
                if not chunk:
                    break
                try:
                    await self._receiver.write(chunk)
                    self._active()
                    self.writes += 1
                    self.bytes_sent += len(chunk)
                finally:
                    del chunk
        self._active()
        self.state = "finishing"
        await self._receiver.finish()
        self._active()

    async def _cleanup(self):
        try:
            if not self._task.done():
                self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            if self._spawn is not None:
                try:
                    self._process = await self._spawn
                except (Exception, asyncio.CancelledError):
                    pass  # unsuccessful spawn has no owned process handle
            if self._process is not None:
                try:
                    await stop_process(self._process)
                    self.worker_reaped = self._process.returncode is not None
                finally:
                    try:
                        await self._process.stdin.wait_closed()
                    except (BrokenPipeError, ConnectionResetError):
                        pass  # expected when terminating a worker with pending input
        finally:
            self._wire = self._receiver = None
            if self._directory is not None:
                self._directory.cleanup()
                self._directory = None
            self._task = self._spawn = self._process = None

    async def run(self):
        if self._task is not None or self.state != "ready":
            raise ValueError("output job is one-use and must be active")
        self._task = asyncio.create_task(self._execute())
        try:
            await asyncio.wait_for(asyncio.shield(self._task), self._timeout)
        except asyncio.TimeoutError:
            self.cancel("timeout")
            raise
        except asyncio.CancelledError:
            self.cancel()
            raise
        except Exception:
            if self.reason is None:
                self.reason, self.state = "output_failed", "failed"
            raise
        finally:
            cleanup = asyncio.create_task(self._cleanup())
            interrupted = False
            while True:
                try:
                    await asyncio.shield(cleanup)
                    break
                except asyncio.CancelledError:
                    interrupted = True
                    self.cancel()
                except Exception:
                    if self.reason is None:
                        self.reason, self.state = "cleanup_failed", "failed"
                    raise
            if interrupted:
                raise asyncio.CancelledError
        self._active()
        self.state = "complete"
