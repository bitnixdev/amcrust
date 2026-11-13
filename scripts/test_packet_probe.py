"""Packet IPC parser/fanout tests; no native libraries required."""
import asyncio
import json
import struct
import unittest

from packet_ingest_probe import MAX_FRAME, MAX_SIDE, PacketQueue, Protocol, read_frame


def encode(value):
    return json.dumps(value).encode()


def hello():
    return {"type": "hello", "version": 1, "generation": 1, "streams": 1,
            "avcodec_version": 1, "skip_samples_type": 11}


def stream():
    return {"type": "stream", "generation": 1, "index": 0, "codec": "h264",
            "media": "video", "time_base": [1, 90000], "extradata": "abcd", "side_data": [],
            **{key: 0 for key in ("profile", "level", "width", "height", "sample_rate", "channels",
                                  "initial_padding", "trailing_padding", "seek_preroll")}}


def packet():
    return {"type": "packet", "generation": 1, "sequence": 0, "stream": 0,
            "pts": 9007199254740993, "dts": -3000, "duration": 6000, "flags": 1,
            "data": "0102", "side_data": []}


def ready():
    state = Protocol()
    state.accept(encode(hello()))
    state.accept(encode(stream()))
    return state


class ProtocolTests(unittest.TestCase):
    def test_exact_large_signed_timestamps_and_missing_are_distinct(self):
        obj = packet()
        decoded = ready().accept(encode(obj))
        self.assertEqual(decoded["pts"], 9007199254740993)
        self.assertEqual(decoded["dts"], -3000)
        for pts in (None, 0):
            obj["pts"] = pts
            self.assertIs(ready().accept(encode(obj))["pts"], pts)

    def test_bad_lifecycle(self):
        for obj in (packet(), stream(), {**hello(), "version": 2}, {**hello(), "generation": 2}):
            with self.assertRaises(ValueError):
                Protocol().accept(encode(obj))
        state = ready()
        for obj in (hello(), stream(), {**packet(), "sequence": 1}, {**packet(), "stream": 1},
                    {"type": "end", "generation": 1, "packets": 1}):
            with self.assertRaises(ValueError):
                state.accept(encode(obj))

    def test_end_and_trailing_frame(self):
        state = ready()
        state.accept(encode(packet()))
        state.accept(encode({"type": "end", "generation": 1, "packets": 1}))
        self.assertTrue(state.ended)
        with self.assertRaises(ValueError):
            state.accept(encode(packet()))

    def test_missing_configuration(self):
        state = Protocol()
        state.accept(encode(hello()))
        with self.assertRaises(ValueError):
            state.accept(encode(packet()))

    def test_malformed_schema_and_byte_fields(self):
        for raw in (b"{", b"[]", b"null", b"\xff", encode({"type": "packet"})):
            with self.assertRaises(ValueError):
                ready().accept(raw)
        for field, value in (("data", "ab cd"), ("data", "a"), ("pts", True),
                             ("pts", 1.2), ("pts", -(1 << 63)), ("duration", -1),
                             ("generation", 2), ("side_data", [{"type": 1, "data": "00" * (MAX_SIDE + 1)}])):
            with self.assertRaises(ValueError):
                ready().accept(encode({**packet(), field: value}))


class FramingTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_and_coalesced_frames(self):
        reader = asyncio.StreamReader()
        raw = encode(hello())
        wire = struct.pack("!I", len(raw)) + raw
        task = asyncio.create_task(read_frame(reader))
        reader.feed_data(wire[:2])
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        reader.feed_data(wire[2:] + wire)
        reader.feed_eof()
        self.assertEqual(await task, raw)
        self.assertEqual(await read_frame(reader), raw)
        self.assertIsNone(await read_frame(reader))

    async def test_truncation_and_size_limit(self):
        for wire in (b"\x00", struct.pack("!I", 5) + b"x",
                     struct.pack("!I", 0), struct.pack("!I", MAX_FRAME + 1)):
            reader = asyncio.StreamReader()
            reader.feed_data(wire)
            reader.feed_eof()
            with self.assertRaises(ValueError):
                await read_frame(reader)

    async def test_oversize_header_rejected_without_reading_body(self):
        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack("!I", MAX_FRAME + 1))
        with self.assertRaises(ValueError):
            await asyncio.wait_for(read_frame(reader), 0.1)

    async def test_byte_and_count_bounds_detach_only_slow_subscriber(self):
        fast = PacketQueue(10, 2)
        slow = PacketQueue(3, 2)
        for frame in (b"abc", b"def"):
            fast.offer(frame)
            slow.offer(frame)
        self.assertFalse(fast.detached)
        self.assertTrue(slow.detached)
        self.assertEqual(slow.bytes, 0)
        self.assertEqual(slow.peak, 3)
        self.assertEqual(await fast.receive(), b"abc")
        self.assertEqual(await fast.receive(), b"def")
        self.assertEqual(fast.bytes, 0)
        bounded = PacketQueue(100, 1)
        bounded.offer(b"a")
        bounded.offer(b"b")
        self.assertTrue(bounded.detached)
        bounded.offer(b"ignored")
        self.assertEqual(bounded.bytes, 0)


if __name__ == "__main__":
    unittest.main()
