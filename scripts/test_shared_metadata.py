"""One-time schema parsing with immutable publication and local lifecycle guards."""
import asyncio
from dataclasses import FrozenInstanceError
from fractions import Fraction
from unittest.mock import patch
import unittest

from packet_ingest_probe import (SharedSource, SharedCursor, SharedFrame, SharedMediaQueue,
                                 QueueClosed, byte_field)
from packet_wire import ByteRange, decode_frame, encode_frame, inspect_metadata
from test_packet_probe import hello, stream, packet


def objects():
    return [hello(), stream(), {**packet(), "pts": 0, "dts": 0, "duration": 6000,
            "side_data": [{"type": 11, "data": "00ab"}]},
            {"type": "end", "generation": 1, "packets": 1}]


def published():
    source = SharedSource()
    return source, [source.accept(encode_frame(obj)) for obj in objects()]


class SharedMetadataTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_parse_per_frame_regardless_of_subscriber_count(self):
        source = SharedSource()
        queues = [SharedMediaQueue(100000, 1) for _ in range(8)]
        cursors = [SharedCursor() for _ in queues]
        with patch("packet_ingest_probe.decode_frame", wraps=decode_frame) as decode:
            for obj in objects():
                raw = encode_frame(obj)
                frame = source.accept(raw)
                for queue, cursor in zip(queues, cursors):
                    self.assertTrue(queue.offer(frame))
                    self.assertIs(await queue.receive(), frame)
                    self.assertIs(cursor.accept(frame), frame.metadata)
                    if obj["type"] == "stream":
                        self.assertIs(queue.protocol.streams[0], frame.metadata)
                        self.assertIs(source.streams[0], frame.metadata)
                self.assertEqual(inspect_metadata(frame.metadata), obj)
            self.assertEqual(decode.call_count, len(objects()))
        self.assertEqual(source.parse_count, len(objects()))
        self.assertTrue(all(cursor.ended for cursor in cursors))
        self.assertTrue(all(queue.protocol.ended for queue in queues))

    def test_nested_metadata_and_ranges_are_immutable(self):
        _, frames = published()
        descriptor, packet_frame = frames[1:3]
        with self.assertRaises(TypeError):
            descriptor.metadata["time_base"][0] = 99
        with self.assertRaises(TypeError):
            packet_frame.metadata["side_data"][0]["type"] = 99
        with self.assertRaises(TypeError):
            packet_frame.metadata["pts"] = 99
        with self.assertRaises(FrozenInstanceError):
            packet_frame.ordinal = 99
        data = packet_frame.metadata["data"]
        self.assertIsInstance(data, ByteRange)
        with self.assertRaises(FrozenInstanceError):
            data.offset = 0
        left, right = byte_field(data), byte_field(data)
        self.assertIsNot(left, right)
        self.assertIs(left.obj, packet_frame.raw)
        left.release()
        self.assertEqual(right, b"\x01\x02")
        self.assertEqual(byte_field(data), right)

    def test_constructor_cannot_accidentally_publish_unvalidated_metadata(self):
        with self.assertRaises(ValueError):
            SharedFrame(b"bad", {}, object(), 0, object())
        with self.assertRaises(ValueError):
            ByteRange(b"x", 0, 2)
        with self.assertRaises(ValueError):
            ByteRange(bytearray(b"x"), 0, 1)

    def test_same_generation_from_distinct_sources_is_not_interchangeable(self):
        _, first = published()
        _, second = published()
        cursor = SharedCursor()
        cursor.accept(first[0])
        with self.assertRaisesRegex(ValueError, "identity"):
            cursor.accept(second[1])
        self.assertTrue(cursor.failed)
        with self.assertRaises(ValueError):
            cursor.accept(first[1])
        self.assertFalse(cursor.streams)

    def test_gaps_duplicates_late_join_and_trailing_frames_fail_closed(self):
        for order in ((1,), (0, 2), (0, 1, 1), (0, 1, 2, 3, 3)):
            _, frames = published()
            cursor = SharedCursor()
            with self.assertRaises(ValueError):
                for index in order:
                    cursor.accept(frames[index])
            self.assertTrue(cursor.failed)
        _, frames = published()
        with self.assertRaises(ValueError):
            SharedCursor(2).accept(frames[0])
        with self.assertRaises(ValueError):
            SharedCursor().accept(frames[0].raw)

    def test_source_failure_clears_state_and_cannot_resume(self):
        source = SharedSource()
        first = source.accept(encode_frame(hello()))
        source.accept(encode_frame(stream()))
        with self.assertRaises(ValueError):
            source.accept(encode_frame({**packet(), "sequence": 5}))
        self.assertTrue(source.failed)
        self.assertIsNone(source.hello)
        self.assertFalse(source.streams)
        with self.assertRaises(ValueError):
            source.accept(encode_frame(packet()))
        self.assertEqual(first.metadata["type"], "hello")  # cannot revoke prior delivery

    async def test_queue_failure_wakes_waiter_without_affecting_peer(self):
        _, frames = published()
        queues = [SharedMediaQueue(100000, 1) for _ in range(2)]
        for queue in queues:
            for frame in frames[:2]:
                queue.offer(frame)
                await queue.receive()
        waiter = asyncio.create_task(queues[0].receive())
        await asyncio.sleep(0)
        with self.assertRaises(ValueError):
            queues[0].offer(frames[3])  # skip a packet
        with self.assertRaises(QueueClosed):
            await asyncio.wait_for(waiter, 0.5)
        self.assertIsNone(queues[0].protocol)
        self.assertTrue(queues[1].offer(frames[2]))
        self.assertIs(await queues[1].receive(), frames[2])
        self.assertFalse(queues[1].detached)

    async def test_media_budget_and_byte_count_backstops(self):
        source = SharedSource()
        slow = SharedMediaQueue(100000, Fraction(1, 10))
        fast = SharedMediaQueue(100000, Fraction(1, 10))
        for obj in (hello(), stream()):
            frame = source.accept(encode_frame(obj))
            for queue in (slow, fast):
                queue.offer(frame)
                await queue.receive()
        for sequence in range(2):
            frame = source.accept(encode_frame({**packet(), "sequence": sequence,
                "pts": sequence * 6000, "dts": sequence * 6000, "duration": 6000}))
            slow.offer(frame)
            self.assertTrue(fast.offer(frame))
            self.assertIs(await fast.receive(), frame)
        self.assertEqual(slow.overflow_limit, "media")
        self.assertEqual(slow.bytes, 0)
        self.assertFalse(slow.spans)
        self.assertFalse(fast.detached)
        _, frames = published()
        for queue, limit in ((SharedMediaQueue(1, 1), "bytes"),
                             (SharedMediaQueue(100000, 1, max_frames=1), "frames")):
            for frame in frames[:2]:
                queue.offer(frame)
            self.assertEqual(queue.overflow_limit, limit)

    async def test_delivered_metadata_survives_queue_and_source_cleanup(self):
        source, frames = published()
        queue = SharedMediaQueue(100000, 1)
        for frame in frames:
            queue.offer(frame)
            received = await queue.receive()
            if received.metadata["type"] == "packet":
                held = received.metadata
        queue.detach("source_complete")
        del source, frames, received, frame
        self.assertIsNone(queue.protocol)
        self.assertEqual(byte_field(held["data"]), b"\x01\x02")
        self.assertEqual(byte_field(held["side_data"][0]["data"]), b"\x00\xab")
