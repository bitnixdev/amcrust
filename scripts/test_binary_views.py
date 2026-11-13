"""Binary fanout views: ownership, isolation, and no payload materialization."""
import asyncio
import hashlib
import unittest

from packet_ingest_probe import MediaQueue, Protocol, QueueClosed, byte_field, summarize
from packet_wire import decode_frame, encode_frame
from test_packet_probe import hello, packet, stream


class BinaryViewTests(unittest.IsolatedAsyncioTestCase):
    def test_views_share_only_immutable_backing_bytes(self):
        obj = {**packet(), "data": "00ff007b7d", "side_data": [{"type": 11, "data": "00ab"}]}
        raw = encode_frame(obj)
        left, right = (decode_frame(raw, binary=True) for _ in range(2))
        for owner, key in ((left, "data"), (left["side_data"][0], "data")):
            view = owner[key]
            self.assertIs(view.obj, raw)
            self.assertTrue(view.readonly)
            self.assertIs(byte_field(view), view)
            with self.assertRaises(TypeError):
                view[0] = 1
        self.assertIsNot(left["data"], right["data"])
        left["side_data"][0]["type"] = 99
        self.assertEqual(right["side_data"][0]["type"], 11)
        left["data"].release()
        self.assertEqual(right["data"], bytes.fromhex(obj["data"]))
        self.assertEqual(decode_frame(raw), obj)  # explicit inspection path unchanged

    def test_descriptor_views_survive_later_frames(self):
        protocol = Protocol(binary=True)
        protocol.accept(encode_frame(hello()))
        raw = encode_frame(stream())
        descriptor = protocol.accept(raw)
        self.assertIs(descriptor["extradata"].obj, raw)
        data = protocol.accept(encode_frame(packet()))["data"]
        del raw
        protocol.accept(encode_frame({"type": "end", "generation": 1, "packets": 1}))
        self.assertEqual(descriptor["extradata"], bytes.fromhex(stream()["extradata"]))
        self.assertEqual(data, bytes.fromhex(packet()["data"]))

    def test_hashing_and_side_metadata_use_original_views(self):
        raw = encode_frame({**packet(), "side_data": [{"type": 11, "data": "00ab"}]})
        frame = decode_frame(raw, binary=True)
        summary = summarize(frame)
        self.assertEqual(summary["hash"], "SHA256:" + hashlib.sha256(b"\x01\x02").hexdigest())
        self.assertIs(summary["side_data"][0]["data"], frame["side_data"][0]["data"])
        self.assertIs(summary["side_data"][0]["data"].obj, raw)

    def test_readonly_alias_of_mutable_storage_is_not_accepted(self):
        mutable = bytearray(b"ab")
        for value in (memoryview(mutable), memoryview(mutable).toreadonly(),
                      memoryview(b"abcd")[::2], memoryview(b"abcd").cast("I")):
            with self.assertRaises(ValueError):
                byte_field(value)
        with self.assertRaises(ValueError):
            byte_field(memoryview(b"abc"), maximum=2)

    async def test_consumer_view_outlives_peer_detach_and_queue_drain(self):
        queues = [MediaQueue(100000, 1) for _ in range(2)]
        for frame in (hello(), stream()):
            raw = encode_frame(frame)
            for queue in queues:
                queue.offer(raw)
                await queue.receive()
        raw = encode_frame({**packet(), "pts": 0, "dts": 0, "duration": 6000})
        for queue in queues:
            self.assertTrue(queue.offer(raw))
        held = decode_frame(await queues[0].receive(), binary=True)["data"]
        self.assertIs(held.obj, raw)
        queues[1].detach("source_failed")
        queues[0].detach("cancelled")
        del raw
        self.assertEqual(held, b"\x01\x02")
        self.assertEqual(sum(queue.bytes for queue in queues), 0)
        self.assertTrue(all(queue.protocol is None for queue in queues))
        # Delivered views intentionally own their bytes outside queue accounting.
        with self.assertRaises(QueueClosed):
            await queues[1].receive()

    async def test_queue_validation_uses_views_and_preserves_terminal_wakeup(self):
        queue = MediaQueue(100000, 1)
        queue.offer(encode_frame(hello()))
        raw = encode_frame(stream())
        queue.offer(raw)
        self.assertIs(queue.protocol.streams[0]["extradata"].obj, raw)
        await queue.receive()
        await queue.receive()
        waiter = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        with self.assertRaises(ValueError):
            queue.offer(encode_frame({**packet(), "generation": 2}))
        with self.assertRaises(QueueClosed):
            await asyncio.wait_for(waiter, 0.5)
        self.assertIsNone(queue.protocol)
