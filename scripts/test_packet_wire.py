"""Binary envelope boundary/round-trip tests; native sink shares fault vectors."""
import asyncio
import json
import struct
import unittest

from packet_ingest_probe import Protocol, read_frame
from packet_wire import HEADER, MAGIC, MAX_BYTES, MAX_METADATA, MAX_SIDE, MAX_WIRE, decode_frame, encode_frame
from packet_wire_faults import malformed_bodies
from test_packet_probe import hello, packet, stream


class BinaryWireTests(unittest.IsolatedAsyncioTestCase):
    def test_round_trip_all_kinds_exact_timestamps_and_arbitrary_bytes(self):
        payload = bytes(range(256)).hex()
        for obj in (hello(), stream(), {**packet(), "data": payload,
                    "pts": (1 << 63) - 1, "dts": -(1 << 63) + 1,
                    "side_data": [{"type": 11, "data": "0000ff00"}]},
                    {"type": "end", "generation": 1, "packets": 1}):
            raw = encode_frame(obj)
            self.assertEqual(decode_frame(raw), obj)
            self.assertEqual(raw[:4], MAGIC)
        obj = {**packet(), "data": payload * 100}
        self.assertLess(len(encode_frame(obj)), len(json.dumps(obj).encode()))

    def test_maximum_payload_and_side_data_are_raw_not_hex(self):
        obj = {**packet(), "data": "ff" * MAX_BYTES,
               "side_data": [{"type": 11, "data": "ab" * MAX_SIDE}]}
        raw = encode_frame(obj)
        _, meta_size, blob_size = HEADER.unpack_from(raw)
        self.assertLess(meta_size, MAX_METADATA)
        self.assertEqual(blob_size, MAX_BYTES + MAX_SIDE)
        self.assertEqual(len(raw), HEADER.size + meta_size + blob_size)
        self.assertEqual(decode_frame(raw), obj)

    def test_empty_byte_fields_and_non_mutating_encoder(self):
        obj = {**stream(), "extradata": "", "side_data": [{"type": 0, "data": ""}]}
        self.assertEqual(decode_frame(encode_frame(obj)), obj)
        self.assertEqual(obj["extradata"], "")
        self.assertEqual(obj["side_data"][0]["data"], "")

    def test_shared_malformed_vectors(self):
        for name, raw in malformed_bodies(encode_frame(packet())).items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                decode_frame(raw)

    def test_metadata_caps_schema_and_non_finite_values(self):
        for obj in ({**hello(), "extra": "x" * MAX_METADATA},
                    {**hello(), "extra": float("nan")},
                    {**packet(), "data": "ab cd"}):
            with self.assertRaises(ValueError):
                encode_frame(obj)
        for metadata in (b"[]", b"{", b"\xff", b'{"type":"hello","extra":NaN}'):
            with self.assertRaises(ValueError):
                decode_frame(HEADER.pack(MAGIC, len(metadata), 0) + metadata)

    async def test_every_split_and_coalesced_binary_frames(self):
        raw = encode_frame({**packet(), "data": "00ff00007b7d"})
        wire = struct.pack("!I", len(raw)) + raw
        for split in range(1, len(wire)):
            reader = asyncio.StreamReader()
            task = asyncio.create_task(read_frame(reader))
            reader.feed_data(wire[:split])
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            reader.feed_data(wire[split:] + wire)
            reader.feed_eof()
            self.assertEqual(await task, raw)
            self.assertEqual(await read_frame(reader), raw)
            self.assertIsNone(await read_frame(reader))

    async def test_outer_limit_rejected_before_waiting_for_body(self):
        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack("!I", MAX_WIRE + 1))
        with self.assertRaises(ValueError):
            await asyncio.wait_for(read_frame(reader), 0.2)

    def test_protocol_has_no_legacy_json_fallback(self):
        with self.assertRaises(ValueError):
            Protocol().accept(json.dumps(hello()).encode())
