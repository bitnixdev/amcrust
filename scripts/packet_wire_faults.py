"""Malformed AMC2 bodies shared by Python and native-sink rejection proofs."""
import copy
import json

from packet_wire import HEADER, MAGIC, MAX_BLOB, MAX_BYTES, MAX_METADATA, MAX_SIDE


def malformed_bodies(raw):
    _, metadata_size, blob_size = HEADER.unpack_from(raw)
    metadata = json.loads(raw[HEADER.size:HEADER.size + metadata_size])
    blob = raw[HEADER.size + metadata_size:]

    def body(obj, data=blob):
        encoded = json.dumps(obj, separators=(",", ":")).encode()
        return HEADER.pack(MAGIC, len(encoded), len(data)) + encoded + data

    def ref_change(key, value):
        obj = copy.deepcopy(metadata)
        obj["data"][key] = value
        return body(obj)

    overlap = copy.deepcopy(metadata)
    overlap["side_data"] = [{"type": 0, "data": {"offset": 0, "length": 1}}]
    too_many = copy.deepcopy(metadata)
    too_many["side_data"] = [{"type": 0, "data": {"offset": blob_size, "length": 0}}] * 65
    oversized = copy.deepcopy(metadata)
    oversized["data"] = {"offset": 0, "length": MAX_BYTES + 1}
    oversized["side_data"] = []
    side = copy.deepcopy(metadata)
    side["data"] = {"offset": 0, "length": 1}
    side["side_data"] = [
        {"type": 0, "data": {"offset": 1, "length": MAX_SIDE}},
        {"type": 1, "data": {"offset": MAX_SIDE + 1, "length": 1}}]
    nested = copy.deepcopy(metadata)
    value = None
    for _ in range(40):
        value = [value]
    nested["nested"] = value
    return {
        "legacy-json": b'{"type":"packet"}',
        "magic": b"AMC9" + raw[4:],
        "short-body": b"AMC2",
        "empty-metadata": HEADER.pack(MAGIC, 0, len(blob)) + blob,
        "metadata-limit": HEADER.pack(MAGIC, MAX_METADATA + 1, blob_size) + raw[HEADER.size:],
        "blob-limit": HEADER.pack(MAGIC, metadata_size, MAX_BLOB + 1) + raw[HEADER.size:],
        "length-mismatch": HEADER.pack(MAGIC, metadata_size, blob_size + 1) + raw[HEADER.size:],
        "trailing-body": raw + b"x",
        "unused-bytes": body(metadata, blob + b"x"),
        "offset-gap": ref_change("offset", 1),
        "offset-bool": ref_change("offset", False),
        "length-negative": ref_change("length", -1),
        "length-oob": ref_change("length", blob_size + 1),
        "length-overflow": ref_change("length", 1 << 64),
        "reference-extra-key": ref_change("extra", 0),
        "overlap": body(overlap),
        "side-count": body(too_many),
        "payload-limit": body(oversized, b"x" * (MAX_BYTES + 1)),
        "side-total": body(side, b"x" * (MAX_SIDE + 2)),
        "metadata-depth": body(nested),
    }
