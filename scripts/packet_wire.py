"""AMC2: bounded JSON metadata plus raw binary byte fields (experimental ABI).

Queue bodies are binary. Binary decoding exposes independent read-only views into
immutable bodies; inspection decoding retains hex for comparison/bitstream tools.
Metadata parsing still allocates; this is not an end-to-end zero-copy claim.
"""
import copy
import json
import struct

MAGIC = b"AMC2"
MAX_METADATA = 64 * 1024
MAX_BYTES = 4 * 1024 * 1024
MAX_SIDE = 1024 * 1024
MAX_BLOB = MAX_BYTES + MAX_SIDE
HEADER = struct.Struct("!4sII")
MAX_WIRE = HEADER.size + MAX_METADATA + MAX_BLOB


def byte_fields(obj):
    kind = obj.get("type")
    if kind not in ("hello", "stream", "packet", "end"):
        raise ValueError("unknown binary frame kind")
    if kind in ("stream", "packet"):
        yield obj, "extradata" if kind == "stream" else "data", MAX_BYTES
        side = obj["side_data"]
        if not isinstance(side, list) or len(side) > 64:
            raise ValueError("side data count exceeded")
        for entry in side:
            yield entry, "data", MAX_SIDE


def check_depth(value, depth=0):
    if depth >= 32:
        raise ValueError("metadata nesting limit")
    if isinstance(value, dict):
        for item in value.values():
            check_depth(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            check_depth(item, depth + 1)


def encode_frame(obj):
    """Return a body; transport adds the big-endian u32 body length."""
    try:
        obj = copy.deepcopy(obj)
        check_depth(obj)
        blob = bytearray()
        side_total = 0
        for index, (owner, key, maximum) in enumerate(byte_fields(obj)):
            value = owner[key]
            if (not isinstance(value, str) or len(value) > maximum * 2 or len(value) % 2
                    or any(c not in "0123456789abcdef" for c in value)):
                raise ValueError("invalid byte field")
            data = bytes.fromhex(value)
            if index:
                side_total += len(data)
                if side_total > MAX_SIDE:
                    raise ValueError("side data limit exceeded")
            owner[key] = {"offset": len(blob), "length": len(data)}
            blob.extend(data)
        metadata = json.dumps(obj, separators=(",", ":"), allow_nan=False).encode()
        if not 0 < len(metadata) <= MAX_METADATA or len(blob) > MAX_BLOB:
            raise ValueError("binary frame limit exceeded")
        return HEADER.pack(MAGIC, len(metadata), len(blob)) + metadata + blob
    except (KeyError, TypeError, AttributeError, RecursionError) as error:
        raise ValueError("invalid binary schema") from error


def decode_frame(raw, *, binary=False):
    try:
        if not isinstance(raw, bytes) or not HEADER.size < len(raw) <= MAX_WIRE:
            raise ValueError("binary frame limit exceeded")
        magic, meta_size, blob_size = HEADER.unpack_from(raw)
        if (magic != MAGIC or not 0 < meta_size <= MAX_METADATA or blob_size > MAX_BLOB
                or HEADER.size + meta_size + blob_size != len(raw)):
            raise ValueError("invalid binary envelope")
        def reject_constant(value):
            raise ValueError("non-finite metadata number")
        obj = json.loads(raw[HEADER.size:HEADER.size + meta_size].decode("utf-8"),
                         parse_constant=reject_constant)
        if not isinstance(obj, dict):
            raise ValueError("metadata object required")
        check_depth(obj)
        blob = memoryview(raw)[HEADER.size + meta_size:]
        used, side_total = 0, 0
        for index, (owner, key, maximum) in enumerate(byte_fields(obj)):
            ref = owner[key]
            if (not isinstance(ref, dict) or set(ref) != {"offset", "length"}
                    or type(ref["offset"]) is not int or type(ref["length"]) is not int
                    or ref["offset"] != used or not 0 <= ref["length"] <= maximum
                    or ref["length"] > blob_size - used):
                raise ValueError("invalid binary byte reference")
            size = ref["length"]
            if index:
                side_total += size
                if side_total > MAX_SIDE:
                    raise ValueError("side data limit exceeded")
            view = blob[used:used + size]
            owner[key] = view if binary else view.hex()
            used += size
        if used != blob_size:
            raise ValueError("unreferenced binary bytes")
        return obj
    except (KeyError, TypeError, AttributeError, UnicodeError, RecursionError) as error:
        raise ValueError("invalid binary schema") from error
