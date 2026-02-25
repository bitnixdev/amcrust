# HomeKit Data Stream (HDS) Wire Format Reference

Extracted from HAP-NodeJS (`homebridge/HAP-NodeJS`, branch `latest`) and the
community secure-video-specification. This is the implementation reference for
amcrust's HSV support (`src/hsv/`).

## 1. SetupDataStreamTransport characteristic

- UUID `00000131-0000-1000-8000-0026BB765291`, TLV8, perms PAIRED_READ, PAIRED_WRITE, WRITE_RESPONSE.
- Lives on service **DataStreamTransportManagement** (`00000129-…`) together with the
  **Version** characteristic (`00000037-…`, string) whose value must be `"1.0"`.

Write request TLV8:

| TLV type | Name                 | Value                                             |
| -------- | -------------------- | ------------------------------------------------- |
| 1        | SESSION_COMMAND_TYPE | 1 byte; `0` = START_SESSION (only accepted value) |
| 2        | TRANSPORT_TYPE       | 1 byte; `0` = HOMEKIT_DATA_STREAM                 |
| 3        | CONTROLLER_KEY_SALT  | exactly 32 bytes                                  |

Write-response TLV8 (order: 1, 2, 3):

| TLV type | Name                              | Value                                                |
| -------- | --------------------------------- | ---------------------------------------------------- |
| 1        | STATUS                            | 1 byte; 0=SUCCESS, 1=GENERIC_ERROR, 2=BUSY           |
| 2        | TRANSPORT_TYPE_SESSION_PARAMETERS | nested TLV: inner 1 = TCP_LISTENING_PORT, u16 **LE** |
| 3        | ACCESSORY_KEY_SALT                | 32 random bytes                                      |

Write-response mechanics: response TLV8 is base64-encoded as the characteristic's
write-response value; HAP layer replies **207 Multi-Status** with
`{"characteristics":[{"aid":…,"iid":…,"status":0,"value":"<base64>"}]}` when the
write had `"r": true`. A subsequent read returns the same TLV **without** the
accessory key salt (types 1 and 2 only).

## 2. SupportedDataStreamTransportConfiguration

UUID `00000130-…`, TLV8, PAIRED_READ. One outer TLV type 1 per transport, each
containing inner TLV 1 = transport type (0 = HDS).
Default bytes: `01 03 01 01 00` (base64 `AQMBAQA=`).

## 3. HDS session key derivation

- HKDF-**SHA-512** (RFC 5869).
- IKM: the raw 32-byte **X25519 shared secret from HAP Pair Verify** of the HAP
  session that wrote SetupDataStreamTransport (not the derived session keys).
- Salt: `controllerKeySalt || accessoryKeySalt` (controller first, 64 bytes).
- Info: accessory→controller `"HDS-Read-Encryption-Key"`,
  controller→accessory `"HDS-Write-Encryption-Key"`.
- Key length: 32 bytes each.

## 4. HDS TCP frame format

```
byte 0        : payloadType, always 0x01 (others ignored)
bytes 1..3    : payload length, uint24 BIG-endian (ciphertext length)
bytes 4..4+L  : ciphertext
last 16 bytes : Poly1305 tag
```

- AAD = the 4-byte header.
- Cipher: ChaCha20-Poly1305 **IETF** (96-bit nonce).
- Nonce: `00 00 00 00 || counter_u64_LE`; counter starts at 0; independent
  counter per direction; sender post-increments; receiver increments **only
  after successful decryption** (important for trial decryption).
- Max payload: 0xFFFFF (1,048,575) bytes despite 24-bit field; violation closes
  the connection.
- Frames may be split/coalesced across TCP segments — buffer partial frames.

## 5. Payload plaintext layout

```
byte 0       : headerLength (u8)
bytes 1..1+h : header dictionary (DataStream binary encoding)
rest         : message dictionary
```

## 6. DataStream binary encoding

All multi-byte values little-endian unless noted.

| Tag                 | Meaning                                                       | Payload                          |
| ------------------- | ------------------------------------------------------------- | -------------------------------- |
| 0x00                | invalid                                                       | decode error                     |
| 0x01 / 0x02         | true / false                                                  | none                             |
| 0x03                | terminator                                                    | none                             |
| 0x04                | null                                                          | none                             |
| 0x05                | UUID                                                          | 16 bytes big-endian              |
| 0x06                | date                                                          | f64 LE, seconds since 2001-01-01 |
| 0x07                | integer −1                                                    | none                             |
| 0x08–0x2E           | small int 0–39                                                | none (tag − 0x08)                |
| 0x30/0x31/0x32/0x33 | int8/int16/int32/int64 (signed LE)                            | 1/2/4/8 bytes                    |
| 0x35 / 0x36         | f32 / f64 LE                                                  | 4 / 8 bytes                      |
| 0x40–0x60           | UTF-8 short (len = tag−0x40, 0–32)                            | bytes                            |
| 0x61/0x62/0x63/0x64 | UTF-8 len8/len16/len32/len64                                  | len then bytes                   |
| 0x6F                | UTF-8 NUL-terminated                                          | bytes until 0x00                 |
| 0x70–0x90           | data short (len = tag−0x70, 0–32)                             | bytes                            |
| 0x91/0x92/0x93/0x94 | data len8/len16/len32/len64                                   | len then bytes                   |
| 0x9F                | data terminated by 0x03                                       | bytes                            |
| 0xA0–0xCF           | back-reference to (tag−0xA0)-th previously-decoded leaf value | none                             |
| 0xD0–0xDE           | array short (count = tag−0xD0)                                | elements                         |
| 0xDF                | array terminated                                              | elements until 0x03              |
| 0xE0–0xEE           | dict short (count pairs)                                      | key/value pairs                  |
| 0xEF                | dict terminated                                               | pairs until 0x03 in key position |

Notes:

- Writer int-width choice: −1 → 0x07; 0..39 → small int; else smallest of
  int8/int16/int32; larger → int64. Non-integers → f64.
- HAP-NodeJS's "int64" write only stores low 32 bits + 4 zero bytes; ids are
  capped below 2^32. Encode header `id`/`status` as tag 0x33 to byte-match.
- Writer thresholds: arrays ≤12 short form; dicts ≤14 short form.
- **Back-references (compression)**: the reader must track every decoded _leaf_
  value in decode order (scalars incl. true/false/−1/small ints, strings, data,
  UUID, date — not containers) and resolve 0xA0+n to the n-th tracked value
  (n ≤ 47). HAP-NodeJS's writer emits these for repeated values (e.g. the
  string "dataSend"). Our writer may simply never emit them.
- HAP-NodeJS has a decode bug for short-form data (0x70–0x90 decodes as
  undefined) — avoid sending short-form data to it; decode correctly ourselves.

## 7. Control protocol handshake

- First message on a connection MUST be protocol `"control"`, request `"hello"`
  within 10 s of socket open, else close.
- Request header: `{"protocol": "control", "request": "hello", "id": <int>}`, message `{}`.
- Response header: `{"protocol": "control", "response": "hello", "id": <same>,
"status": 0}`, message `{}` (id/status written as tag 0x33).
- Header vocabulary: `protocol`, `event`, `request`, `response`, `id`, `status`.
  Presence of `event`/`request`/`response` determines the message kind.

## 8. dataSend protocol

Topics: `open` (request), `data` (event), `ack` (event), `close` (event).

**open** request message: `{"streamId": <int>, "type": "ipcamera.recording",
"target": "controller", "reason": <string?>}`.
Success response: header status 0, message `{"status": 0}`. Rejection: header
status 6 (protocol-specific error), message `{"status": <reason>}` — 1
NOT_ALLOWED (recording disabled), 2 BUSY (stream already running), 9
INVALID_CONFIGURATION (no selected config), 5 UNEXPECTED_FAILURE.

**data** event:

```
header:  {"protocol": "dataSend", "event": "data"}
message: {"streamId": <int>, "packets": [ {
  "data": <bytes ≤ 0x40000>,
  "metadata": {
    "dataType": "mediaInitialization" | "mediaFragment",
    "dataSequenceNumber": <int, 1 = init, 2 = first fragment>,
    "dataChunkSequenceNumber": <int, starts at 1 per fragment>,
    "isLastDataChunk": <bool>,
    "dataTotalSize": <int>          // only on chunk 1 of each fragment
  } } ],
  "endOfStream": <bool>             // only on a fragment's last chunk; true iff final fragment
}
```

**ack** (controller→accessory after endOfStream): `{"streamId":…, "endOfStream": <bool>}`;
controller closes the HDS connection ~5 s later.

**close** event: `{"streamId": <int>, "reason": <int>}` (HAP-NodeJS uses key
`reason` both directions). Reasons: 0 NORMAL, 1 NOT_ALLOWED, 2 BUSY, 3
CANCELLED, 4 UNSUPPORTED, 5 UNEXPECTED_FAILURE, 6 TIMEOUT, 7 BAD_DATA, 8
PROTOCOL_ERROR, 9 INVALID_CONFIGURATION.

Header-level status codes: 0 SUCCESS … 6 PROTOCOL_SPECIFIC_ERROR.

## 9. RecordingManagement TLVs

Characteristics: SupportedCameraRecordingConfiguration `00000205`,
SupportedVideoRecordingConfiguration `00000206`,
SupportedAudioRecordingConfiguration `00000207`,
SelectedCameraRecordingConfiguration `00000209` (write admin-only).
Services: CameraRecordingManagement `00000204`, CameraOperatingMode `0000021A`.

**SupportedCameraRecordingConfiguration**: 1 = prebuffer ms (i32 LE, ≥4000);
2 = event trigger options (8-byte field, i32 LE bitmask in first 4 bytes:
0x01 motion, 0x02 doorbell); 3 = media container configs (0x00-delimited list):
inner 1 = container type (0 = fMP4), inner 2 = params → 1 = fragment length ms (i32 LE, typ. 4000).

**SupportedVideoRecordingConfiguration**: 1 = codec config: 1 = codec (0 =
H.264; H.265=1 reserved/unsupported by controllers), 2 = params (1 = profiles
0/1/2, 2 = levels 3.1→0 3.2→1 4.0→2, delimited lists), 3 = attributes list
(1 = width u16 LE, 2 = height u16 LE, 3 = fps u8).

**SupportedAudioRecordingConfiguration**: 1 = codec config (list): 1 = codec
(0 = AAC-LC, 1 = AAC-ELD), 2 = params: 1 = channels u8, 2 = bitrate mode
(0 variable, 1 constant), 3 = sample rates (u8 each, delimited list:
8k=0 16k=1 24k=2 32k=3 44.1k=4 48k=5).

**SelectedCameraRecordingConfiguration** write: 1 = selected general (prebuffer
i32 LE, trigger i32 LE, media container config); 2 = selected video (codec,
params incl. 3 = bitrate i32 LE and 4 = iFrameInterval i32 LE ms, attributes);
3 = selected audio (channels, bitrate mode, sample rate, 4 = max audio bitrate u32 LE).
Persist the raw value + hash of supported configs; reads before any write →
SERVICE_COMMUNICATION_FAILURE (-70402). Restore only while supported configs unchanged.

fMP4 requirements: first packet = `ftyp`+`moov` init; every fragment =
`moof`+`mdat` starting with a keyframe, duration = selected fragment length;
iFrameInterval == fragment length (typ. 4000 ms); omit audio when
RecordingAudioActive is false. ffmpeg's
`-movflags frag_keyframe+empty_moov+default_base_moof` produces this shape.

## 10. Operational behavior

- One TCP listener per accessory, bound lazily on an **ephemeral port** at the
  first setup write; port reported per-session in the write-response; closed
  when the last connection closes and no sessions are pending.
- Prepared session expires if no TCP connection within **10 s**.
- No identify frame: the **first frame** on a new socket is trial-decrypted
  (nonce 0) with each prepared session's controller→accessory key; first match
  wins; failures don't advance counters; no match → close.
- HDS connection is bound to the HAP connection that set it up: when that HAP
  connection closes, close the HDS connection.
- TCP keepalive + nodelay; no HDS-level keepalive frames.
- Request ids: random `[0, 2^32)`. Response routing by `id` alone.
- Timeouts: hello 10 s; response to accessory-sent request 10 s;
  recording-stream graceful close watchdog ~12 s.
- One recording stream at a time (reject concurrent opens BUSY); multiple HDS
  connections (multiple hubs) supported concurrently.
- Chunk fragments at 256 KiB.
