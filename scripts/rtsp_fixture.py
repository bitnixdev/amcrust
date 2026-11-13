"""Small deterministic loopback RTSP camera fixture, not a general RTSP server.

Digest authentication, H.264 FU-A and MPEG4-GENERIC AAC over interleaved TCP.
Only synthetic credentials/media; no forwarding, external URLs or camera access.
"""
import asyncio
import base64
import hashlib
from pathlib import Path
import re
import struct

from ingest_probe import command


REALM = "amcrust-fixture"
NONCE = "synthetic-fixed-nonce"


def nals(data):
    return [part for part in re.split(b"\x00\x00(?:\x00)?\x01", data) if part]


def access_units(data):
    units, current = [], []
    for nal in nals(data):
        if nal[0] & 31 == 9 and current:
            units.append(current)
            current = []
        current.append(nal)
    if current:
        units.append(current)
    if not units or any(not any(n[0] & 31 in (1, 5) for n in unit) for unit in units):
        raise ValueError("fixture requires AUD-delimited video access units")
    return units


def aac_frames(data):
    frames = []
    while data:
        if len(data) < 7 or data[0] != 255 or data[1] & 0xF6 != 0xF0:
            raise ValueError("invalid ADTS fixture")
        length = ((data[3] & 3) << 11) | (data[4] << 3) | (data[5] >> 5)
        header = 7 if data[1] & 1 else 9
        rate_index = (data[2] >> 2) & 15
        channels = ((data[2] & 1) << 2) | (data[3] >> 6)
        if (length <= header or length > len(data) or rate_index != 3 or channels != 1
                or data[2] >> 6 != 1 or data[6] & 3):
            raise ValueError("fixture requires single-block AAC-LC 48 kHz mono")
        frames.append(data[header:length])
        data = data[length:]
    return frames


def fragments(nal, mtu=1000):
    if len(nal) <= mtu:
        return [nal]
    chunks = [nal[i:i + mtu - 2] for i in range(1, len(nal), mtu - 2)]
    return [bytes([(nal[0] & 0xE0) | 28, (nal[0] & 31) |
                   (0x80 if i == 0 else 0) | (0x40 if i == len(chunks) - 1 else 0)]) + chunk
            for i, chunk in enumerate(chunks)]


def authorized(header, method, uri):
    if not header.startswith("Digest "):
        return False
    fields = dict(re.findall(r'(\w+)="([^"]*)"', header))
    if (fields.get("username") != "fixture" or fields.get("realm") != REALM
            or fields.get("nonce") != NONCE or fields.get("uri") != uri):
        return False
    md5 = lambda value: hashlib.md5(value.encode()).hexdigest()
    expected = md5(f"{md5('fixture:' + REALM + ':fixture')}:{NONCE}:{md5(method + ':' + uri)}")
    return fields.get("response") == expected


async def make_media(directory, width, height):
    video = Path(directory) / f"{width}.h264"
    audio = Path(directory) / f"{width}.aac"
    await command("ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
                  f"testsrc2=size={width}x{height}:rate=15", "-t", "4", "-c:v", "libx264",
                  "-preset", "ultrafast", "-tune", "zerolatency", "-g", "15", "-bf", "0",
                  "-x264-params", "aud=1:repeat-headers=1:scenecut=0", "-f", "h264", str(video))
    await command("ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
                  "sine=frequency=1000:sample_rate=48000", "-t", "4", "-c:a", "aac",
                  "-ac", "1", "-f", "adts", str(audio))
    return access_units(video.read_bytes()), aac_frames(audio.read_bytes())


class CameraFixture:
    def __init__(self, media, mode="disconnect"):
        self.video, self.audio = media
        if mode not in ("disconnect", "stall", "silent", "rollback", "reject"):
            raise ValueError("unknown fixture mode")
        self.mode = mode
        self.connections = 0
        self.challenges = 0
        self.authenticated = 0
        self.played = 0
        self.sent = [0, 0]
        self.tasks = set()
        self.errors = []
        self.server = None

    async def __aenter__(self):
        self.server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.errors and args[0] is None:
            raise RuntimeError(f"fixture error: {self.errors[0]}")

    def _accept(self, reader, writer):
        task = asyncio.create_task(self._serve(reader, writer))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    def sdp(self):
        sps = next(n for unit in self.video for n in unit if n[0] & 31 == 7)
        pps = next(n for unit in self.video for n in unit if n[0] & 31 == 8)
        sets = ",".join(base64.b64encode(n).decode() for n in (sps, pps))
        return ("v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=fixture\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n"
                "a=control:*\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\n"
                f"a=fmtp:96 packetization-mode=1;profile-level-id={sps[1:4].hex()};sprop-parameter-sets={sets}\r\n"
                "a=control:trackID=0\r\nm=audio 0 RTP/AVP 97\r\na=rtpmap:97 MPEG4-GENERIC/48000/1\r\n"
                "a=fmtp:97 streamtype=5;profile-level-id=1;mode=AAC-hbr;config=1188;"
                "SizeLength=13;IndexLength=3;IndexDeltaLength=3\r\na=control:trackID=1\r\n").encode()

    async def _serve(self, reader, writer):
        self.connections += 1
        media_task = None
        channels = {}
        try:
            while True:
                first = await reader.readexactly(1)
                if first == b"$":
                    header = await reader.readexactly(3)
                    await reader.readexactly(struct.unpack("!H", header[1:])[0])
                    continue  # Controller RTCP is irrelevant to this fixed sender.
                raw = first + await reader.readuntil(b"\r\n\r\n")
                lines = raw.decode().split("\r\n")
                method, uri, _ = lines[0].split()
                headers = {line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip()
                           for line in lines[1:] if ":" in line}
                if int(headers.get("content-length", "0")):
                    raise ValueError("unexpected request body")
                status, body, extra = "200 OK", b"", {}
                if method != "OPTIONS" and (self.mode == "reject" or not authorized(
                        headers.get("authorization", ""), method, uri)):
                    self.challenges += 1
                    status = "401 Unauthorized"
                    extra["WWW-Authenticate"] = f'Digest realm="{REALM}", nonce="{NONCE}"'
                elif method == "OPTIONS":
                    extra["Public"] = "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN, GET_PARAMETER"
                else:
                    self.authenticated += 1
                    if method == "DESCRIBE":
                        body = self.sdp()
                        extra = {"Content-Type": "application/sdp",
                                 "Content-Base": f"rtsp://127.0.0.1:{self.port}/camera/"}
                    elif method == "SETUP":
                        track = int(uri.rsplit("=", 1)[1])
                        transport = headers.get("transport", "")
                        match = re.search(r"interleaved=(\d+)-(\d+)", transport)
                        if track not in (0, 1) or "RTP/AVP/TCP" not in transport or not match:
                            raise ValueError("expected TCP-interleaved setup")
                        channels[track] = int(match[1])
                        extra = {"Session": "12345678;timeout=60", "Transport": transport}
                    elif method in ("PLAY", "GET_PARAMETER", "TEARDOWN"):
                        extra["Session"] = "12345678"
                    else:
                        raise ValueError("unsupported fixture method")
                response = f"RTSP/1.0 {status}\r\nCSeq: {headers['cseq']}\r\nContent-Length: {len(body)}\r\n"
                response += "".join(f"{key}: {value}\r\n" for key, value in extra.items()) + "\r\n"
                writer.write(response.encode() + body)
                await writer.drain()
                if method == "PLAY" and status == "200 OK":
                    if media_task is not None or set(channels) != {0, 1}:
                        raise ValueError("invalid PLAY lifecycle")
                    self.played += 1
                    media_task = asyncio.create_task(self._send(writer, channels))
                if method == "TEARDOWN":
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.errors.append(str(error))
        finally:
            if media_task:
                media_task.cancel()
                results = await asyncio.gather(media_task, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception) and not isinstance(result, ConnectionError):
                        self.errors.append(str(result))
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def _send(self, writer, channels):
        if self.mode == "silent":
            return
        events = [(i / 15, 0, i, au) for i, au in enumerate(self.video)]
        events += [(i * 1024 / 48000, 1, i, au) for i, au in enumerate(self.audio)]
        sequence = [65000, 65000]
        started = asyncio.get_running_loop().time()
        for at, track, index, au in sorted(events):
            # Enough media to discover both tracks, join late, and reach a new IDR.
            if at >= 2.2:
                break
            await asyncio.sleep(max(0, started + at - asyncio.get_running_loop().time()))
            if track == 0:
                timestamp = index * 6000
                if self.mode == "rollback" and index >= 20:
                    timestamp -= 60000
                payloads = [part for nal in au for part in fragments(nal)]
            else:
                timestamp = index * 1024
                payloads = [struct.pack("!HH", 16, len(au) << 3) + au]
            for i, payload in enumerate(payloads):
                rtp = struct.pack("!BBHII", 0x80, (0x80 if i == len(payloads) - 1 else 0) | (96 + track),
                                  sequence[track], timestamp & 0xFFFFFFFF, 0x10203040 + track) + payload
                sequence[track] = (sequence[track] + 1) & 0xFFFF
                writer.write(b"$" + bytes([channels[track]]) + struct.pack("!H", len(rtp)) + rtp)
                self.sent[track] += 1
            await writer.drain()
        if self.mode in ("disconnect", "rollback"):
            writer.close()
        # stall deliberately leaves the RTSP socket alive but sends no more media.
