"""Small deterministic loopback RTSP camera fixture, not a general RTSP server.

Digest authentication, H.264 FU-A and MPEG4-GENERIC AAC over interleaved TCP.
Only synthetic credentials/media; no forwarding, external URLs or camera access.
"""
import asyncio
import base64
import hashlib
import math
from pathlib import Path
import re
import struct

from ingest_probe import command


REALM = "amcrust-fixture"
NONCE = "synthetic-fixed-nonce"
CLOCK_FAULTS = ("clock-jump", "reports-lost", "video-reports-lost", "clock-silent", "report-once-lost",
                "late-reports", "audio-report-late")


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


async def make_media(directory, width, height, profile=None, duration=4, aac_pns=None):
    if type(duration) is not int or not 1 <= duration <= 30:
        raise ValueError("unsupported fixture duration")
    if aac_pns is not None and type(aac_pns) is not bool:
        raise ValueError("unsupported AAC PNS setting")
    if profile not in (None, "main", "high"):
        raise ValueError("unsupported fixture profile")
    video = Path(directory) / f"{width}.h264"
    audio = Path(directory) / f"{width}.aac"
    await command("ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
                  f"testsrc2=size={width}x{height}:rate=15", "-t", str(duration), "-c:v", "libx264",
                  "-preset", "veryfast" if profile else "ultrafast", "-tune", "zerolatency", "-g", "15", "-bf", "0",
                  *(["-profile:v", profile] if profile else []),
                  "-x264-params", "aud=1:repeat-headers=1:scenecut=0", "-f", "h264", str(video))
    await command("ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
                  "sine=frequency=1000:sample_rate=48000", "-t", str(duration), "-c:a", "aac",
                  "-ac", "1", *(["-aac_pns", str(int(aac_pns))] if aac_pns is not None else []),
                  "-f", "adts", str(audio))
    return access_units(video.read_bytes()), aac_frames(audio.read_bytes())


def sender_report(track, origin, elapsed, packets, octets):
    """Compound SR + SDES, with a shared synthetic NTP epoch and CNAME."""
    rate = (90000, 48000)[track]
    ssrc = 0x10203040 + track
    ntp = (3900000000 << 32) + int(elapsed * (1 << 32))
    timestamp = (origin + int(elapsed * rate)) & 0xFFFFFFFF
    sr = struct.pack("!BBHIQIII", 0x80, 200, 6, ssrc, ntp, timestamp, packets, octets)
    cname = b"shared-fixture-clock"
    chunk = struct.pack("!I", ssrc) + bytes([1, len(cname)]) + cname + b"\0"
    chunk += bytes((-len(chunk)) % 4)
    return sr + struct.pack("!BBH", 0x81, 202, len(chunk) // 4) + chunk


class CameraFixture:
    def __init__(self, media, mode="disconnect", *, clocked=False, audio_offset=0,
                 staggered_reports=False, replacement_sps=None, send_seconds=2.2):
        self.video, self.audio = media
        if type(send_seconds) not in (int, float) or not 0 < send_seconds <= 30:
            raise ValueError("unsupported fixture send duration")
        self.send_seconds = send_seconds
        if mode not in ("disconnect", "stall", "silent", "rollback", "reject", "parameter-change") + CLOCK_FAULTS:
            raise ValueError("unknown fixture mode")
        self.mode = mode
        if (mode == "parameter-change") != (replacement_sps is not None):
            raise ValueError("parameter-change mode requires replacement SPS")
        if replacement_sps is not None and (not isinstance(replacement_sps, bytes) or
                not 1 <= len(replacement_sps) <= 4096 or replacement_sps[0] & 31 != 7):
            raise ValueError("invalid replacement SPS")
        self.replacement_sps = replacement_sps
        if audio_offset < 0 or audio_offset > 0.5 or (audio_offset and not clocked):
            raise ValueError("unsupported fixture audio offset")
        if mode in CLOCK_FAULTS and not clocked:
            raise ValueError("clock fault requires sender reports")
        if staggered_reports and (not clocked or not audio_offset):
            raise ValueError("staggered reports require delayed clocked audio")
        self.staggered_reports = staggered_reports
        self.clocked = clocked
        self.audio_offset = audio_offset
        self.origins = (0xFFFF0000, 0xABCDEF00) if clocked else (0, 0)
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
        events += [(self.audio_offset + i * 1024 / 48000, 1, i, au) for i, au in enumerate(self.audio)]
        if self.clocked:
            events += [(at / 2, 2, at, None) for at in range(math.ceil(self.send_seconds * 2))]
            if self.staggered_reports:
                events.append((self.audio_offset, 3, 0, None))
        octets = [0, 0]
        sent = [0, 0]  # RTCP counters belong to this connection, not all fixture clients.
        sequence = [65000, 65000]
        started = asyncio.get_running_loop().time()
        # SR precedes media at the same instant, including both initial reports.
        for at, track, index, au in sorted(events, key=lambda event: (event[0], -1 if event[1] >= 2 else event[1])):
            # Enough media to discover both tracks, join late, and reach a new IDR.
            if at >= self.send_seconds or (self.mode == "clock-silent" and at >= 0.5):
                break
            await asyncio.sleep(max(0, started + at - asyncio.get_running_loop().time()))
            if track >= 2:
                for report_track in ((1,) if track == 3 else (0, 1)):
                    if ((self.mode == "late-reports" and at < 1) or
                            (self.mode == "audio-report-late" and report_track == 1 and at < 1) or
                            (self.mode == "reports-lost" and at > 0) or
                            (self.mode == "video-reports-lost" and report_track == 0 and at > 0) or
                            (self.mode == "report-once-lost" and at == 0.5)):
                        continue
                    if self.staggered_reports and report_track == 1 and at < self.audio_offset:
                        continue
                    origin = self.origins[report_track]
                    if self.mode == "clock-jump" and report_track == 0 and at >= 1:
                        origin += 18000
                    report = sender_report(report_track, origin, at,
                                           sent[report_track], octets[report_track])
                    writer.write(b"$" + bytes([channels[report_track] + 1]) + struct.pack("!H", len(report)) + report)
                await writer.drain()
                continue
            if track == 0:
                timestamp = self.origins[0] + index * 6000
                if self.mode == "rollback" and index >= 20:
                    timestamp -= 60000
                if self.mode == "parameter-change" and index >= 15:
                    au = [self.replacement_sps if nal[0] & 31 == 7 else nal for nal in au]
                payloads = [part for nal in au for part in fragments(nal)]
            else:
                timestamp = self.origins[1] + int(self.audio_offset * 48000) + index * 1024
                payloads = [struct.pack("!HH", 16, len(au) << 3) + au]
            for i, payload in enumerate(payloads):
                rtp = struct.pack("!BBHII", 0x80, (0x80 if i == len(payloads) - 1 else 0) | (96 + track),
                                  sequence[track], timestamp & 0xFFFFFFFF, 0x10203040 + track) + payload
                sequence[track] = (sequence[track] + 1) & 0xFFFF
                writer.write(b"$" + bytes([channels[track]]) + struct.pack("!H", len(rtp)) + rtp)
                self.sent[track] += 1
                sent[track] += 1
                octets[track] += len(payload)
            await writer.drain()
        if self.mode in ("disconnect", "rollback"):
            writer.close()
        # stall deliberately leaves the RTSP socket alive but sends no more media.
