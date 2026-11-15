"""Finite mux controls for captured history/live deliveries; corpus is the oracle only."""
from fractions import Fraction
import hashlib

from av_output_probe import (compare_timing, decoded_audio, decoded_video, mux,
                             pack_wire, pcm_difference)
from ingest_probe import inspect
from packet_wire import decode_frame


async def output_windows(sink, directory, generation, frames, first_sequence, plans,
                         units, continuous, full_info, reference_video, pns, delivered):
    source = [obj for obj in frames if obj["type"] == "packet"]
    all_audio = [obj for obj in source if obj["stream"] == 1]
    boundary = Fraction(source[first_sequence]["pts"], 90000)
    reports = []
    for preceding, records in plans.items():
        if not records:
            raise ValueError("window lacks IDR coverage")
        # Use the finite corpus only to verify actual leased history/live input.
        chosen = {record.sequence for record in records}
        for record in records:
            obj = source[record.sequence]
            if (obj["stream"] != 1 or record.sequence >= first_sequence or
                    Fraction(obj["pts"], 48000) != record.start or
                    Fraction(obj["pts"] + obj["duration"], 48000) != record.end or
                    hashlib.sha256(bytes.fromhex(obj["data"])).hexdigest() != record.sha256):
                raise ValueError("window plan differs from captured AAC oracle")
        selected = [obj for obj in source if obj["sequence"] in chosen or obj["sequence"] >= first_sequence]
        audio = [obj for obj in selected if obj["stream"] == 1]
        audio_start = next(i for i, obj in enumerate(all_audio) if obj["sequence"] == audio[0]["sequence"])
        if audio != all_audio[audio_start:] or not records[preceding].start <= boundary < records[preceding].end:
            raise ValueError("window did not yield contiguous AAC covering IDR")
        received = [decode_frame(raw) for raw in delivered[preceding]["frames"]]
        headers = delivered[preceding]["headers"]
        if (received != selected + [frames[-1]] or headers !=
                [obj for obj in frames if obj["type"] in ("hello", "stream")]):
            raise ValueError("leased history/live handoff differs from original source selection")
        private = list(headers)
        private += [{**obj, "sequence": i} for i, obj in enumerate(received[:-1])]
        private.append({**received[-1], "packets": len(received) - 1})
        path = directory / f"window-{generation}-{preceding}.mp4"
        await mux(sink, path, pack_wire(private))
        info = await inspect(path)
        expected = [obj for obj in full_info["packets"] if obj["stream_index"] == 0][15:]
        expected += [obj for obj in full_info["packets"] if obj["stream_index"] == 1][audio_start:]
        if compare_timing({"streams": full_info["streams"], "packets": expected}, info) != 0:
            raise ValueError("window mux changed exact source clocks or packet payloads")
        if await decoded_video(path) != reference_video[15:30]:
            raise ValueError("window mux changed decoded video")
        direct = directory / f"window-{generation}-{preceding}.aac"
        direct.write_bytes(b"".join(units[audio_start:len(all_audio)]))
        actual = await decoded_audio(path)
        if actual != await decoded_audio(direct):
            raise ValueError("window PCM differs from direct unchanged ADTS control")
        reference = continuous[audio_start * 2048:]
        full_difference = pcm_difference(reference, actual)
        lead = boundary - records[0].start
        samples = lead * 48000
        if samples.denominator != 1 or samples < 0:
            raise ValueError("fixture video boundary is not an integral audio sample")
        # Analysis only: these leading samples remain in the actual output file.
        target_difference = pcm_difference(reference[int(samples) * 2:], actual[int(samples) * 2:])
        if not full_difference["different_samples"]:
            raise ValueError("fresh window unexpectedly matches continuous full-history PCM")
        if preceding == 1:
            if pns is False and target_difference["different_samples"]:
                raise ValueError("PNS-off preceding-AU target is no longer exact")
            if pns is None and not target_difference["different_samples"]:
                raise ValueError("default PNS window no longer exposes residual target differences")
        reports.append({"preceding_aus": preceding, "historical_aus": len(records),
                        "historical_source_sequences": [r.sequence for r in records],
                        "first_audio_source_index": audio_start, "audio_packets": len(audio),
                        "audio_leads_video_seconds_exact": str(lead),
                        "leading_audio_samples_retained": int(samples),
                        "full_window_pcm_difference": full_difference,
                        "pcm_difference_from_video_boundary": target_difference,
                        "mux_payloads_clocks_video_exact": True,
                        "fresh_adts_control_exact": True,
                        "actual_output_trimmed_or_padded": False})
    return reports
