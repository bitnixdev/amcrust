/* Local-file / loopback-fixture packet IPC experiment. Not production ingest.
 * AMC2 bounded metadata + raw byte envelope; see packet_wire.h.
 * Integers retain their native precision; AV_NOPTS_VALUE is JSON null.
 */
#include <libavformat/avformat.h>
#include <libavcodec/avcodec.h>
#include <libavutil/log.h>
#include <libavutil/mem.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>

#include "packet_wire.h"
#define MAX_FRAME WIRE_METADATA
#define MAX_PAYLOAD (4U * 1024U * 1024U)
#define MAX_SIDE_TOTAL (1U * 1024U * 1024U)
#define MAX_STREAMS 8
static char *frame;
static size_t used, blob_used, blob_count;
static const uint8_t *blob_parts[65];
static size_t blob_sizes[65];
static unsigned generation = 1;

static void fail(const char *message) {
    fprintf(stderr, "packet worker: %s\n", message);
    exit(1);
}

static void append(const char *format, ...) {
    va_list args;
    va_start(args, format);
    int count = vsnprintf(frame + used, MAX_FRAME + 1 - used, format, args);
    va_end(args);
    if (count < 0 || (size_t)count > MAX_FRAME - used)
        fail("frame limit exceeded");
    used += (size_t)count;
}

static void blob(const uint8_t *data, size_t size) {
    if (size > MAX_PAYLOAD || blob_count >= 65 || size > WIRE_BLOB - blob_used)
        fail("byte field limit exceeded");
    append("{\"offset\":%zu,\"length\":%zu}", blob_used, size);
    blob_parts[blob_count] = data;
    blob_sizes[blob_count++] = size;
    blob_used += size;
}

static void put32(uint8_t *p, uint32_t n) {
    p[0] = n >> 24; p[1] = n >> 16; p[2] = n >> 8; p[3] = n;
}

static void timestamp(int64_t value) {
    if (value == AV_NOPTS_VALUE) append("null");
    else append("%lld", (long long)value);
}

static void emit(void) {
    uint8_t header[4 + WIRE_HEADER];
    put32(header, WIRE_HEADER + used + blob_used);
    memcpy(header + 4, WIRE_MAGIC, 4);
    put32(header + 8, used);
    put32(header + 12, blob_used);
    if (fwrite(header, 1, sizeof(header), stdout) != sizeof(header) ||
        fwrite(frame, 1, used, stdout) != used) fail("output closed");
    for (size_t i = 0; i < blob_count; i++)
        if (blob_sizes[i] && fwrite(blob_parts[i], 1, blob_sizes[i], stdout) != blob_sizes[i])
            fail("output closed");
    if (fflush(stdout)) fail("output closed");
    used = blob_used = blob_count = 0;
}

static void side_data(const AVPacketSideData *data, int count) {
    size_t total = 0;
    if (count < 0 || count > 64) fail("side data count exceeded");
    append("[");
    for (int i = 0; i < count; i++) {
        if (data[i].size > MAX_SIDE_TOTAL - total) fail("side data limit exceeded");
        total += data[i].size;
        append("%s{\"type\":%d,\"data\":", i ? "," : "", data[i].type);
        blob(data[i].data, data[i].size);
        append("}");
    }
    append("]");
}

static unsigned parse_generation(const char *value) {
    char *end;
    unsigned long epoch = strtoul(value, &end, 10);
    if (!*value || *end || epoch < 1 || epoch > 0x7fffffffUL) fail("invalid generation");
    return (unsigned)epoch;
}

static void resource_report(void) {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage)) {
        fputs("AMC_RESOURCE_ERROR\n", stderr);
        return;
    }
    double user = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1000000.0;
    double system = usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1000000.0;
#ifdef __APPLE__
    unsigned long long peak_bytes = (unsigned long long)usage.ru_maxrss;
#else
    unsigned long long peak_bytes = (unsigned long long)usage.ru_maxrss * 1024ULL;
#endif
    fprintf(stderr, "AMC_RESOURCE {\"user_cpu_seconds\":%.9f,\"system_cpu_seconds\":%.9f,"
                    "\"peak_rss_bytes\":%llu}\n", user, system, peak_bytes);
}

int main(int argc, char **argv) {
    int resource_fixture = argc == 4 && !strcmp(argv[1], "--rtsp-resource-fixture");
    int file_generation = argc == 4 && !strcmp(argv[1], "--file-generation");
    int rtsp_fixture = resource_fixture || (argc == 4 && !strcmp(argv[1], "--rtsp-fixture"));
    /* Historical fixture exporter only; production acquisition is Rust-owned. */
    if (argc != 2 && !rtsp_fixture && !file_generation)
        fail("usage: ingest-worker-probe LOCAL_FILE | --rtsp-fixture PORT GENERATION | --rtsp-resource-fixture PORT GENERATION | --file-generation GENERATION LOCAL_FILE");
    char fixture_url[128];
    const char *source = argv[1];
    if (rtsp_fixture) {
        char *end;
        unsigned long port = strtoul(argv[2], &end, 10);
        if (!*argv[2] || *end || port < 1 || port > 65535) fail("invalid fixture port");
        generation = parse_generation(argv[3]);
        if (resource_fixture && atexit(resource_report)) fail("cannot register resource report");
        /* Only synthetic fixture credentials and a numeric loopback port. */
        snprintf(fixture_url, sizeof(fixture_url),
                 "rtsp://fixture:fixture@127.0.0.1:%lu/camera", port);
        source = fixture_url;
    } else if (file_generation) {
        generation = parse_generation(argv[2]);
        source = argv[3];
    }
    av_log_set_level(AV_LOG_QUIET);
    frame = av_malloc(MAX_FRAME + 1);
    if (!frame) fail("allocation failed");
    AVFormatContext *input = NULL;
    AVDictionary *options = NULL;
    /* Network acquisition is opt-in and restricted to our loopback fixture URL. */
    av_dict_set(&options, "protocol_whitelist", rtsp_fixture ? "tcp,rtp" : "file", 0);
    if (rtsp_fixture) {
        av_dict_set(&options, "rtsp_transport", "tcp", 0);
        av_dict_set(&options, "timeout", "2000000", 0);
        av_dict_set(&options, "analyzeduration", "200000", 0);
        av_dict_set(&options, "probesize", "65536", 0);
        av_dict_set(&options, "fpsprobesize", "2", 0);
    }
    int result = avformat_open_input(&input, source,
                                    rtsp_fixture ? av_find_input_format("rtsp") : NULL, &options);
    av_dict_free(&options);
    if (result < 0) fail("cannot open local input");
    if (avformat_find_stream_info(input, NULL) < 0) fail("cannot read stream info");
    if (!input->nb_streams || input->nb_streams > MAX_STREAMS) fail("unsupported stream count");

    append("{\"type\":\"hello\",\"version\":1,\"generation\":%u,\"streams\":%u,\"avcodec_version\":%u,\"skip_samples_type\":%d,\"start_time_realtime\":",
           generation, input->nb_streams, avcodec_version(), AV_PKT_DATA_SKIP_SAMPLES);
    timestamp(input->start_time_realtime);
    append("}");
    emit();
    for (unsigned i = 0; i < input->nb_streams; i++) {
        AVStream *stream = input->streams[i];
        AVCodecParameters *p = stream->codecpar;
        if (p->codec_type != AVMEDIA_TYPE_VIDEO && p->codec_type != AVMEDIA_TYPE_AUDIO)
            fail("only audio/video supported");
        append("{\"type\":\"stream\",\"generation\":%u,\"index\":%u,\"codec\":\"%s\","
               "\"media\":\"%s\",\"time_base\":[%d,%d],\"profile\":%d,\"level\":%d,"
               "\"width\":%d,\"height\":%d,\"sample_rate\":%d,\"channels\":%d,"
               "\"initial_padding\":%d,\"trailing_padding\":%d,\"seek_preroll\":%d,\"extradata\":",
               generation, i, avcodec_get_name(p->codec_id), av_get_media_type_string(p->codec_type),
               stream->time_base.num, stream->time_base.den, p->profile, p->level,
               p->width, p->height, p->sample_rate, p->ch_layout.nb_channels,
               p->initial_padding, p->trailing_padding, p->seek_preroll);
        if (p->extradata_size < 0) fail("invalid codec configuration");
        blob(p->extradata, p->extradata_size);
        char layout[256];
        if (p->codec_type == AVMEDIA_TYPE_AUDIO) {
            int length = av_channel_layout_describe(&p->ch_layout, layout, sizeof(layout));
            if (length < 0 || length >= (int)sizeof(layout)) fail("unsupported channel layout");
        } else layout[0] = 0;
        /* FFmpeg's named layouts contain no JSON control/quote characters. Refuse
         * unexpected custom names instead of emitting an ambiguous descriptor. */
        for (const char *c = layout; *c; c++)
            if ((unsigned char)*c < 32 || *c == '"' || *c == '\\') fail("unsupported layout name");
        append(",\"format\":%d,\"frame_size\":%d,\"channel_layout\":\"%s\",\"video_delay\":%d,"
               "\"sample_aspect_ratio\":[%d,%d],\"color_range\":%d,\"color_primaries\":%d,"
               "\"color_trc\":%d,\"color_space\":%d,\"chroma_location\":%d,\"side_data\":",
               p->format, p->frame_size, layout, p->video_delay, p->sample_aspect_ratio.num, p->sample_aspect_ratio.den,
               p->color_range, p->color_primaries, p->color_trc, p->color_space, p->chroma_location);
        side_data(p->coded_side_data, p->nb_coded_side_data);
        append("}");
        emit();
    }
    AVPacket *packet = av_packet_alloc();
    if (!packet) fail("packet allocation failed");
    uint64_t sequence = 0;
    while ((result = av_read_frame(input, packet)) >= 0) {
        if (packet->size < 0 || packet->size > (int)MAX_PAYLOAD)
            fail("packet limit exceeded");
        append("{\"type\":\"packet\",\"generation\":%u,\"sequence\":%llu,\"stream\":%d,\"pts\":",
               generation, (unsigned long long)sequence++, packet->stream_index);
        timestamp(packet->pts);
        append(",\"dts\":");
        timestamp(packet->dts);
        append(",\"duration\":%lld,\"flags\":%d,\"data\":",
               (long long)packet->duration, packet->flags);
        blob(packet->data, packet->size);
        append(",\"side_data\":");
        side_data(packet->side_data, packet->side_data_elems);
        size_t sr_size = 0;
        const uint8_t *sr_data = av_packet_get_side_data(packet, AV_PKT_DATA_RTCP_SR, &sr_size);
        if (sr_data) {
            if (sr_size != sizeof(AVRTCPSenderReport)) fail("invalid sender report side data");
            AVRTCPSenderReport sr;
            memcpy(&sr, sr_data, sizeof(sr));
            append(",\"rtcp_sr\":{\"ssrc\":%u,\"ntp\":%llu,\"rtp\":%u}",
                   sr.ssrc, (unsigned long long)sr.ntp_timestamp, sr.rtp_timestamp);
        }
        append("}");
        emit();
        av_packet_unref(packet);
    }
    /* A live source ending is a discontinuity, never successful finite media. */
    if (rtsp_fixture || result != AVERROR_EOF) fail("input read failed");
    append("{\"type\":\"end\",\"generation\":%u,\"packets\":%llu}", generation, (unsigned long long)sequence);
    emit();
    av_packet_free(&packet);
    avformat_close_input(&input);
    av_free(frame);
    return 0;
}
