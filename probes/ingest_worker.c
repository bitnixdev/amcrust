/* Local-file-only packet IPC experiment. Not a production RTSP worker.
 * Each frame: big-endian u32 JSON byte length, followed by UTF-8 JSON.
 * Integers retain their native precision; AV_NOPTS_VALUE is JSON null.
 * Payload/codec/side-data bytes use hex for an inspectable proof, not efficiency.
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

#define MAX_FRAME (16U * 1024U * 1024U)
#define MAX_PAYLOAD (4U * 1024U * 1024U)
#define MAX_SIDE_TOTAL (1U * 1024U * 1024U)
#define MAX_STREAMS 8
static char *frame;
static size_t used;

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

static void hex(const uint8_t *data, size_t size) {
    static const char digits[] = "0123456789abcdef";
    if (size > MAX_PAYLOAD || size * 2 + 2 > MAX_FRAME - used)
        fail("byte field limit exceeded");
    frame[used++] = '"';
    for (size_t i = 0; i < size; i++) {
        frame[used++] = digits[data[i] >> 4];
        frame[used++] = digits[data[i] & 15];
    }
    frame[used++] = '"';
    frame[used] = 0;
}

static void timestamp(int64_t value) {
    if (value == AV_NOPTS_VALUE) append("null");
    else append("%lld", (long long)value);
}

static void emit(void) {
    uint8_t header[] = {used >> 24, used >> 16, used >> 8, used};
    if (fwrite(header, 1, sizeof(header), stdout) != sizeof(header) ||
        fwrite(frame, 1, used, stdout) != used || fflush(stdout))
        fail("output closed");
    used = 0;
}

static void side_data(const AVPacketSideData *data, int count) {
    size_t total = 0;
    if (count < 0 || count > 64) fail("side data count exceeded");
    append("[");
    for (int i = 0; i < count; i++) {
        if (data[i].size > MAX_SIDE_TOTAL - total) fail("side data limit exceeded");
        total += data[i].size;
        append("%s{\"type\":%d,\"data\":", i ? "," : "", data[i].type);
        hex(data[i].data, data[i].size);
        append("}");
    }
    append("]");
}

int main(int argc, char **argv) {
    if (argc != 2) fail("usage: ingest-worker-probe LOCAL_FILE");
    av_log_set_level(AV_LOG_QUIET);
    frame = av_malloc(MAX_FRAME + 1);
    if (!frame) fail("allocation failed");
    AVFormatContext *input = NULL;
    AVDictionary *options = NULL;
    /* Deliberately refuse network protocols during the proof. */
    av_dict_set(&options, "protocol_whitelist", "file", 0);
    int result = avformat_open_input(&input, argv[1], NULL, &options);
    av_dict_free(&options);
    if (result < 0) fail("cannot open local input");
    if (avformat_find_stream_info(input, NULL) < 0) fail("cannot read stream info");
    if (!input->nb_streams || input->nb_streams > MAX_STREAMS) fail("unsupported stream count");

    append("{\"type\":\"hello\",\"version\":1,\"generation\":1,\"streams\":%u,\"avcodec_version\":%u,\"skip_samples_type\":%d}",
           input->nb_streams, avcodec_version(), AV_PKT_DATA_SKIP_SAMPLES);
    emit();
    for (unsigned i = 0; i < input->nb_streams; i++) {
        AVStream *stream = input->streams[i];
        AVCodecParameters *p = stream->codecpar;
        if (p->codec_type != AVMEDIA_TYPE_VIDEO && p->codec_type != AVMEDIA_TYPE_AUDIO)
            fail("only audio/video supported");
        append("{\"type\":\"stream\",\"generation\":1,\"index\":%u,\"codec\":\"%s\","
               "\"media\":\"%s\",\"time_base\":[%d,%d],\"profile\":%d,\"level\":%d,"
               "\"width\":%d,\"height\":%d,\"sample_rate\":%d,\"channels\":%d,"
               "\"initial_padding\":%d,\"trailing_padding\":%d,\"seek_preroll\":%d,\"extradata\":",
               i, avcodec_get_name(p->codec_id), av_get_media_type_string(p->codec_type),
               stream->time_base.num, stream->time_base.den, p->profile, p->level,
               p->width, p->height, p->sample_rate, p->ch_layout.nb_channels,
               p->initial_padding, p->trailing_padding, p->seek_preroll);
        if (p->extradata_size < 0) fail("invalid codec configuration");
        hex(p->extradata, p->extradata_size);
        append(",\"side_data\":");
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
        append("{\"type\":\"packet\",\"generation\":1,\"sequence\":%llu,\"stream\":%d,\"pts\":",
               (unsigned long long)sequence++, packet->stream_index);
        timestamp(packet->pts);
        append(",\"dts\":");
        timestamp(packet->dts);
        append(",\"duration\":%lld,\"flags\":%d,\"data\":",
               (long long)packet->duration, packet->flags);
        hex(packet->data, packet->size);
        append(",\"side_data\":");
        side_data(packet->side_data, packet->side_data_elems);
        append("}");
        emit();
        av_packet_unref(packet);
    }
    if (result != AVERROR_EOF) fail("input read failed");
    append("{\"type\":\"end\",\"generation\":1,\"packets\":%llu}", (unsigned long long)sequence);
    emit();
    av_packet_free(&packet);
    avformat_close_input(&input);
    av_free(frame);
    return 0;
}
