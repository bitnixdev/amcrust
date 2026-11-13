/* Experimental one-generation packet IPC -> fragmented MP4 sink.
 * Local files only, exclusive create, delete incomplete output on reported errors.
 * This is not a production muxer or a complete AVCodecParameters wire ABI.
 */
#include <json-c/json.h>
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/channel_layout.h>
#include <libavutil/mem.h>
#include <libavutil/mathematics.h>
#include <libavutil/pixdesc.h>
#include <libavutil/samplefmt.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_FRAME (16U * 1024U * 1024U)
#define MAX_BYTES (4U * 1024U * 1024U)
#define MAX_SIDE (1U * 1024U * 1024U)
#define MAX_STREAMS 8
static const char *output_path;
static int created;
static FILE *output_file;
static AVFormatContext *output;
static AVIOContext *io;

static _Noreturn void fail(const char *message) {
    fprintf(stderr, "mux worker: %s\n", message);
    if (created) unlink(output_path);
    exit(1);
}

static json_object *field(json_object *object, const char *key) {
    json_object *value;
    if (!json_object_is_type(object, json_type_object) ||
        !json_object_object_get_ex(object, key, &value)) fail("missing field");
    return value;
}

static int64_t number(json_object *value, int64_t low, int64_t high) {
    if (!json_object_is_type(value, json_type_int)) fail("integer required");
    /* json-c also stores uint64 integers; get_int64 alone would saturate them. */
    const char *encoded = json_object_get_string(value);
    char *end;
    errno = 0;
    long long parsed = strtoll(encoded, &end, 10);
    if (errno == ERANGE || *end) fail("integer out of range");
    int64_t n = parsed;
    if (n < low || n > high) fail("integer out of range");
    return n;
}

static int64_t num(json_object *object, const char *key, int64_t low, int64_t high) {
    return number(field(object, key), low, high);
}

static const char *text(json_object *object, const char *key) {
    json_object *value = field(object, key);
    if (!json_object_is_type(value, json_type_string)) fail("string required");
    const char *s = json_object_get_string(value);
    if ((size_t)json_object_get_string_len(value) != strlen(s)) fail("embedded string NUL");
    return s;
}

static int nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    fail("invalid hex byte field");
    return 0;
}

static uint8_t *bytes(json_object *object, const char *key, size_t maximum, size_t *size) {
    const char *s = text(object, key);
    size_t length = strlen(s);
    if (length % 2 || length / 2 > maximum) fail("byte field limit exceeded");
    *size = length / 2;
    uint8_t *data = av_mallocz(*size + AV_INPUT_BUFFER_PADDING_SIZE);
    if (!data) fail("allocation failed");
    for (size_t i = 0; i < *size; i++) data[i] = (nibble(s[2*i]) << 4) | nibble(s[2*i+1]);
    return data;
}

static AVRational rational(json_object *object, const char *key, int zero_allowed) {
    json_object *array = field(object, key);
    if (!json_object_is_type(array, json_type_array) || json_object_array_length(array) != 2)
        fail("invalid rational");
    AVRational q = {
        (int)number(json_object_array_get_idx(array, 0), zero_allowed ? 0 : 1, INT_MAX),
        (int)number(json_object_array_get_idx(array, 1), 1, INT_MAX)
    };
    return q;
}

static void side_data(json_object *object, AVPacket *packet, AVCodecParameters *parameters) {
    json_object *array = field(object, "side_data");
    if (!json_object_is_type(array, json_type_array) || json_object_array_length(array) > 64)
        fail("side data limit exceeded");
    size_t total = 0;
    int seen[AV_PKT_DATA_NB] = {0};
    for (size_t i = 0; i < json_object_array_length(array); i++) {
        json_object *entry = json_object_array_get_idx(array, i);
        int type = (int)num(entry, "type", 0, AV_PKT_DATA_NB - 1);
        if (seen[type]++) fail("duplicate side data type");
        size_t size;
        uint8_t *data = bytes(entry, "data", MAX_SIDE - total, &size);
        total += size;
        uint8_t *destination;
        if (packet) destination = av_packet_new_side_data(packet, type, size);
        else {
            AVPacketSideData *sd = av_packet_side_data_new(&parameters->coded_side_data,
                &parameters->nb_coded_side_data, type, size, 0);
            destination = sd ? sd->data : NULL;
        }
        if (!destination) fail("side data allocation failed");
        memcpy(destination, data, size);
        av_free(data);
    }
}

static json_object *read_frame(void) {
    uint8_t header[4];
    size_t n = fread(header, 1, 4, stdin);
    if (!n && feof(stdin)) return NULL;
    if (n != 4) fail("truncated frame header");
    uint32_t size = ((uint32_t)header[0] << 24) | ((uint32_t)header[1] << 16) |
                    ((uint32_t)header[2] << 8) | header[3];
    if (!size || size > MAX_FRAME) fail("frame limit exceeded");
    char *body = malloc((size_t)size + 1);
    if (!body) fail("allocation failed");
    if (fread(body, 1, size, stdin) != size) fail("truncated frame body");
    body[size] = 0;
    json_tokener *parser = json_tokener_new_ex(32);
    if (!parser) fail("parser allocation failed");
    json_tokener_set_flags(parser, JSON_TOKENER_STRICT | JSON_TOKENER_VALIDATE_UTF8);
    json_object *object = json_tokener_parse_ex(parser, body, size);
    if (json_tokener_get_error(parser) != json_tokener_success ||
        json_tokener_get_parse_end(parser) != size ||
        !json_object_is_type(object, json_type_object)) fail("invalid JSON frame");
    json_tokener_free(parser);
    free(body);
    return object;
}

static int write_bytes(void *opaque, const uint8_t *buffer, int size) {
    FILE *file = opaque;
    return fwrite(buffer, 1, size, file) == (size_t)size ? size : AVERROR(EIO);
}

static void start_output(void) {
    int fd = open(output_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) fail("output must be a new local file");
    created = 1;
    output_file = fdopen(fd, "wb");
    if (!output_file) fail("cannot open output stream");
    uint8_t *buffer = av_malloc(32768);
    if (!buffer) fail("allocation failed");
    io = avio_alloc_context(buffer, 32768, 1, output_file, NULL, write_bytes, NULL);
    if (!io) fail("output allocation failed");
    output->pb = io;
    output->flags |= AVFMT_FLAG_CUSTOM_IO;
    output->avoid_negative_ts = AVFMT_AVOID_NEG_TS_DISABLED;
    AVDictionary *options = NULL;
    /* delay_moov lets the muxer establish both track epochs before writing init.
     * No per-track timestamp zeroing or synthetic packet-order clocks here. */
    av_dict_set(&options, "movflags", "frag_keyframe+delay_moov+default_base_moof", 0);
    av_dict_set(&options, "use_editlist", "1", 0);
    int64_t movie_clock = 1;
    for (unsigned i = 0; i < output->nb_streams; i++) {
        int64_t denominator = output->streams[i]->time_base.den;
        int64_t factor = denominator / av_gcd(movie_clock, denominator);
        if (movie_clock > INT_MAX / factor) fail("movie timescale limit exceeded");
        movie_clock *= factor;
    }
    av_dict_set_int(&options, "movie_timescale", movie_clock, 0);
    if (avformat_write_header(output, &options) < 0) fail("cannot write MP4 header");
    av_dict_free(&options);
}

int main(int argc, char **argv) {
    if (argc != 2) fail("usage: mux-worker-probe NEW_LOCAL_MP4");
    output_path = argv[1];
    av_log_set_level(AV_LOG_ERROR);
    if (avformat_alloc_output_context2(&output, NULL, "mp4", NULL) < 0 || !output)
        fail("cannot allocate muxer");
    json_object *object = read_frame();
    if (!object || strcmp(text(object, "type"), "hello") || num(object, "version", 1, 1) != 1)
        fail("missing hello");
    int64_t generation = num(object, "generation", 1, INT_MAX);
    unsigned streams = (unsigned)num(object, "streams", 1, MAX_STREAMS);
    if (num(object, "avcodec_version", 1, UINT_MAX) != avcodec_version()) fail("library version mismatch");
    json_object_put(object);
    AVRational clocks[MAX_STREAMS];
    int64_t last_dts[MAX_STREAMS];
    int started_track[MAX_STREAMS] = {0};
    for (unsigned i = 0; i < streams; i++) {
        object = read_frame();
        if (!object || strcmp(text(object, "type"), "stream") ||
            num(object, "generation", 1, INT_MAX) != generation ||
            num(object, "index", 0, MAX_STREAMS-1) != i) fail("invalid descriptor order/generation");
        AVStream *stream = avformat_new_stream(output, NULL);
        if (!stream) fail("stream allocation failed");
        AVCodecParameters *p = stream->codecpar;
        const char *codec = text(object, "codec");
        const char *media = text(object, "media");
        if (!strcmp(codec, "h264") && !strcmp(media, "video")) {
            p->codec_type = AVMEDIA_TYPE_VIDEO;
            p->codec_id = AV_CODEC_ID_H264;
            p->width = (int)num(object, "width", 1, 8192);
            p->height = (int)num(object, "height", 1, 8192);
            p->format = (int)num(object, "format", -1, AV_PIX_FMT_NB-1);
            p->sample_aspect_ratio = rational(object, "sample_aspect_ratio", 1);
            p->color_range = (int)num(object, "color_range", 0, AVCOL_RANGE_NB-1);
            p->color_primaries = (int)num(object, "color_primaries", 0, AVCOL_PRI_NB-1);
            p->color_trc = (int)num(object, "color_trc", 0, AVCOL_TRC_NB-1);
            p->color_space = (int)num(object, "color_space", 0, AVCOL_SPC_NB-1);
            p->chroma_location = (int)num(object, "chroma_location", 0, AVCHROMA_LOC_NB-1);
        } else if (!strcmp(codec, "aac") && !strcmp(media, "audio")) {
            p->codec_type = AVMEDIA_TYPE_AUDIO;
            p->codec_id = AV_CODEC_ID_AAC;
            p->sample_rate = (int)num(object, "sample_rate", 8000, 192000);
            p->format = (int)num(object, "format", -1, AV_SAMPLE_FMT_NB-1);
            if (av_channel_layout_from_string(&p->ch_layout, text(object, "channel_layout")) < 0 ||
                p->ch_layout.nb_channels != num(object, "channels", 1, 8)) fail("invalid channel layout");
            p->frame_size = (int)num(object, "frame_size", 1, 8192);
            p->initial_padding = (int)num(object, "initial_padding", 0, INT_MAX);
            p->trailing_padding = (int)num(object, "trailing_padding", 0, INT_MAX);
            p->seek_preroll = (int)num(object, "seek_preroll", 0, INT_MAX);
        } else fail("only H264/AAC supported");
        p->profile = (int)num(object, "profile", INT_MIN, INT_MAX);
        p->level = (int)num(object, "level", INT_MIN, INT_MAX);
        clocks[i] = rational(object, "time_base", 0);
        stream->time_base = clocks[i];
        size_t size;
        p->extradata = bytes(object, "extradata", MAX_BYTES, &size);
        p->extradata_size = (int)size;
        if (!size) fail("missing codec configuration");
        side_data(object, NULL, p);
        last_dts[i] = AV_NOPTS_VALUE;
        json_object_put(object);
    }
    uint64_t sequence = 0;
    int ended = 0;
    while ((object = read_frame())) {
        if (ended || num(object, "generation", 1, INT_MAX) != generation) fail("mixed/closed generation");
        const char *type = text(object, "type");
        if (!strcmp(type, "end")) {
            if (num(object, "packets", 0, INT64_MAX) != (int64_t)sequence || !sequence)
                fail("invalid end count");
            ended = 1;
        } else if (!strcmp(type, "packet")) {
            if (num(object, "sequence", 0, INT64_MAX) != (int64_t)sequence++) fail("packet gap/duplicate");
            unsigned index = (unsigned)num(object, "stream", 0, streams-1);
            AVPacket *packet = av_packet_alloc();
            if (!packet) fail("packet allocation failed");
            size_t size;
            uint8_t *data = bytes(object, "data", MAX_BYTES, &size);
            if (!size || av_packet_from_data(packet, data, (int)size) < 0)
                fail("packet data allocation failed");
            packet->stream_index = index;
            /* Missing timestamps need a separately proven normalization policy. */
            if (json_object_is_type(field(object, "pts"), json_type_null) ||
                json_object_is_type(field(object, "dts"), json_type_null)) fail("missing packet timestamp");
            packet->pts = num(object, "pts", INT64_MIN+1, INT64_MAX);
            packet->dts = num(object, "dts", INT64_MIN+1, INT64_MAX);
            packet->duration = num(object, "duration", 1, INT64_MAX);
            packet->flags = (int)num(object, "flags", 0, INT_MAX);
            side_data(object, packet, NULL);
            if (last_dts[index] != AV_NOPTS_VALUE && packet->dts <= last_dts[index]) fail("nonmonotonic DTS");
            if (!started_track[index] && output->streams[index]->codecpar->codec_type == AVMEDIA_TYPE_VIDEO &&
                !(packet->flags & AV_PKT_FLAG_KEY)) fail("video must start at keyframe");
            started_track[index] = 1;
            last_dts[index] = packet->dts;
            if (!created) start_output();
            av_packet_rescale_ts(packet, clocks[index], output->streams[index]->time_base);
            if (av_interleaved_write_frame(output, packet) < 0) fail("packet mux failed");
            av_packet_free(&packet);
        } else fail("unexpected frame type");
        json_object_put(object);
    }
    if (!ended) fail("truncated generation");
    for (unsigned i = 0; i < streams; i++) if (!started_track[i]) fail("empty track");
    if (av_write_trailer(output) < 0) fail("trailer failed");
    avio_flush(io);
    if (io->error || fflush(output_file) || fclose(output_file)) fail("output write failed");
    av_freep(&io->buffer);
    avio_context_free(&io);
    avformat_free_context(output);
    return 0;
}
