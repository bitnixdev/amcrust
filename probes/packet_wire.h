/* Experimental AMC2 envelope, all length integers big-endian u32.
 * u32 body_length; magic[4]; u32 metadata_length; u32 blob_length;
 * UTF-8 JSON metadata; raw blob bytes. No native structs on the wire.
 * Byte refs are {offset,length}, contiguous in primary/side-data order.
 */
#ifndef AMCRUST_PACKET_WIRE_H
#define AMCRUST_PACKET_WIRE_H
#define WIRE_MAGIC "AMC2"
#define WIRE_HEADER 12U
#define WIRE_METADATA (64U * 1024U)
#define WIRE_BLOB (5U * 1024U * 1024U)
#define WIRE_MAX (WIRE_HEADER + WIRE_METADATA + WIRE_BLOB)
#endif
