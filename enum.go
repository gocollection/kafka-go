package kafka_go

/*
OffsetType specifies strategy to determine the starting offset of a consumer group
if there is not previously committed offset that consumer group in the cluster.
Offset type setting will be ignored all together if client finds any existing committed
offset in the cluster while registering its consumer process to the cluster.
*/
type OffsetType string

const (
	// Set offset to the offset of the next message to be appeared in the partition
	OtNewest OffsetType = "newest"

	// Set offset to the offset of the oldest available message present in the partition
	OtOldest OffsetType = "oldest"
)

/*
CompressionType specifies the type of message compression before publishing
messages to the cluster
*/
type CompressionType int8

const (
	// No compression
	CtNone CompressionType = iota
	// GZIP compression type
	CtGzip
	// SAPPY data compression
	CtSnappy
	// LZ4 algorithm compression
	CtLz4
	// Z-standard compression
	CtZstd
)

/*
AcknowledgmentType specifies the kind of ack end user should expect while publishing
message to the cluster
*/
type AcknowledgmentType int16

const (
	// Acknowledge from all in-sync replicas
	AtAll AcknowledgmentType = iota - 1
	// No acknowledgement required
	AtNone
	// Acknowledge only from master broker
	AtLocal
)
