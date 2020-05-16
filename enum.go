package kafka_go

type OffsetType string

const (
	Newest OffsetType = "newest"
	Oldest OffsetType = "oldest"
)
