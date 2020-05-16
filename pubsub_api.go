package kafka_go

import (
	"context"
)

type Consumer interface {
	Start(ctx context.Context)
	Stop()
}

type Message struct {
	// topic of the message
	Topic string

	// partition within the topic
	Partition int32

	// offset within the partition
	Offset int64

	// partition key bytes
	Key []byte

	// actual message bytes
	Value []byte
}

type TopicHandler interface {
	Handle(ctx context.Context, msg *Message) bool
}

type Producer interface {
	SendMessageAsync(topic string, key string, msg []byte) error
	SendMessageSync(topic string, key string, msg []byte) error
}
