package kafka_go

import (
	"context"
	"sync"
	"testing"
)

func TestKafkaParamTopicRequired(t *testing.T) {
	_, err := NewKafkaConsumer(&KafkaParams{
		Brokers:       []string{"localhost:9090"},
		GroupID:       "test-cg",
		OffsetInitial: Newest,
	})

	if err != nil {
		if err.Error() != "topics can not be empty" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with topic empty")
	}
}

func TestKafkaParamHandlerRequired(t *testing.T) {
	_, err := NewKafkaConsumer(&KafkaParams{
		Brokers:       []string{"localhost:9090"},
		GroupID:       "test-cg",
		OffsetInitial: Newest,
		Topics:        []string{"test-topic"},
	})

	if err != nil {
		if err.Error() != "handler is not optional" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with topic handler empty")
	}
}

func ExampleKakfaConsumerSingle() {
	consumer, err := NewKafkaConsumer(&KafkaParams{
		Brokers:       []string{"broker-1", "broker-2", "broker-3"},
		GroupID:       "test-cg",
		OffsetInitial: Newest,
		Topics:        []string{"test-topic"},
		Handlers: map[string]TopicHandler{
			"test-topic": &testTopicHandler{},
		},
	})
	if err != nil {
		Logger.Panicf("Error creating consumer instance %v", err)
	}
	consumer.Start(context.Background())
}

func ExampleKakfaConsumerMultiple() {

	// Consumer A
	consumerA, err := NewKafkaConsumer(&KafkaParams{
		Brokers:       []string{"brokerA-1", "brokerA-2", "brokerA-3"},
		GroupID:       "test-cg-a",
		OffsetInitial: Newest,
		Topics:        []string{"test-topic-a"},
		Handlers: map[string]TopicHandler{
			"test-topic-a": &testTopicHandler{},
		},
	})
	if err != nil {
		Logger.Panicf("Error creating consumer a instance %v", err)
	}

	// Consumer B
	consumerB, err := NewKafkaConsumer(&KafkaParams{
		Brokers:       []string{"brokerB-1", "brokerB-2", "brokerB-3"},
		GroupID:       "test-cg-b",
		OffsetInitial: Newest,
		Topics:        []string{"test-topic-b"},
		Handlers: map[string]TopicHandler{
			"test-topic-b": &testTopicHandler{},
		},
	})
	if err != nil {
		Logger.Panicf("Error creating consumer b instance %v", err)
	}

	group := sync.WaitGroup{}
	group.Add(2)
	go func() {
		defer group.Done()
		consumerA.Start(context.Background())
	}()
	go func() {
		defer group.Done()
		consumerB.Start(context.Background())
	}()
	group.Wait()
}

// test topic handler
type testTopicHandler struct {
}

func (t *testTopicHandler) Handle(ctx context.Context, msg *Message) bool {
	Logger.Printf("Topic: %v, Partition: %v, Message: %v", msg.Topic, msg.Partition, string(msg.Value))
	return true
}
