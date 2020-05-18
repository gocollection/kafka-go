package kafka_go

import (
	"context"
	"sync"
	"testing"
)

func TestConsumerBrokerRequired(t *testing.T) {
	_, err := NewKafkaConsumer(&KafkaConsumerParam{
		GroupID:       "test-cg",
		OffsetInitial: OtNewest,
	})
	if err != nil {
		if err.Error() != "at least one broker is mandatory" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with broker list nil")
	}

	_, err = NewKafkaConsumer(&KafkaConsumerParam{
		Brokers:       []string{},
		GroupID:       "test-cg",
		OffsetInitial: OtNewest,
	})
	if err != nil {
		if err.Error() != "at least one broker is mandatory" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with broker list empty")
	}
}

func TestConsumerTopicRequired(t *testing.T) {
	_, err := NewKafkaConsumer(&KafkaConsumerParam{
		Brokers:       []string{"localhost:9090"},
		GroupID:       "test-cg",
		OffsetInitial: OtNewest,
	})

	if err != nil {
		if err.Error() != "topics can not be empty" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with topic empty")
	}
}

func TestConsumerHandlerRequired(t *testing.T) {
	_, err := NewKafkaConsumer(&KafkaConsumerParam{
		Brokers:       []string{"localhost:9090"},
		GroupID:       "test-cg",
		OffsetInitial: OtNewest,
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

func ExampleConsumerSingle() {
	consumer, err := NewKafkaConsumer(&KafkaConsumerParam{
		Brokers:       []string{"broker-1", "broker-2", "broker-3"},
		GroupID:       "test-cg",
		OffsetInitial: OtNewest,
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

func ExampleConsumerMultiple() {

	// Consumer A
	consumerA, err := NewKafkaConsumer(&KafkaConsumerParam{
		Brokers:       []string{"brokerA-1", "brokerA-2", "brokerA-3"},
		GroupID:       "test-cg-a",
		OffsetInitial: OtNewest,
		Topics:        []string{"test-topic-a"},
		Handlers: map[string]TopicHandler{
			"test-topic-a": &testTopicHandler{},
		},
	})
	if err != nil {
		Logger.Panicf("Error creating consumer a instance %v", err)
	}

	// Consumer B
	consumerB, err := NewKafkaConsumer(&KafkaConsumerParam{
		Brokers:       []string{"brokerB-1", "brokerB-2", "brokerB-3"},
		GroupID:       "test-cg-b",
		OffsetInitial: OtNewest,
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

func (t *testTopicHandler) Handle(ctx context.Context, msg *SubscriberMessage) bool {
	Logger.Printf("Topic: %v, Partition: %v, SubscriberMessage: %v", msg.Topic, msg.Partition, string(msg.Value))
	return true
}
