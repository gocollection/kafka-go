package kafka_go

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"syscall"
)

/*
NewKafkaConsumer creates a new kafkaConsumer instance used for consuming from kafka cluster.
Needs KafkaConsumerParam to instantiate the connection.

example:
	consumer, err := NewKafkaConsumer(&KafkaConsumerParam{
		Brokers:       []string{"localhost:9091", "localhost:9092", "localhost:9093"},
		GroupID:       "test-cg",
		OffsetInitial: OtNewest,
		Topics:        []string{"test-topic"},
		Handlers: map[string]TopicHandler{
			"test-topic": &testTopicHandler{},
		},
	})
	if err != nil {
		// handle error
	}
	// start the blocking consumer process
	consumerA.Start(context.Background())

Refer to test example for better understanding
*/
func NewKafkaConsumer(params *KafkaConsumerParam) (*kafkaConsumer, error) {
	if params.Brokers == nil || len(params.Brokers) == 0 {
		Logger.Println("No broker provided")
		return nil, fmt.Errorf("at least one broker is mandatory")
	}
	if params.Topics == nil || len(params.Topics) == 0 {
		Logger.Println("No topic to be subscribed")
		return nil, fmt.Errorf("topics can not be empty")
	}
	if params.Handlers == nil {
		Logger.Println("handlers absent")
		return nil, fmt.Errorf("handler is not optional")
	}
	if params.Fallbacks == nil {
		Logger.Println("Optional fallback handler is absent")
		params.Fallbacks = make(map[string]TopicHandler)
	}
	if params.ClientID == "" {
		// client id for given consume group
		params.ClientID = "client-" + params.GroupID
	}
	consumerGroup, err := getConsumerGroup(params)
	if err != nil {
		return nil, err
	}
	return &kafkaConsumer{
		cg:     consumerGroup,
		cgh:    newConsumerHandler(params.Handlers, params.Fallbacks),
		topics: params.Topics,
	}, nil
}

/*
KafkaConsumerParam is the input expected from the user to start a consumer session with the kafka cluster.
*/
type KafkaConsumerParam struct {
	// Brokers in kafka clusters
	Brokers []string

	// Consumer group id of this consumer group
	GroupID string

	// List of topics to start listening from
	Topics []string

	// Topic to handlers map to consumer message from a topic.
	Handlers map[string]TopicHandler

	// [Optional]
	// Topic to its fallback handler map.
	// If the Main handler returns false, it will try to fallback handler.
	// it will commit the offset not matter fallback handler returns true or false.
	Fallbacks map[string]TopicHandler

	// [Optional]
	// Client identity for logging purpose
	ClientID string

	// [Optional]
	// The initial offset to use if no offset was previously committed.
	// Should be OtNewest or OtOldest. defaults - OtNewest.
	OffsetInitial OffsetType

	// [Optional]
	// kafka cluster version. eg - "2.2.1" default - "2.3.0"
	Version string
}

/*
kafkaConsumer instance provides API to start the consumption related process with the cluster
*/
type kafkaConsumer struct {
	cg     sarama.ConsumerGroup
	cgh    *consumerHandler
	topics []string
	cancel context.CancelFunc
}

/*
Start begins the message consumption process from the subscribed topics. Pass claimed messaged to
the configured Topic handlers if any or simple ignore the message. Based on the boolean response
from the handler either it mark the message as consumed or try for the fallback handler if any to handle
failure from the main handler. At the end it will mark the message as consumed even if the fallback handler
fails.

Its upto the user to make sure that, handler should not starve the consumer for too long. A retry with publish
failed message to another topic is considered a better approach and also helps clearing out the pending messages
quickly.

Note - messages are consumed in a separate goroutine per partition per topic. Means subscribing to 2 topics with 5
(claimed) partition each will run 10 goroutines in parallel to consume messages from all the partitions.
*/
func (kc *kafkaConsumer) Start(parentContext context.Context) {
	ctx, cancel := context.WithCancel(parentContext)
	kc.cancel = cancel
	cg := kc.cg
	// go routine to log consumer error if any
	go func() {
		for err := range cg.Errors() {
			if consumerError, ok := err.(*sarama.ConsumerError); ok {
				Logger.Printf("Consumer failed: %v", consumerError)
			} else {
				Logger.Printf("Consumer unknown error: %v", err)
			}
		}
	}()

	go func() {
		for {
			if err := cg.Consume(ctx, kc.topics, kc.cgh); err != nil {
				Logger.Printf("Error while registering consumer, %v", err)
			}
			if ctx.Err() != nil {
				Logger.Println("Consumer context has been canceled")
				return
			}
			kc.cgh.ready = make(chan bool)
		}
	}()

	// Await till the consumer has been set up
	<-kc.cgh.ready
	Logger.Println("Consumer up and running")
	stopSignal := watchStopSignal(kc)
	<-stopSignal
	Logger.Printf("Consumer down")
}

/*
Stop cancels the context passed to the consumer group session and hence cause all the goroutines to return
immediately from consuming messages from partitions of the subscribed topics. At the end closes the consumer
group to avoid any possible leaks.
*/
func (kc *kafkaConsumer) Stop() {
	kc.cancel()
	if err := kc.cg.Close(); err != nil {
		Logger.Printf("Error closing consumer, %v", err)
	}
}

func watchStopSignal(consumer *kafkaConsumer) chan bool {
	stopSignal := make(chan os.Signal)
	doneClosure := make(chan bool)

	signal.Notify(stopSignal, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-stopSignal
		Logger.Printf("Stop signal received %v", sig)
		consumer.Stop()
		doneClosure <- true
	}()
	return doneClosure
}

func getConsumerGroup(params *KafkaConsumerParam) (sarama.ConsumerGroup, error) {
	config, err := getConsumerConfig(params)
	if err != nil {
		return nil, err
	}
	return sarama.NewConsumerGroup(params.Brokers, params.GroupID, config)
}

func getConsumerConfig(params *KafkaConsumerParam) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.ClientID = params.ClientID

	if params.Version != "" {
		version, err := sarama.ParseKafkaVersion(params.Version)
		if err != nil {
			return nil, err
		}
		config.Version = version
	} else {
		config.Version = sarama.MaxVersion
	}

	if params.OffsetInitial == OtOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	config.Consumer.Return.Errors = true
	return config, nil
}
