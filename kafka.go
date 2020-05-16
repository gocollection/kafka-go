package kafka_go

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"syscall"
)

func NewKafkaConsumer(params *KafkaParams) (*KafkaConsumer, error) {
	if params.Topics == nil || len(params.Topics) == 0 {
		Logger.Println("No topic to be subscribed")
		return nil, fmt.Errorf("topics can not be empty")
	}
	if params.Handlers == nil {
		Logger.Println("Handlers absent")
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
	return &KafkaConsumer{
		cg:     consumerGroup,
		cgh:    NewConsumerHandler(params.Handlers, params.Fallbacks),
		topics: params.Topics,
	}, nil
}

type KafkaParams struct {
	Brokers []string
	GroupID string

	// [Optional]
	// client identity for logging purpose
	ClientID string

	// [Optional]
	// The initial offset to use if no offset was previously committed.
	// Should be Newest or Oldest. defaults - Newest.
	OffsetInitial OffsetType

	// [Optional]
	// kafka cluster version. eg - "2.2.1" default - "2.3.0"
	Version string

	// List of topics to start listening from
	Topics []string

	// Topic to handlers map to consumer message from a topic.
	Handlers map[string]TopicHandler

	// [Optional]
	// Topic to its fallback handler map.
	// If the Main handler returns false, it will try to fallback handler.
	// it will commit the offset not matter fallback handler returns true or false.
	Fallbacks map[string]TopicHandler
}

type KafkaConsumer struct {
	cg     sarama.ConsumerGroup
	cgh    *ConsumerHandler
	topics []string
	cancel context.CancelFunc
}

func (kc *KafkaConsumer) Start(parentContext context.Context) {
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
			kc.cgh.Ready = make(chan bool)
		}
	}()

	// Await till the consumer has been set up
	<-kc.cgh.Ready
	Logger.Println("Consumer up and running")
	stopSignal := watchStopSignal(kc)
	<-stopSignal
	Logger.Printf("Consumer down")
}

func (kc *KafkaConsumer) Stop() {
	kc.cancel()
	if err := kc.cg.Close(); err != nil {
		Logger.Printf("Error closing consumer, %v", err)
	}
}

func watchStopSignal(consumer *KafkaConsumer) chan bool {
	stopSignal := make(chan os.Signal)
	doneClosure := make(chan bool)

	signal.Notify(stopSignal, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-stopSignal
		Logger.Printf("signal received %v", sig)
		consumer.Stop()
		doneClosure <- true
	}()
	return doneClosure
}

func getConsumerGroup(params *KafkaParams) (sarama.ConsumerGroup, error) {
	config, err := getConsumerConfig(params)
	if err != nil {
		return nil, err
	}
	return sarama.NewConsumerGroup(params.Brokers, params.GroupID, config)
}

func getConsumerConfig(params *KafkaParams) (*sarama.Config, error) {
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

	if params.OffsetInitial == Oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	config.Consumer.Return.Errors = true
	return config, nil
}
