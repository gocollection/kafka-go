package kafka_go

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"syscall"
	"time"
)

/*
NewKafkaProducer creates a kafkaProducer instance used for producing messages to kafka cluster.
Input kafkaProducerParam should be created using DefaultKafkaProducerParam and then the required
params should changed from there. Its not allowed to construct kafkaProducerParam directly. This
is enforced to prevent user from setting unexpected default values.

If the user is only interested with providing broker address and do not willing to changes any default
param values, NewKafkaProducerQuick can be used instead in such cases. which only takes the broker
as input.

example:
	params := DefaultKafkaProducerParam([]string{"localhost:9091", "localhost:9092", "localhost:9093"})
	params.Retry = 5
	params.Acknowledge = AtAll
	producer, err := NewKafkaProducer(params)
*/
func NewKafkaProducer(params *kafkaProducerParam) (*kafkaProducer, error) {
	if params.Brokers == nil || len(params.Brokers) == 0 {
		Logger.Println("No broker provided")
		return nil, fmt.Errorf("at least one broker is mandatory")
	}
	if params.Compression > 4 || params.Compression < 0 {
		Logger.Println("Invalid compression type")
		return nil, fmt.Errorf("compression type is not valid")
	}
	if params.Acknowledge > 1 || params.Acknowledge < -1 {
		Logger.Println("Invalid acknowledgement type")
		return nil, fmt.Errorf("acknowledgement type is not valid")
	}
	config := getConfig(params)
	producer := &kafkaProducer{
		ap: asyncProducer(params.Brokers, config),
		sp: syncProducer(params.Brokers, config),
	}
	watchCloseSignal(producer)
	return producer, nil
}

/*
NewKafkaProducerQuick is quicker way to instantiate a producer instance by just providing the
brokers address all together. It will take case of other params and set them to there best default
values. In case user want to have better configured producer as per their use case, they should
rather use NewKafkaProducer with custom kafkaProducerParam key values. DefaultKafkaProducerParam
is the only way to create kafkaProducerParam because changing any other params associated.
*/
func NewKafkaProducerQuick(brokers []string) (*kafkaProducer, error) {
	params := DefaultKafkaProducerParam(brokers)
	return NewKafkaProducer(params)
}

/*
DefaultKafkaProducerParam is only way to create kafkaProducerParam instance to be used for producer
instance creation. This hsa been restricted to avoid user from setting bad default values while
configuring a producer. User interested in changing the params should first create param instance
using this method and then change the specific param key if needed.

example:
	params := DefaultKafkaProducerParam(brokers)
	params.LogError = false
*/
func DefaultKafkaProducerParam(brokers []string) *kafkaProducerParam {
	return &kafkaProducerParam{
		Brokers:        brokers,
		FlushFrequency: 500 * time.Millisecond,
		FlushMessages:  5,
		LogError:       true,
		Retry:          3,
		Compression:    CtSnappy,
		Acknowledge:    AtLocal,
	}
}

/*
kafkaProducerParam can't be created by end user to protect default values.
its make it easy to start from default values by using DefaultKafkaProducerParam.
All you need is just the brokers to start using the producer.
*/
type kafkaProducerParam struct {
	// Brokers in kafka clusters
	Brokers []string

	// [Optional]
	// Client identity for logging purpose
	ClientID string

	// [Optional]
	// kafka cluster version. eg - "2.2.1" default - "2.3.0"
	Version string

	// [Optional]
	// How frequently should the message be flushed to the cluster. default - 500ms
	FlushFrequency time.Duration

	// [Optional]
	// Number of message to trigger a flush. default - 5
	FlushMessages int

	// [Optional]
	// Log error on failure while publishing messaged. default - true
	// applicable only for async producer
	LogError bool

	// [Optional]
	// Total retries to publish a message. default - 3
	Retry int

	// [Optional]
	// Compression while publishing the message. default - snappy
	Compression CompressionType

	// [Optional]
	// Acknowledgement for the published message. default - local
	Acknowledge AcknowledgmentType
}

type kafkaProducer struct {
	ap sarama.AsyncProducer
	sp sarama.SyncProducer
}

func (kp *kafkaProducer) PublishSync(message *PublisherMessage) (meta map[string]interface{}, err error) {
	partition, offset, err := kp.sp.SendMessage(&sarama.ProducerMessage{
		Topic: message.Topic,
		Key:   sarama.StringEncoder(message.Key),
		Value: sarama.ByteEncoder(message.Data),
	})
	meta = make(map[string]interface{})
	meta["partition"] = partition
	meta["offset"] = offset
	return
}

func (kp *kafkaProducer) PublishSyncBulk(messages []*PublisherMessage) error {
	producerMessages := make([]*sarama.ProducerMessage, 0, len(messages))
	for _, message := range messages {
		producerMessages = append(producerMessages, &sarama.ProducerMessage{
			Topic: message.Topic,
			Key:   sarama.StringEncoder(message.Key),
			Value: sarama.ByteEncoder(message.Data),
		})
	}
	return kp.sp.SendMessages(producerMessages)
}

func (kp *kafkaProducer) PublishAsync(message *PublisherMessage) {
	kp.ap.Input() <- &sarama.ProducerMessage{
		Topic: message.Topic,
		Key:   sarama.StringEncoder(message.Key),
		Value: sarama.ByteEncoder(message.Data),
	}
}

func (kp *kafkaProducer) PublishAsyncBulk(messages []*PublisherMessage) {
	for _, message := range messages {
		go func(msg *PublisherMessage) {
			kp.ap.Input() <- &sarama.ProducerMessage{
				Topic: msg.Topic,
				Key:   sarama.StringEncoder(msg.Key),
				Value: sarama.ByteEncoder(msg.Data),
			}
		}(message)
	}
}

func (kp *kafkaProducer) Close() {
	err := kp.sp.Close()
	if err != nil {
		Logger.Printf("Error closing sync producer, %v", err)
	}
	err = kp.ap.Close()
	if err != nil {
		Logger.Printf("Error closing async producer, %v", err)
	}
	Logger.Printf("Producer closed")
}

func watchCloseSignal(producer *kafkaProducer) {
	stopSignal := make(chan os.Signal)
	signal.Notify(stopSignal, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-stopSignal
		Logger.Printf("Close signal received %v", sig)
		producer.Close()
	}()
}

func getConfig(params *kafkaProducerParam) *sarama.Config {
	config := sarama.NewConfig()
	if params.ClientID != "" {
		config.ClientID = params.ClientID
	}
	if params.Version != "" {
		version, err := sarama.ParseKafkaVersion(params.Version)
		if err != nil {
			Logger.Fatalf("Error parsing kafka version, %v", err)
		}
		config.Version = version
	} else {
		config.Version = sarama.MaxVersion
	}
	config.Producer.RequiredAcks = sarama.RequiredAcks(params.Acknowledge)
	config.Producer.Compression = sarama.CompressionCodec(params.Compression)
	config.Producer.Flush.Frequency = params.FlushFrequency
	config.Producer.Flush.Messages = params.FlushMessages
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = params.LogError
	config.Producer.Retry.Max = params.Retry
	return config
}

func syncProducer(brokers []string, config *sarama.Config) sarama.SyncProducer {
	// overriding Return keys for sync producer
	config.Producer.Return.Errors = true
	syncProducer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		Logger.Fatalf("Unable to create sync producer: %v", err)
	}
	return syncProducer
}

func asyncProducer(brokers []string, config *sarama.Config) sarama.AsyncProducer {
	asyncProducer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		Logger.Fatalf("Unable to create async producer: %v", err)
	}
	if config.Producer.Return.Errors {
		go func() {
			for err := range asyncProducer.Errors() {
				Logger.Printf("Error from async producer: %v", err)
			}
		}()
	}
	if config.Producer.Return.Successes {
		go func() {
			for range asyncProducer.Successes() {
				continue
			}
		}()
	}

	return asyncProducer
}
