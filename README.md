# Go Kafka Client - an easy abstraction
[![GoDoc][godoc-img]][godoc] [![Mit License][mit-img]][mit] [![Build Status][ci-img]][ci]

**kafka_go** is an abstraction over popular kafka client sarama (https://github.com/Shopify/sarama).
Though sarama provides good enough APIs to integrate with a kafka cluster but still lags simplicity and
needs a bit of domain knowledge even for a standard use case. 

End-user has to maintain **fail-safety**, **reclaim**
after re-balancing or similar scenarios, API doesn't seem very intuitive for the first time kafka users.
kafka_go tries to solve all such problems with its easy to understand APIs to start consuming from a kafka
cluster with significant less domain knowledge and complete fail-safety.

# How kafka_go helps
The package abstracts out fairly easy and intuitive abstraction to integrate with a kafka cluster with minimal effort.
It takes care of the complexity of fail-safety, possible leaks scenarios, re-balance handling, and export easy to use functional
interface. The API is divided into Consumer and Producer modules -  

## Consumer 
The consumer is an important abstraction of any PubSub library. The package tries to make it as simple as possible.
```go
type Consumer interface {

	//Start should trigger the actual message consumption process, it should be blocking in nature to avoid killing
	//process immaturely.
	Start(ctx context.Context)

	//Stop should trigger the closure of the consumption process. Should cancel the context to relieve resources and take
	//care of possible leaks
	Stop()
}
```

The package implements this simple generic **Consumer** interface for **Kafka**. A user can instantiate a consumer instance 
with minimal effort, Example -

```go
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
```
Refer to examples in the code base for better understanding.

## KafkaConsumerParam
It provides a single place to configure the Consumer client. ConsumerParam provider rich set of keys configurable
by the user with fairly standard defaults to start consuming out of the box. 

```go
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
	// default - "no fallback"
	Fallbacks map[string]TopicHandler

	// [Optional]
	// List of middleware to be triggered post claim of every message & before actual
	// message handling. Middleware will be triggered in increasing order order of index.
	// default - "no middleware"
	middleware []ConsumerMiddleware

	// [Optional]
	// List of interceptor, like Middleware it trigger post claim of every message, but unlike
	// middleware interceptor is available after the actual handler return. Interceptors are
	// triggered in layered manner, lower index being the outer layer and vice versa. This is
	// similar to recursive call, the one called first will return last.
	// default - "noOpInterceptor"
	interceptor []ConsumerInterceptor

	// [Optional]
	// Attach a meta map with every claimed message before passing it to actual handler, can be used
	// to persist state during the lifecycle of a claimed message. Middleware or Interceptor can also
	// use this meta to store variable across. default - "false"
	MessageMeta bool

	// [Optional]
	// Client identity for logging purpose
	ClientID string

	// [Optional]
	// The initial offset to use if no offset was previously committed.
	// Should be OtNewest or OtOldest. defaults - OtNewest.
	OffsetInitial OffsetType

	// [Optional]
	// kafka cluster version. eg - "2.2.1" default - "2.3.0"
	// supports versions from "0.8.x" to "2.3.x"
	Version string
}
```

## TopicHandler

```go
type TopicHandler interface {
	// Handle gets the actual message to be handled. Business logic for a given
	// message should ideally be implemented here.
	Handle(ctx context.Context, message *SubscriberMessage) bool
}
```
TopicHandler is an intuitive interface that exposes a single function **Handle**.
An implementation of this interface should be used as handler for messages from a topic.
Technically the business logic post receiving the message from a topic should trigger from here.
The handler has not been tightly bound to a topic on purpose to allow it to be used for multiple
topics at once. The implementation should be **thread/goroutine safe** obviously. Example - 

```go
// test topic handler
type testTopicHandler struct {
}

func (t *testTopicHandler) Handle(ctx context.Context, msg *SubscriberMessage) bool {
	Logger.Printf("Topic: %v, Partition: %v, SubscriberMessage: %v", msg.Topic, msg.Partition, string(msg.Value))
	return true
}
```

## Middleware

```go
type ConsumerMiddleware func(ctx context.Context, msg *SubscriberMessage)
```
Middleware is a construct similar to **Before** advice in _AOP_. Middleware comes just after claiming a message
from a topic and before passing it to its handler down the line. Middleware can be used to decorate the message or
preprocess something across topics before actual handling. This saves from writing common code across all the topic
handlers. The middleware can be optionally used to enrich message meta which can be used inside actual handler or later
in the handling pipeline. Multiple independent middleware can be used which pre-process the message in a pre-defined
manner. Use cases can be -

* Filtering messages to pass relevant messages to the actual handler
* Validating message for correctness
* logging, throttling, decorating etc are few other possibilities 

## Interceptor

```go
type ConsumerInterceptor func(ctx context.Context, msg *SubscriberMessage, handler func(context.Context, *SubscriberMessage) bool) bool
``` 
Interceptor is a construct similar to **Around** advice in _AOP_. Interceptor life cycle start just after claiming a
message around the handler and available even after the handler return. Interceptor might come handy start some processing
before actual message handling and ending it after the handling is done. Use cases can be -

* Newrelic transaction start before message handling and close after handler returns
* Collect data around message processing like time to process a message or so
* Take some common action on expected/unexpected handling of a message, like publish to dead letter

Multiple independent interceptor can be used which works in a layered fashion. For example if the List of interceptor
consist of **IC_1**, **IC_2**, **IC_3**, the processing will look like -

```
IC_1
{
  before_1
    IC_2
    {
      before_2
        IC_3
        {
          before_3
            message_handler(.....)
          after_3
        }
      after_2
    }
  after_1
}
```

## Producer
The next obvious abstraction that any PubSub library will export is **Producer**.

```go
type Producer interface {

	// PublishSync sends a message to the PubSub cluster in Sync way. Call to this function is blocking and
	// returns only after the publish is done or result in an error. Meta contains the publish related meta info
	PublishSync(message *PublisherMessage) (meta map[string]interface{}, err error)

	// PublishSyncBulk sends messages to the PubSub cluster in sync all at once. Call to this function is blocking
	// and return only after publish attempt is done for all the messages. Return error if the bulk publish is
	// partially successful
	PublishSyncBulk(messages []*PublisherMessage) error

	// PublishAsync sends a message to the PubSub cluster in Async way. Call to this function is non-blocking and
	// returns immediately.
	PublishAsync(message *PublisherMessage)

	// PublishAsyncBulk sends messages in bulk to the PubSub cluster in Async way. Call to this function is non-blocking
	// and return immediately
	PublishAsyncBulk(messages []*PublisherMessage)

	// Close triggers the closure of the associated producer client to avoid any leaks
	Close()
}
```
The package implements the Producer interface to provide a rich set of functionality to produce
messages to kafka cluster in every possible way. The abstraction clearly hides the complexity to manage **channels**, provides high throughput **async bulk publish** functionality to publish a fairly huge
amount of messages at once without blocking the main process. Though the user is advised to take care
of **CPU/Memory implications** of producing too many messages at once. Example - 

```go
producer, err := NewKafkaProducerQuick([]string{"broker-1", "broker-2", "broker-3"})
if err != nil {
	Logger.Fatalf("Error creating producer client, %v", err)
}
data, _ := json.Marshal("test message")
producer.PublishAsync(&PublisherMessage{
	Topic: "test-topic",
	Key:   "test-key",
	Data:  data,
})
```
For better understanding and implementation, refer to the coe base and test examples.

## Future work 
Package kept the **Consumer** and **Producer** interfaces fairly generic and can be implemented
for different PubSub frameworks like **Redis-PubSub**, **AWS SNS-SQS**, **Rabbit-MQ**, etc in future. The intent of this package is to make integration to Kafka like PubSub framework fairly easy with minimal efforts.
The author decided to create this package after personally _struggling_ making the integration stable and
fail-safe. The author observed that most of the developers are struggling around the same problem and end up
writing their own abstraction to make it look clean. Although most of these abstractions are similar but
mostly written considering their own repo structure under consideration. This package tried taking all such
abstraction under consideration and export fairly **reusable** code across various use cases.

The license has been chosen [![Mit License][mit-img]][mit] on purpose. Feel free to take **fork** and raise a pull request
for any possible **bugs** or **new use cases**. Always keep in mind the intent of the package to keep it fairly **simple** to understand and use.

[godoc-img]: https://godoc.org/github.com/saurav534/kafka-go?status.svg
[godoc]: https://pkg.go.dev/github.com/saurav534/kafka-go?tab=doc

[mit-img]: http://img.shields.io/badge/License-MIT-blue.svg
[mit]: https://github.com/saurav534/kafka-go/blob/master/LICENSE

[ci-img]: https://travis-ci.com/gocollection/kafka-go.svg?branch=master
[ci]: https://travis-ci.com/github/gocollection/kafka-go/branches