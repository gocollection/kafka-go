package kafka_go

import (
	"context"
)

/*
Consumer is the exposed functionality available to the end customer to interact with its
consumer instance. The interface is implemented by kafkaConsumer in the package. The interface
is generic enough can be used with any other pubsub services.
*/
type Consumer interface {
	//Start should trigger the actual message consumption process, it should be blocking in nature to avoid killing
	//process immaturely.
	Start(ctx context.Context)

	//Stop should trigger the closure of consumption process. Should cancel the context to relieve resources and take
	//care of possible leaks
	Stop()
}

/*
SubscriberMessage instance will be received by configured topic handler.
Contains data required in standard use cases.
*/
type SubscriberMessage struct {
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

	// any state to carry with message
	Meta map[string]interface{}
}

/*
TopicHandler should be implemented by the user to consume message from a topic.
SubscriberMessage received from a topic forwarded to once of such handlers to take care of
the business logic required.
*/
type TopicHandler interface {
	// Handle gets the actual message to be handled. A business logic for a given
	// message should ideally be implemented here.
	Handle(ctx context.Context, message *SubscriberMessage) bool
}

/*
PublisherMessage instance should be used to publish data on a topic.
*/
type PublisherMessage struct {
	// Topic on which message is to be published
	Topic string

	// Partition key to decide partition
	Key string

	// Actual data to be published
	Data []byte
}

/*
Producer is the exposed functionality to the end customer to interact with the producer instance.
The interface is implemented by kafkaConsumer in the package
*/
type Producer interface {
	// PublishSync send message to the pubsub cluster in Sync way. Call to this function is blocking and
	// returns only after the publish is done or result in an error. Meta contains the publish related meta info
	PublishSync(message *PublisherMessage) (meta map[string]interface{}, err error)

	// PublishSyncBulk send messages to the pubsub cluster in sync all at once. Call to this function is blocking
	// and return only after publish attempt is done for all the messages. Return error if the bulk publish is
	// partially successful
	PublishSyncBulk(messages []*PublisherMessage) error

	// PublishAsync send message to the pubsub cluster in Async way. Call to this function is non blocking and
	// returns immediately.
	PublishAsync(message *PublisherMessage)

	// PublishAsyncBulk send messages in bulk to the pubsub cluster in Async way. Call to this function is non blocking
	// and return immediately
	PublishAsyncBulk(messages []*PublisherMessage)

	// Close triggers the closure of the associated producer client to avoid any leaks
	Close()
}

/*
Consumer functional middleware to be used to touch message before it get passed to the actual message handler.
Its similar to Before advice in AOP. Middleware can also set some sort of message state that can be retrieved
and used later at the time of message handling. Can be thought of as pre handler across all topics and can be used
to decorate message before passing it to the handler.

see SubscriberMessage.Meta

middleware : MW

msg => MW_0 => MW_1 => ...... => MW_n => [msg_handler]

*/
type ConsumerMiddleware func(ctx context.Context, msg *SubscriberMessage)

/*
Interceptor is construct similar to Around advice in AOP. An interceptor will be able to not only touch the message
or execute something before message being passed to the handler, but also get to do the needful post the handler returns.

B : task before handler
A : task after handler
interceptor : IC

msg => IC_0 => {B_0 -> IC_1 => {{B_1 -> .... ->IC_n => {..{B_n -> [msg_handler] -> A_n}..} -> .... -> A_1}} -> A_0}
*/
type ConsumerInterceptor func(ctx context.Context, msg *SubscriberMessage, handler func(context.Context, *SubscriberMessage) bool) bool
