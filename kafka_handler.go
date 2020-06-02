package kafka_go

import (
	"context"
	"github.com/Shopify/sarama"
)

func newConsumerHandler(handlers map[string]TopicHandler, fallback map[string]TopicHandler,
	middleware []ConsumerMiddleware, interceptor []ConsumerInterceptor, meta bool) *consumerHandler {
	consumerHandler := &consumerHandler{
		ready:             make(chan bool),
		handlers:          handlers,
		fallbacks:         fallback,
		middleware:        middleware,
		middlewarePresent: middleware != nil && len(middleware) > 0,
		messageMeta:       meta,
	}
	if interceptor == nil || len(interceptor) == 0 {
		consumerHandler.interceptor = noOpInterceptor
	} else {
		chainedInterceptor := func(ctx context.Context, msg *SubscriberMessage, handler func(context.Context, *SubscriberMessage) bool) bool {
			for i := len(interceptor) - 1; i >= 0; i-- {
				currentInterceptor := interceptor[i]
				currentHandler := handler
				handler = func(ctx context.Context, msg *SubscriberMessage) bool {
					return currentInterceptor(ctx, msg, currentHandler)
				}
			}
			return handler(ctx, msg)
		}
		consumerHandler.interceptor = chainedInterceptor
	}
	return consumerHandler
}

type consumerHandler struct {
	ready               chan bool
	retryCount          int8
	handlers, fallbacks map[string]TopicHandler
	middleware          []ConsumerMiddleware
	interceptor         ConsumerInterceptor
	middlewarePresent   bool
	messageMeta         bool
}

func (ch *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	ch.retryCount = 0
	close(ch.ready)
	return nil
}

func (ch *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	ch.retryCount = 0
	return nil
}

func (ch *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	messageChan := claim.Messages()
	for {
		select {
		case message := <-messageChan:
			{
				if message == nil {
					continue
				}
				if handler := ch.handlers[message.Topic]; handler != nil {
					msg := &SubscriberMessage{
						Topic:     message.Topic,
						Partition: message.Partition,
						Offset:    message.Offset,
						Key:       message.Key,
						Value:     message.Value,
					}
					if ch.messageMeta {
						msg.Meta = make(map[string]interface{})
					}

					// invoke Middleware if present
					if ch.middlewarePresent {
						ch.invokeMiddleware(session.Context(), msg)
					}
					if ch.interceptor(session.Context(), msg, handler.Handle) {
						// successful handling
						session.MarkMessage(message, "")
					} else {
						// handling failed, trying fallback handler if any
						if fbHandler := ch.fallbacks[message.Topic]; fbHandler != nil {
							fbHandler.Handle(session.Context(), msg)
						}
						session.MarkMessage(message, "")
					}
				} else {
					Logger.Printf("No handler found for topic %v", message.Topic)
					session.MarkMessage(message, "")
				}
			}
		case <-session.Context().Done():
			{
				Logger.Println("Releasing kafka consumer handler")
				return nil
			}
		}
	}
}

func (ch *consumerHandler) invokeMiddleware(ctx context.Context, msg *SubscriberMessage) {
	for _, m := range ch.middleware {
		m(ctx, msg)
	}
}

func noOpInterceptor(ctx context.Context, msg *SubscriberMessage, handler func(context.Context, *SubscriberMessage) bool) bool {
	return handler(ctx, msg)
}
