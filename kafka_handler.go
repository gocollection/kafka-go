package kafka_go

import (
	"github.com/Shopify/sarama"
)

func newConsumerHandler(handlers map[string]TopicHandler, fallback map[string]TopicHandler) *consumerHandler {
	return &consumerHandler{
		ready:     make(chan bool),
		handlers:  handlers,
		fallbacks: fallback,
	}
}

type consumerHandler struct {
	ready               chan bool
	handlers, fallbacks map[string]TopicHandler
}

func (ch *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(ch.ready)
	return nil
}

func (ch *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
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
					if handler.Handle(session.Context(), msg) {
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
