package kafka_go

import (
	"github.com/Shopify/sarama"
)

func NewConsumerHandler(handlers map[string]TopicHandler, fallback map[string]TopicHandler) *ConsumerHandler {
	return &ConsumerHandler{
		Ready:     make(chan bool),
		Handlers:  handlers,
		Fallbacks: fallback,
	}
}

type ConsumerHandler struct {
	Ready               chan bool
	Handlers, Fallbacks map[string]TopicHandler
}

func (ch *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(ch.Ready)
	return nil
}

func (ch *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (ch *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	messageChan := claim.Messages()
	for {
		select {
		case message := <-messageChan:
			{
				if message == nil {
					continue
				}
				if handler := ch.Handlers[message.Topic]; handler != nil {
					msg := &Message{
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
						if fbHandler := ch.Fallbacks[message.Topic]; fbHandler != nil {
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
