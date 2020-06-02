package kafka_go

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"sort"
	"testing"
	"time"
)

func TestConsumeClaim(t *testing.T) {
	handler := newConsumerHandler(map[string]TopicHandler{
		"test-topic": &testTopicHandler{},
	}, map[string]TopicHandler{}, []ConsumerMiddleware{testMiddleware1, testMiddleware2},
		[]ConsumerInterceptor{testInterceptor1, testInterceptor2}, true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx = context.WithValue(ctx, "meta", make(map[string]interface{}))
	msgChan := make(chan *sarama.ConsumerMessage)
	go func() {
		for i := 0; i < 100; i++ {
			msgChan <- &sarama.ConsumerMessage{
				Key:       []byte(fmt.Sprintf("key-%v", i)),
				Value:     []byte(fmt.Sprintf("value-%v", i)),
				Topic:     "test-topic",
				Partition: 0,
				Offset:    0,
			}
		}
		cancel()
	}()
	err := handler.ConsumeClaim(mockConsumerGroupSession(ctx), mockConsumerGroupClaim(msgChan))
	if err != nil {
		t.Errorf("unkown error %v", err)
	} else {
		meta := ctx.Value("meta").(map[string]interface{})
		if meta["msg"] == nil {
			t.Errorf("Message not received by handler")
		} else {
			messages := meta["msg"].([]string)
			if len(messages) != 100 {
				t.Errorf("Few messages missed")
			}
			expectedKeys := make([]string, 0, 100)
			for i := 0; i < 100; i++ {
				expectedKeys = append(expectedKeys, fmt.Sprintf("key-%v", i))
			}
			for i := 0; i < len(expectedKeys); i++ {
				if messages[i] != expectedKeys[i] {
					t.Errorf("extected message key %v, found event %v", expectedKeys[i], messages[i])
				}
			}
			delete(meta, "msg")
		}
		expectedEvents := []string{"mw1", "mw2", "before1", "before2", "after2", "after1"}
		if len(meta) != len(expectedEvents) {
			t.Errorf("%v event count not matched", 6-len(meta))
		} else {
			events := make([]string, 0)
			for k, _ := range meta {
				events = append(events, k)
			}
			sort.Slice(events, func(i, j int) bool {
				return meta[events[i]].(int64) < meta[events[j]].(int64)
			})

			for i := 0; i < len(expectedEvents); i++ {
				if events[i] != expectedEvents[i] {
					t.Errorf("extected event %v, found event %v", expectedEvents[i], events[i])
				}
			}
		}
	}
}
