package kafka_go

import (
	"context"
	"github.com/Shopify/sarama"
	"time"
)

func mockConsumerGroupSession(ctx context.Context) *mockSession {
	return &mockSession{ctx: ctx}
}

func mockConsumerGroupClaim(msgChan <-chan *sarama.ConsumerMessage) *mockClaim {
	return &mockClaim{msgChan: msgChan}
}

// test topic handler
type testTopicHandler struct {
}

func (t *testTopicHandler) Handle(ctx context.Context, msg *SubscriberMessage) bool {
	Logger.Printf("Topic: %v, Partition: %v, SubscriberMessage: %v", msg.Topic, msg.Partition, string(msg.Value))
	meta := ctx.Value("meta").(map[string]interface{})
	if meta["msg"] == nil {
		meta["msg"] = make([]string, 0)
	}
	messages := meta["msg"].([]string)
	meta["msg"] = append(messages, string(msg.Key))
	return true
}

func testMiddleware1(ctx context.Context, msg *SubscriberMessage) {
	Logger.Printf("Middleware 1 triggered: %v", string(msg.Key))
	meta := ctx.Value("meta").(map[string]interface{})
	msg.Meta["mw1"] = time.Now().UnixNano()
	meta["mw1"] = msg.Meta["mw1"]
}

func testMiddleware2(ctx context.Context, msg *SubscriberMessage) {
	Logger.Printf("Middleware 2 triggered: %v", string(msg.Key))
	meta := ctx.Value("meta").(map[string]interface{})
	msg.Meta["mw2"] = time.Now().UnixNano()
	meta["mw2"] = msg.Meta["mw2"]
}

func testInterceptor1(ctx context.Context, msg *SubscriberMessage, handler func(context.Context, *SubscriberMessage) bool) (res bool) {
	Logger.Printf("Interceptor 1 before handler: %v", string(msg.Key))
	meta := ctx.Value("meta").(map[string]interface{})
	msg.Meta["before1"] = time.Now().UnixNano()
	meta["before1"] = msg.Meta["before1"]
	defer func() {
		msg.Meta["after1"] = time.Now().UnixNano()
		meta["after1"] = msg.Meta["after1"]
		Logger.Printf("Interceptor 1 after handler: %v, res: %v", string(msg.Key), res)
	}()
	res = handler(ctx, msg)
	return
}

func testInterceptor2(ctx context.Context, msg *SubscriberMessage, handler func(context.Context, *SubscriberMessage) bool) (res bool) {
	Logger.Printf("Interceptor 2 before handler: %v", string(msg.Key))
	meta := ctx.Value("meta").(map[string]interface{})
	msg.Meta["before2"] = time.Now().UnixNano()
	meta["before2"] = msg.Meta["before2"]
	defer func() {
		msg.Meta["after2"] = time.Now().UnixNano()
		meta["after2"] = msg.Meta["after2"]
		Logger.Printf("Interceptor 2 after handler: %v, res: %v", string(msg.Key), res)
	}()
	res = handler(ctx, msg)
	return
}

type mockSession struct {
	ctx context.Context
}

func (m *mockSession) Claims() map[string][]int32 {
	panic("no impl")
}

func (m *mockSession) MemberID() string {
	panic("no impl")
}

func (m *mockSession) GenerationID() int32 {
	panic("no impl")
}

func (m *mockSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	panic("no impl")
}

func (m *mockSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	panic("no impl")
}

func (m *mockSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	Logger.Printf("%v/%v/%v marked", msg.Topic, msg.Partition, msg.Offset)
}

func (m *mockSession) Context() context.Context {
	return m.ctx
}

type mockClaim struct {
	msgChan <-chan *sarama.ConsumerMessage
}

func (m *mockClaim) Topic() string {
	panic("no impl")
}

func (m *mockClaim) Partition() int32 {
	panic("no impl")
}

func (m *mockClaim) InitialOffset() int64 {
	panic("no impl")
}

func (m *mockClaim) HighWaterMarkOffset() int64 {
	panic("no impl")
}

func (m *mockClaim) Messages() <-chan *sarama.ConsumerMessage {
	return m.msgChan
}
