package kafka_go

import (
	"encoding/json"
	"testing"
)

func TestProducerBrokerRequired(t *testing.T) {
	_, err := NewKafkaProducer(DefaultKafkaProducerParam([]string{}))
	if err != nil {
		if err.Error() != "at least one broker is mandatory" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with broker list empty")
	}

	_, err = NewKafkaProducerQuick([]string{})
	if err != nil {
		if err.Error() != "at least one broker is mandatory" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with broker list nil")
	}
}

func TestProducerInvalidCompression(t *testing.T) {
	param := DefaultKafkaProducerParam([]string{"localhost:9091"})
	param.Compression = -1
	_, err := NewKafkaProducer(param)
	if err != nil {
		if err.Error() != "compression type is not valid" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with wrong compression type")
	}
}

func TestProducerInvalidAcknowledgement(t *testing.T) {
	param := DefaultKafkaProducerParam([]string{"localhost:9091"})
	param.Acknowledge = 2
	_, err := NewKafkaProducer(param)
	if err != nil {
		if err.Error() != "acknowledgement type is not valid" {
			t.Errorf("Unknown error %v", err)
		}
	} else {
		t.Errorf("No error with wrong acknowledgement type")
	}
}

func ExampleProducer() {
	param := DefaultKafkaProducerParam([]string{"broker-1", "broker-2", "broker-3"})
	param.LogError = false
	producer, err := NewKafkaProducer(param)
	if err != nil {
		Logger.Fatalf("Error creating producer client, %v", err)
	}
	data, _ := json.Marshal("test message")
	meta, err := producer.PublishSync(&PublisherMessage{
		Topic: "test-topic",
		Key:   "test-key",
		Data:  data,
	})
	if err != nil {
		Logger.Printf("Error producing message to kafka cluster, %v", err)
	} else {
		Logger.Printf("Message publishedm, meta %v", meta)
	}
}

func ExampleProducerQuick() {
	producer, err := NewKafkaProducerQuick([]string{"broker-1", "broker-2", "broker-3"})
	Logger.Printf("Producer: %v, error: %v", producer, err)
	// rest is like the example above ExampleProducer
}
