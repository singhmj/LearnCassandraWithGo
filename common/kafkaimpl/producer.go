package kafkaimpl

import (
	"context"

	"github.com/Shopify/sarama"
)

type ProducerImpl struct {
	Producer *sarama.AsyncProducer
	Config   *sarama.Config
	Address  []string
	GroupID  string
	topics   []string
	context  context.Context
}

func CreateNewProducer(address []string /* config *kafkaConfig*/) *ProducerImpl {
	producerImpl := &ProducerImpl{
		Config:  sarama.NewConfig(),
		Address: address,
	}

	producer, err := sarama.NewAsyncProducer(producerImpl.Address, producerImpl.Config)
	if err != nil {
		panic(err)
	}
	producerImpl.Producer = &producer
	return producerImpl
}

func (producerImpl *ProducerImpl) SubscribeErrors() <-chan *sarama.ProducerError {
	return (*producerImpl.Producer).Errors()
}

func (producerImpl *ProducerImpl) Produce(topic string, key string, value string) {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	(*producerImpl.Producer).Input() <- message
}

func (producerImpl *ProducerImpl) Stop() {
	(*producerImpl.Producer).Close()
}
