package kafkaimpl

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
)

type ConsumerImpl struct {
	Config               *sarama.Config
	ConsumerGroup        *sarama.ConsumerGroup
	ConsumerGroupHandler *sarama.ConsumerGroupHandler
	Address              []string
	GroupID              string
	topics               []string
	context              context.Context
}

func CreateNewConsumer(address []string, groupID string, topics []string /* config *kafkaConfig*/) *ConsumerImpl {
	consumer := &ConsumerImpl{
		Config:  sarama.NewConfig(), // default config
		GroupID: groupID,
		Address: address,
	}

	for _, topic := range topics {
		consumer.AddTopic(topic)
	}

	consumer.Init()
	return consumer
}

func (consumer *ConsumerImpl) Init() {
	consumerGroup, err := sarama.NewConsumerGroup(consumer.Address, consumer.GroupID, consumer.Config)
	if err != nil {
		log.Fatalf("Failed to initialize consumer. More info: %v", err)
	}
	consumer.ConsumerGroup = &consumerGroup
	consumer.context = context.Background()
}

func (consumer *ConsumerImpl) AddTopic(topic string) {
	consumer.topics = append(consumer.topics, topic)
}

func (consumer *ConsumerImpl) AddConsumerGroupHandler(handler *sarama.ConsumerGroupHandler) {
	consumer.ConsumerGroupHandler = handler
}

func (consumer *ConsumerImpl) Consume() error {
	return (*consumer.ConsumerGroup).Consume(consumer.context, consumer.topics, *consumer.ConsumerGroupHandler)
}

func (consumer *ConsumerImpl) Stop() {
	(*consumer.ConsumerGroup).Close()
}
