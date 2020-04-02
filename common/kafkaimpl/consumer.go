package kafkaimpl

import (
	"context"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

type ConsumerImpl struct {
	Config               *sarama.Config
	ConsumerGroup        *sarama.ConsumerGroup
	ConsumerGroupHandler sarama.ConsumerGroupHandler
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

	consumer.Config.Version = sarama.V0_11_0_2
	consumer.Config.Consumer.Return.Errors = true
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

func (consumer *ConsumerImpl) RegisterConsumerGroupHandler(handler sarama.ConsumerGroupHandler) {
	consumer.ConsumerGroupHandler = handler
}

func (consumer *ConsumerImpl) Consume() error {
	return (*consumer.ConsumerGroup).Consume(consumer.context, consumer.topics, consumer.ConsumerGroupHandler)
}

func (consumer *ConsumerImpl) Stop() {
	(*consumer.ConsumerGroup).Close()
}

func (consumer *ConsumerImpl) SubscribeToErrors() {
	for err := range (*consumer.ConsumerGroup).Errors() {
		fmt.Printf("an error received in consumer. More info: %v", err)
	}
}
