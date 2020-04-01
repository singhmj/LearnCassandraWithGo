package main

import (
	"fmt"
	"time"

	"./common/kafkaimpl"

	"github.com/Shopify/sarama"
)

type BlogEventsReceiver struct {
	sarama.ConsumerGroupHandler
	Receiver *kafkaimpl.ConsumerImpl
}

func CreateBlogEventsReceiver(receiver *kafkaimpl.ConsumerImpl) *BlogEventsReceiver {
	blogEvents := &BlogEventsReceiver{
		Receiver: receiver,
	}
	return blogEvents
}

func (blogEvents *BlogEventsReceiver) ReceiveFakeEvents() {
	for {
		blogEvents.Receiver.Consume()
		time.Sleep(1000)
	}
}

func (BlogEventsReceiver) Setup(session *sarama.ConsumerGroupSession) error {
	fmt.Println("BLOG EVENTS RECEIVER SETUP CALLED")
	return nil
}

func (BlogEventsReceiver) Cleanup(session *sarama.ConsumerGroupSession) error {
	fmt.Println("BLOG EVENTS RECEIVER CLEANUP CALLED")
	return nil
}

func (BlogEventsReceiver) ConsumeClaim(session *sarama.ConsumerGroupSession, claim *sarama.ConsumerGroupClaim) error {
	fmt.Println("BLOG EVENTS RECEIVER CONSUMER CLAIM CALLED")

	// add database calls here, later on think of moving it somewhere outside
	return nil
}
