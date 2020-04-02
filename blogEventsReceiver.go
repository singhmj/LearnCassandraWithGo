package main

import (
	// "encoding/json"
	"fmt"
	"time"

	"./common/kafkaimpl"

	"github.com/Shopify/sarama"
)

type BlogEventsReceiver struct {
}

func CreateBlogEventsReceiver() *BlogEventsReceiver {
	blogEvents := &BlogEventsReceiver{
		// Receiver: receiver,
	}
	return blogEvents
}

func (blogEvents *BlogEventsReceiver) ReceiveFakeEvents(consumer *kafkaimpl.ConsumerImpl, waitChannel <-chan interface{}) {
	for {
		fmt.Printf("Started consuming...")
		err := consumer.Consume()
		if err != nil {
			fmt.Printf("An error received in consume method: %v", err)
		} else {
			fmt.Printf("A blog event has been received")
		}
		time.Sleep(1 * time.Second)
	}
}

func (BlogEventsReceiver) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("BLOG EVENTS REBALANCE SETUP CALLBACK RECEIVED More info: %v", session)
	return nil
}

func (BlogEventsReceiver) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("BLOG EVENTS SESSION CLEANUP CALLBACK RECEIVED. More info: %v", session)
	return nil
}

func (b BlogEventsReceiver) OnNewBlogEvent(msg *sarama.ConsumerMessage) {
	// TODO:
	// umarshal this message
	// and save the event in database
}

func (b BlogEventsReceiver) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("BLOG EVENTS RECEIVER CONSUMER CLAIM CALLED")
	// var msg SyncMessage
	for {
		select {
		case cMsg := <-claim.Messages():
			fmt.Printf("Received claim message: %v", cMsg)
			b.OnNewBlogEvent(cMsg)
			// do something
			session.MarkMessage(cMsg, "")
		case <-session.Context().Done():
			fmt.Printf("Received context done")
			return nil
		}
	}
}
