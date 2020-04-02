package main

import (
	"fmt"
	"time"

	"./common/kafkaimpl"
)

type BlogEventsPublisher struct {
	Producer *kafkaimpl.ProducerImpl
}

func CreateBlogEventsPublisher(producer *kafkaimpl.ProducerImpl) *BlogEventsPublisher {
	blogEvents := &BlogEventsPublisher{
		Producer: producer,
	}
	return blogEvents
}

func (blogEvents *BlogEventsPublisher) PublishFakeEvents(waitChannel chan<- interface{}) {
	for i := 0; i < 1000; i++ {
		fmt.Println("Producing blog events on MessageQueue")
		blogEvents.Producer.Produce(
			TopicToProduceFakeEventsOn,
			"123",
			"{'uid':123, 'author':'TheNaiveProgrammer', 'title': 'How Not To Write A Wrapper Class?', 'body': 'Pending till the class is written completely' }",
		)
		time.Sleep(1 * time.Second)

		if i == 999 {
			waitChannel <- 1
			break
		}
	}

}

func (blogEvents *BlogEventsPublisher) HandleBrokenEvents(waitChannel chan<- interface{}) {
	for err := range blogEvents.Producer.SubscribeErrors() {
		fmt.Printf("An error received in blogEvents. More info: %v", err)
		// check if message has failed, then resend it
		// or just print the error and write the handling logic at some later point
		// if a critical error is received, it'll be better to signal the main routine to initiate shutdown
	}
}
