package main

import (
	"encoding/json"
	"fmt"
	"time"

	"./common/messages"

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
		fakeEvent := messages.GenerateFakeBlogPost()
		eventInJSON, _ := json.Marshal(fakeEvent)
		blogEvents.Producer.Produce(
			TopicToProduceFakeEventsOn,
			[]byte(fakeEvent.BlogID),
			eventInJSON,
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
