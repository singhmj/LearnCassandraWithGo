package main

import (
	"common/kafkaImpl"
	"time"
)

type BlogEventsPublisher struct {
	Producer *kafkaImpl.Producer
}

func CreateBlogEventsPublisher(producer *kafkaImpl.Consumer) *BlogEventsPublisher {
	blogEvents := &BlogEventsPublisher{
		Producer: producer,
	}
	return blogEvents
}

func (blogEvents *BlogEventsPublisher) PublishFakeEvents(waitChannel chan<- interface{}) {
	for i := 0; i < 1000; i++ {
		blogEvents.Producer.Produce()
		time.Sleep(10000)

		if i == 999 {
			waitChannel <- 1
			break
		}
	}

}

func (blogEvents *BlogEventsPublisher) HandleBrokenEvents(waitChannel chan<- interface{}) {
	for err := range blogEvents.Producer.SubscribeErrorsChannel() {
		// check if message has failed, then resend it
		// or just print the error and write the handling logic at some later point
		// if a critical error is received, it'll be better to signal the main routine to initiate shutdown
	}
}
