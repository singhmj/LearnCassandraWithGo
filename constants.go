package main

const (
	BrokerAddress              = "127.0.0.1:9092"
	DBAddress                  = "127.0.0.1"
	DBKeySpace                 = "blog"
	ConsumerGroupID            = "FakeEventConsumerGroup"
	TopicsForConsumers         = "BLOG-EVENTS"
	TopicToProduceFakeEventsOn = "BLOG-EVENTS"
	DBPoolSize                 = 5
)
