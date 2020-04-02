/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace Blog with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
create table blog.post(date text, id UUID, author text, body text, PRIMARY KEY(id));
create index on blog.post(date);
*/
package main

import (
	"fmt"

	db "./common/db"
	kafkaHelper "./common/kafkaimpl"
)

func WaitTillAllDone(waitChannel <-chan interface{}) {
	// count := 0
	for signal := range waitChannel {
		fmt.Printf("Received a signal on WaitChannel. More info: %v", signal)
		break
	}
}

func main() {
	// connect to the cluster
	dbHelper := db.CreateNewDBHelper(DBAddress, DBKeySpace)
	consumer := kafkaHelper.CreateNewConsumer([]string{BrokerAddress}, ConsumerGroupID, []string{TopicsForConsumers})
	producer := kafkaHelper.CreateNewProducer([]string{BrokerAddress})
	go consumer.SubscribeToErrors()
	go producer.SubscribeErrors()
	waitChannel := make(chan interface{})

	dbHelper.Connect(DBPoolSize)

	eventsPublisher := CreateBlogEventsPublisher(producer)
	eventsReceiver := CreateBlogEventsReceiver(dbHelper)

	consumer.RegisterConsumerGroupHandler(eventsReceiver)

	go eventsPublisher.PublishFakeEvents(waitChannel)
	go eventsReceiver.ReceiveFakeEvents(consumer, waitChannel)

	// TODO: listen to system signals, and shutdown the system accordingly
	WaitTillAllDone(waitChannel)

	defer func() {
		producer.Stop()
		consumer.Stop()
		dbHelper.Disconnect()
	}()
}
