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
	count := 0
	for signal := range waitChannel {
		fmt.Printf("Received a signal on WaitChannel. More info: %v", signal)
		count++
		if count >= 2 {
			break
		}
	}
}

const (
	BrokerAddress      = "127.0.0.1:9095"
	DBAddress          = "127.0.0.1"
	DBKeySpace         = "Blog"
	ConsumerGroupID    = "FakeEventConsumerGroup"
	TopicsForConsumers = "FAKE_BLOGS_EVENTS"
)

func main() {
	// connect to the cluster
	dbHelper := db.CreateNewDBHelper(DBAddress, DBKeySpace)
	consumer := kafkaHelper.CreateNewConsumer([]string{BrokerAddress}, ConsumerGroupID, []string{TopicsForConsumers})
	producer := kafkaHelper.CreateNewProducer([]string{BrokerAddress})
	waitChannel := make(chan interface{})

	dbHelper.Connect(5)

	eventsPublisher := CreateBlogEventsPublisher(producer)
	// eventsReceiver := CreateBlogEventsReceiver(consumer)

	go eventsPublisher.PublishFakeEvents(waitChannel)
	// go eventsReceiver.ReceiveFakeEvents(waitChannel)
	// listen to system signals, and shutdown the system accordingly

	WaitTillAllDone(waitChannel)

	defer func() {
		producer.Stop()
		consumer.Stop()
		dbHelper.Disconnect()
	}()

	// // insert a blog post
	// if err := session.Query(`INSERT INTO post (date, id, author, body) VALUES (?, ?, ?, ?)`,
	// 	"2020-03-31 04:05+0000", gocql.TimeUUID(), "Manjinder", "Hi! This is a sample body.").Exec(); err != nil {
	// 	log.Fatal(err)
	// }

	// var id gocql.UUID
	// var date string
	// var body string
	// var author string

	// if err := session.Query(`SELECT date, id, author, body FROM post WHERE date = ? LIMIT 1`,
	// 	"2020-03-31 04:05+0000").Consistency(gocql.One).Scan(&date, &id, &author, &body); err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("Author: %v, Body: %v", author, body)
}
