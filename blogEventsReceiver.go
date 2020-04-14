package main

import (
	// "encoding/json"
	"encoding/json"
	"fmt"
	"time"

	"./common/db"
	"./common/kafkaimpl"

	"./common/messages"
	"github.com/Shopify/sarama"
)

type BlogEventsReceiver struct {
	DbHelper *db.CustomPool
}

// CreateBlogEventsReceiver :
func CreateBlogEventsReceiver(dbHelper *db.CustomPool) *BlogEventsReceiver {
	blogEvents := &BlogEventsReceiver{
		// Receiver: receiver,
		DbHelper: dbHelper,
	}
	return blogEvents
}

// ReceiveFakeEvents :
func (b *BlogEventsReceiver) ReceiveFakeEvents(consumer *kafkaimpl.ConsumerImpl, waitChannel <-chan interface{}) {
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

// Setup :
func (BlogEventsReceiver) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("BLOG EVENTS REBALANCE SETUP CALLBACK RECEIVED More info: %v", session)
	return nil
}

// Cleanup :
func (BlogEventsReceiver) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("BLOG EVENTS SESSION CLEANUP CALLBACK RECEIVED. More info: %v", session)
	return nil
}

// ConsumeClaim :
func (b BlogEventsReceiver) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("BLOG EVENTS RECEIVER CONSUMER CLAIM CALLED")
	for {
		select {
		case cMsg := <-claim.Messages():
			fmt.Printf("Received a new message. on topic: %v, partition: %v, and offset: %v \n", cMsg.Topic, cMsg.Partition, cMsg.Offset)
			if err := b.onNewMessage(cMsg); err != nil {
				// commit offset on kafka
				session.MarkMessage(cMsg, "")
			}
			// else retry?
		case <-session.Context().Done():
			fmt.Printf("Received context done")
			return nil
		}
	}
}

// onNewBlogPost :
func (b BlogEventsReceiver) onNewBlogPost(post *messages.BlogPost) error {
	fmt.Println("Received a new blog event. Processing it...")
	dbSession, err := b.DbHelper.GetSessionWithoutUsingPool()
	if err != nil {
		return err
	}
	err = post.SaveToDB(dbSession)
	return err
}

// onNewBlogComment :
func (b BlogEventsReceiver) onNewBlogComment() error {
	// TODO:
	return nil
}

// onNewMessage :
func (b BlogEventsReceiver) onNewMessage(msg *sarama.ConsumerMessage) error {
	var header messages.Header
	err := json.Unmarshal(msg.Value, &header)
	if err != nil {
		fmt.Printf("An error encountered while unmarshalling a message header received on topic: %s, partition: %d, and offset: %d. More info: %s", msg.Topic, msg.Partition, msg.Offset, err)
	} else {
		fmt.Println("Message Header: ", header)
		switch header.MessageType {
		case messages.MessageTypeBlogPost:
			{
				// unmarshal the message
				var blogPost messages.BlogPost
				err = json.Unmarshal(msg.Value, &blogPost)
				if err != nil {
					fmt.Printf("An error encountered while unmarshalling a blog event received on topic: %v, partition: %v, and offset: %v. More info: %v", msg.Topic, msg.Partition, msg.Offset, err)
				} else {
					err = b.onNewBlogPost(&blogPost)
				}
			}

		case messages.MessageTypeBlogComment:
			{
				// and save the event in database
				err = b.onNewBlogComment()
			}

		default:
			{
				{
					// unmarshal the message
					var blogPost messages.BlogPost
					err = json.Unmarshal(msg.Value, &blogPost)
					if err != nil {
						fmt.Printf("An error encountered while unmarshalling a blog event received on topic: %v, partition: %v, and offset: %v. More info: %v", msg.Topic, msg.Partition, msg.Offset, err)
					} else {
						err = b.onNewBlogPost(&blogPost)
					}
				}
			}
		}
	}

	return err
}
