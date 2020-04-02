package main

import (
	"fmt"

	"github.com/bxcodec/faker"
)

type BlogEvent struct {
	BlogID      string `json:"blogID" faker:"uuid_hyphenated"`
	Title       string `json:"title" faker:"sentence"`
	Body        string `json:"body" faker:"paragraph"`
	Conclusion  string `json:"conclusion" faker:"paragraph"`
	Author      string `json:"author" faker:"name"`
	AuthorID    string `json:"authorID" faker:"uuid_hyphenated"`
	PublishedAt string `json:"publishedAt" faker:"date"`
}

// GenerateFakeBlogEvent :
func GenerateFakeBlogEvent() *BlogEvent {
	event := &BlogEvent{}
	err := faker.FakeData(&event)
	if err != nil {
		fmt.Println("An error occured while generating fake blog event")
		return nil
	}
	fmt.Println("Generated fake event: %v", event)
	return event
}
