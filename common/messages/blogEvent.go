package messages

import (
	"fmt"

	"github.com/bxcodec/faker"
	"github.com/gocql/gocql"
)

type BlogPost struct {
	Header      Header
	BlogID      string `json:"blogID" faker:"uuid_hyphenated"`
	Title       string `json:"title" faker:"sentence"`
	Body        string `json:"body" faker:"paragraph"`
	Conclusion  string `json:"conclusion" faker:"paragraph"`
	Author      string `json:"author" faker:"name"`
	AuthorID    string `json:"authorID" faker:"uuid_hyphenated"`
	PublishedAt string `json:"publishedAt" faker:"date"`
}

func (*BlogPost) SaveToDB(dbSession *gocql.Session) error {
	return nil
}

// GenerateFakeBlogEvent :
func GenerateFakeBlogPost() *BlogPost {
	post := &BlogPost{}
	post.Header.MessageType = MessageTypeBlogPost
	err := faker.FakeData(&post)
	if err != nil {
		fmt.Println("An error occured while generating fake blog post")
		return nil
	}
	fmt.Println("Generated fake post: %v", post)
	return post
}
