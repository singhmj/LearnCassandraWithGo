package messages

import (
	"fmt"
	"log"

	"github.com/bxcodec/faker"
	"github.com/gocql/gocql"
)

type BlogPost struct {
	Header      Header `json:"Header"`
	BlogID      string `json:"blogID" faker:"uuid_hyphenated"`
	Title       string `json:"title" faker:"sentence"`
	Body        string `json:"body" faker:"paragraph"`
	Conclusion  string `json:"conclusion" faker:"paragraph"`
	Author      string `json:"author" faker:"name"`
	AuthorID    string `json:"authorID" faker:"uuid_hyphenated"`
	PublishedAt string `json:"publishedAt" faker:"date"`
}

func (b *BlogPost) SaveToDB(session *gocql.Session) error {
	// insert a blog post
	if err := session.Query(`INSERT INTO post (blogID, authorId, authorName, title, body, conclusion, publishedAt) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		b.BlogID, b.AuthorID, b.Author, b.Title, b.Body, b.Conclusion, b.PublishedAt).Exec(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Blog Post saved successfully to the database")
	return nil
}

// GenerateFakeBlogEvent :
func GenerateFakeBlogPost() *BlogPost {
	post := &BlogPost{}
	err := faker.FakeData(&post)
	post.Header = Header{
		MessageType: MessageTypeBlogPost,
	}
	if err != nil {
		fmt.Println("An error occured while generating fake blog post")
		return nil
	}
	fmt.Println("Generated fake post: %v", post)
	return post
}

// GetFromDB()
// var id gocql.UUID
// var date string
// var body string
// var author string

// if err := session.Query(`SELECT date, id, author, body FROM post WHERE date = ? LIMIT 1`,
// 	"2020-03-31 04:05+0000").Consistency(gocql.One).Scan(&date, &id, &author, &body); err != nil {
// 	log.Fatal(err)
// }
// fmt.Printf("Author: %v, Body: %v", author, body)
