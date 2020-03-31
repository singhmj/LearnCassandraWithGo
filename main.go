/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace Blog with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
create table blog.post(date text, id UUID, author text, body text, PRIMARY KEY(id));
create index on blog.post(date);
*/
package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

func main() {
	// connect to the cluster
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "blog"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()

	// insert a blog post
	if err := session.Query(`INSERT INTO post (date, id, author, body) VALUES (?, ?, ?, ?)`,
		"2020-03-31 04:05+0000", gocql.TimeUUID(), "Manjinder", "Hi! This is a sample body.").Exec(); err != nil {
		log.Fatal(err)
	}

	var id gocql.UUID
	var date string
	var body string
	var author string

	if err := session.Query(`SELECT date, id, author, body FROM post WHERE date = ? LIMIT 1`,
		"2020-03-31 04:05+0000").Consistency(gocql.One).Scan(&date, &id, &author, &body); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Author: %v, Body: %v", author, body)
}
