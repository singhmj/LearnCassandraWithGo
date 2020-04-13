package db

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/gocql/gocql"
)

var pool *Helper
var session *gocql.Session

// create table blog.query_counter(counter int, PRIMARY KEY(counter));
func cleanTable(session *gocql.Session) {
	err := session.Query("TRUNCATE blog.query_counter").Exec()
	if err != nil {
		panic(fmt.Errorf("Unable to delete data from query_counter table. More info: %v", err))
	}
}

func InitPool(ip string, keyspace string) {
	pool = CreateNewDBHelper(ip, keyspace)
	err := pool.Connect(100)
	if err != nil {
		log.Fatal("Failed to connect to sessions. Moreinfo: ", err)
	}
}

func InitConnection(ip string, keyspace string) {
	cluster := gocql.NewCluster(ip) // "127.0.0.1"
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	localSession, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create the session. More info: %v", err)
	}

	session = localSession
}

func BenchmarkGetObjectFromPoolInSerializedSystem(b *testing.B) {
	InitPool("127.0.0.1", "blog")
	session, _ := pool.GetSessionFromPool()
	cleanTable(session)
	pool.ReturnSessionToPool(session)

	// start benchmarking timer
	for i := 0; i < b.N; i++ {
		session, err := pool.GetSessionFromPool()
		if err != nil {
			log.Fatal("Could not extract session from database")
		}
		if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
			log.Fatal(err)
		}
		pool.ReturnSessionToPool(session)
	}
	// end benchmarking timer
}

func BenchmarkSingularConnectionInSerializedSystem(b *testing.B) {
	InitConnection("127.0.0.1", "blog")
	cleanTable(session)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		// lock before using the session
		// use the session by executing a simple query
		if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
			log.Fatal(err)
		}
		// fmt.Println("New session: ", individualSession)
		// unlock after using the session
	}
	b.StopTimer()
}

func BenchmarkGetObjectFromPoolInConcurrentSystem(b *testing.B) {
	InitPool("127.0.0.1", "blog")
	session, _ := pool.GetSessionFromPool()
	cleanTable(session)
	pool.ReturnSessionToPool(session)

	var wg sync.WaitGroup

	b.StartTimer()
	// start benchmarking timer
	for i := 0; i < b.N; i++ {
		session, err := pool.GetSessionFromPool()
		if err != nil {
			log.Fatal("Could not extract session from database")
		}
		wg.Add(1)
		go func() {
			if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
				log.Fatal(err)
			}
			pool.ReturnSessionToPool(session)
			wg.Done()
		}()
	}

	wg.Wait()
	b.StopTimer()
	// end benchmarking timer
}

func BenchmarkSingularConnectionInConcurrentSystem(b *testing.B) {
	InitConnection("127.0.0.1", "blog")
	cleanTable(session)

	var wg sync.WaitGroup

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		// lock before using the session
		// use the session by executing a simple query
		go func() {
			if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
				log.Fatal(err)
			}
			// fmt.Println("New session: ", individualSession)
			// unlock after using the session
			wg.Done()
		}()
	}

	wg.Wait()
	b.StopTimer()
}
