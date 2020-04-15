package db

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/gocql/gocql"
)

// create table blog.query_counter(counter int, PRIMARY KEY(counter));
func cleanTable(session *gocql.Session) {
	err := session.Query("TRUNCATE blog.query_counter").Exec()
	if err != nil {
		panic(fmt.Errorf("Unable to delete data from query_counter table. More info: %v", err))
	}
}

func InitCustomPool(ip string, keyspace string) *CustomPool {
	return CreatePool("custom", 100, ip, keyspace).(*CustomPool)
}

func InitCustomPoolWithChannels(ip string, keyspace string) *CustomPoolWithChannels {
	return CreatePool("custom-with-channels", 100, ip, keyspace).(*CustomPoolWithChannels)
}

func InitStandardPool(ip string, keyspace string) *StandardPool {
	return CreatePool("standard", 100, ip, keyspace).(*StandardPool)
}

func InitConnection(ip string, keyspace string) *gocql.Session {
	cluster := gocql.NewCluster(ip) // "127.0.0.1"
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create the session. More info: %v", err)
	}

	return session
}

func BenchmarkSingularConnectionInSerializedSystem(b *testing.B) {
	session := InitConnection("127.0.0.1", "blog")
	cleanTable(session)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		// lock before using the session
		// use the session by executing a simple query
		// if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
		// 	log.Fatal(err)
		// }
		// fmt.Println("New session: ", individualSession)
		// unlock after using the session
	}
	b.StopTimer()
}

func BenchmarkGetObjectFromCustomPoolInSerializedSystem(b *testing.B) {
	customPool := InitCustomPool("127.0.0.1", "blog")
	defer customPool.Disconnect()
	session, _ := customPool.GetSessionFromPool()
	cleanTable(session)
	customPool.ReturnSessionToPool(session)

	b.StartTimer()

	// start benchmarking timer
	for i := 0; i < b.N; i++ {
		session, err := customPool.GetSessionFromPool()
		if err != nil {
			log.Fatal("Could not extract session from database")
		}
		// if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
		// 	log.Fatal(err)
		// }
		customPool.ReturnSessionToPool(session)
	}
	// end benchmarking timer
	b.StopTimer()
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
// ------------------ CONCURRENT SYSTEM -----------------
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
func BenchmarkSingularConnectionInConcurrentSystem(b *testing.B) {
	session := InitConnection("127.0.0.1", "blog")
	defer session.Close()
	cleanTable(session)

	var wg sync.WaitGroup

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		// lock before using the session
		// use the session by executing a simple query
		go func() {
			defer wg.Done()
			if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
				log.Fatal(err)
			}
			// fmt.Println("New session: ", individualSession)
			// unlock after using the session
		}()
	}

	wg.Wait()
	b.StopTimer()
}

func BenchmarkGetObjectFromCustomPoolInConcurrentSystem(b *testing.B) {
	customPool := InitCustomPool("127.0.0.1", "blog")
	defer customPool.Disconnect()
	session, _ := customPool.GetSessionFromPool()
	cleanTable(session)
	customPool.ReturnSessionToPool(session)

	var wg sync.WaitGroup

	b.StartTimer()
	// start benchmarking timer
	for i := 0; i < b.N; i++ {
		session, err := customPool.GetSessionFromPool()
		if err != nil {
			b.Error("Could not extract session from database")
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
			// 	b.Error(err)
			// }
			customPool.ReturnSessionToPool(session)
		}()
	}

	wg.Wait()
	b.StopTimer()
	// end benchmarking timer
}

func BenchmarkGetObjectFromCustomPoolWithChannelsInConcurrentSystem(b *testing.B) {
	customPoolWithChannels := InitCustomPoolWithChannels("127.0.0.1", "blog")
	defer customPoolWithChannels.Disconnect()
	session, _ := customPoolWithChannels.GetSessionFromPool()
	cleanTable(session)
	customPoolWithChannels.ReturnSessionToPool(session)

	var wg sync.WaitGroup

	b.StartTimer()
	// start benchmarking timer
	for i := 0; i < b.N; i++ {
		// fmt.Printf("Spawning new go routine. Number: %d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// fmt.Println("something in here")
			session, err := customPoolWithChannels.GetSessionFromPool()
			// fmt.Println("Pool size after getting session from pool: ", customPoolWithChannels.GetPoolSize())
			if err != nil {
				b.Error("Db Session failed")
			}
			// if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
			// 	b.Error(err)
			// }
			customPoolWithChannels.ReturnSessionToPool(session)
		}()
	}

	wg.Wait()
	b.StopTimer()
	// end benchmarking timer
}

// func BenchmarkGetObjectFromStandardPoolInSerializedSystem(b *testing.B) {
// 	standardPool := InitStandardPool("127.0.0.1", "blog")
// 	defer standardPool.Disconnect()
// 	session, _ := standardPool.GetSessionFromPool()
// 	cleanTable(session)
// 	standardPool.ReturnSessionToPool(session)

// 	b.StartTimer()
// 	// start benchmarking timer
// 	for i := 0; i < b.N; i++ {
// 		session, err := standardPool.GetSessionFromPool()
// 		if err != nil {
// 			log.Fatal("Could not extract session from database")
// 		}
// 		// if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
// 		// 	log.Fatal(err)
// 		// }
// 		standardPool.ReturnSessionToPool(session)
// 	}
// 	// end benchmarking timer
// 	b.StopTimer()
// }

// func BenchmarkGetObjectFromStandardPoolInConcurrentSystem(b *testing.B) {
// 	standardPool := InitStandardPool("127.0.0.1", "blog")
// 	defer standardPool.Disconnect()
// 	session, _ := standardPool.GetSessionFromPool()
// 	cleanTable(session)
// 	standardPool.ReturnSessionToPool(session)

// 	var wg sync.WaitGroup

// 	b.StartTimer()
// 	// start benchmarking timer
// 	for i := 0; i < b.N; i++ {
// 		session, err := standardPool.GetSessionFromPool()
// 		if err != nil {
// 			b.Error("Could not extract session from database")
// 		}
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			// if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
// 			// 	b.Error(err)
// 			// }
// 			standardPool.ReturnSessionToPool(session)
// 		}()
// 	}

// 	wg.Wait()
// 	b.StopTimer()
// 	// end benchmarking timer
// }
