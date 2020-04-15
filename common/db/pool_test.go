package db

import (
	"fmt"
	"log"
	"testing"

	"github.com/gocql/gocql"
)

// drop keyspace Blog;
// create keyspace Blog with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// create table blog.query_counter(counter int, PRIMARY KEY(counter));
func cleanTable(session *gocql.Session) {
	err := session.Query("TRUNCATE blog.query_counter").Exec()
	if err != nil {
		panic(fmt.Errorf("Unable to delete data from query_counter table. More info: %v", err))
	}
}

func InitCustomPool(ip string, keyspace string) *CustomPool {
	return CreatePool("custom", 10, ip, keyspace).(*CustomPool)
}

func InitCustomPoolWithChannels(ip string, keyspace string) *CustomPoolWithChannels {
	return CreatePool("custom-with-channels", 10, ip, keyspace).(*CustomPoolWithChannels)
}

func InitStandardPool(ip string, keyspace string) *StandardPool {
	return CreatePool("standard", 10, ip, keyspace).(*StandardPool)
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

func BenchmarkSingularConnectionInSerializedSystemWithDBQuery(b *testing.B) {
	session := InitConnection("127.0.0.1", "blog")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
			log.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkGetObjectFromCustomPoolInSerializedSystemWithDBQuery(b *testing.B) {
	customPool := InitCustomPool("127.0.0.1", "blog")
	defer customPool.Disconnect()

	b.ResetTimer()
	// start benchmarking timer
	for i := 0; i < b.N; i++ {
		session, err := customPool.GetSessionFromPool()
		if err != nil {
			log.Fatal("Could not extract session from database")
		}
		if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
			log.Fatal(err)
		}
		customPool.ReturnSessionToPool(session)
	}
	// end benchmarking timer
	b.StopTimer()
}

func BenchmarkGetObjectFromCustomPoolWithChannelsInSerializedSystemWithDBQuery(b *testing.B) {
	customPoolWithChannels := InitCustomPoolWithChannels("127.0.0.1", "blog")
	defer customPoolWithChannels.Disconnect()
	// start benchmarking timer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		session, err := customPoolWithChannels.GetSessionFromPool()
		if err != nil {
			log.Fatal("Could not extract session from database")
		}
		if err := session.Query(`INSERT INTO blog.query_counter (counter) VALUES (?)`, i).Exec(); err != nil {
			log.Fatal(err)
		}
		customPoolWithChannels.ReturnSessionToPool(session)
	}
	// end benchmarking timer
	b.StopTimer()
}

func BenchmarkGetObjectFromCustomPoolInSerializedSystemWithoutDBQuery(b *testing.B) {
	customPool := InitCustomPool("127.0.0.1", "blog")
	defer customPool.Disconnect()

	b.ResetTimer()
	// start benchmarking timer
	for i := 0; i < b.N; i++ {
		for {
			session, err := customPool.GetSessionFromPool()
			if err == nil {
				customPool.ReturnSessionToPool(session)
				break
			}
		}
	}
	// end benchmarking timer
	b.StopTimer()
}

func BenchmarkGetObjectFromCustomPoolWithChannelsInSerializedSystemWithoutDBQuery(b *testing.B) {
	customPoolWithChannels := InitCustomPoolWithChannels("127.0.0.1", "blog")
	defer customPoolWithChannels.Disconnect()
	// start benchmarking timer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			session, err := customPoolWithChannels.GetSessionFromPool()
			if err == nil {
				customPoolWithChannels.ReturnSessionToPool(session)
				break
			}
		}
	}
	// end benchmarking timer
	b.StopTimer()
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
// ------------------ CONCURRENT SYSTEM -----------------
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
func BenchmarkSingularConnectionInConcurrentSystemUsingDBQuery(b *testing.B) {
	session := InitConnection("127.0.0.1", "blog")
	defer session.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var counter int
		for pb.Next() {
			if err := session.Query(`SELECT counter FROM blog.query_counter`).
				Scan(&counter); err != nil {
				log.Fatal(err)
			}
		}
	})
	b.StopTimer()
}

func BenchmarkGetObjectFromCustomPoolInConcurrentSystemUsingDBQuery(b *testing.B) {
	customPool := InitCustomPool("127.0.0.1", "blog")
	defer customPool.Disconnect()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var counter int
		for pb.Next() {
			var session *gocql.Session
			var err error
			for {
				session, err = customPool.GetSessionFromPool()
				if err == nil {
					break
				}
			}

			if err := session.Query(`SELECT counter FROM blog.query_counter`).
				Scan(&counter); err != nil {
				log.Fatal(err)
			}
			customPool.ReturnSessionToPool(session)
		}
	})
	b.StopTimer()
	// end benchmarking timer
}

func BenchmarkGetObjectFromCustomPoolWithChannelsInConcurrentSystemUsingDBQuery(b *testing.B) {
	customPoolWithChannels := InitCustomPoolWithChannels("127.0.0.1", "blog")
	defer customPoolWithChannels.Disconnect()

	// var wg sync.WaitGroup

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var counter int
		for pb.Next() {
			var session *gocql.Session
			var err error
			for {
				session, err = customPoolWithChannels.GetSessionFromPool()
				if err == nil {
					break
				}
			}

			if err := session.Query(`SELECT counter FROM blog.query_counter`).
				Scan(&counter); err != nil {
				log.Fatal(err)
			}
			customPoolWithChannels.ReturnSessionToPool(session)
		}
	})
	b.StopTimer()
	// end benchmarking timer
}

func BenchmarkGetObjectFromCustomPoolInConcurrentSystemWithoutDBQuery(b *testing.B) {
	customPool := InitCustomPool("127.0.0.1", "blog")
	defer customPool.Disconnect()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var session *gocql.Session
			var err error
			for {
				session, err = customPool.GetSessionFromPool()
				if err == nil {
					break
				}
			}
			customPool.ReturnSessionToPool(session)
		}
	})
	b.StopTimer()
	// end benchmarking timer
}

func BenchmarkGetObjectFromCustomPoolWithChannelsInConcurrentSystemWithoutDBQuery(b *testing.B) {
	customPoolWithChannels := InitCustomPoolWithChannels("127.0.0.1", "blog")
	defer customPoolWithChannels.Disconnect()

	// var wg sync.WaitGroup

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var session *gocql.Session
			var err error
			for {
				session, err = customPoolWithChannels.GetSessionFromPool()
				if err == nil {
					break
				}
			}
			customPoolWithChannels.ReturnSessionToPool(session)
		}
	})
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
