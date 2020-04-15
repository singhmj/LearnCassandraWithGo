package db

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gocql/gocql"
)

// CustomPool : This pool implementation is really naive, there's a room for a lot of improvements
// TODO: Locking consumes too much of cpu cycles, try finding some good way of doing it
// perhaps look into the mutex implementation and use spin locks, if required
// and try finding some data structure that would consume less operations and space than ops on a slice
// TODO: Profiling
type CustomPool struct {
	cluster               *gocql.ClusterConfig
	sessions              []*gocql.Session
	poolLock              *sync.Mutex
	connectionsAllocated  int
	connectionsToAllocate int
}

// Init :
func (selfObject *CustomPool) Init(ip string, keyspace string) {
	selfObject.cluster = gocql.NewCluster(ip)
	selfObject.cluster.Keyspace = keyspace
	selfObject.cluster.Consistency = gocql.Quorum
	selfObject.poolLock = &sync.Mutex{}
	selfObject.connectionsAllocated = 0
	selfObject.connectionsToAllocate = 0
}

// GetPoolSize :
func (selfObject *CustomPool) GetPoolSize() int {
	selfObject.poolLock.Lock()
	defer selfObject.poolLock.Unlock()
	return selfObject.connectionsToAllocate
}

// IncreasePoolSize :
func (selfObject *CustomPool) IncreasePoolSize(newPoolSize int) error {
	selfObject.poolLock.Lock()
	defer selfObject.poolLock.Unlock()

	if newPoolSize < selfObject.connectionsAllocated {
		return errors.New("you cannot scale down the database pool")
	}
	selfObject.connectionsToAllocate++
	return nil
}

// GetSessionWithoutUsingPool :
func (selfObject *CustomPool) GetSessionWithoutUsingPool() (*gocql.Session, error) {
	// this lock has been acquired to protect cluster
	selfObject.poolLock.Lock()
	defer selfObject.poolLock.Unlock()
	return CreateSession(selfObject.cluster)
}

// GetSessionFromPool :
func (selfObject *CustomPool) GetSessionFromPool() (*gocql.Session, error) {
	selfObject.poolLock.Lock()
	defer selfObject.poolLock.Unlock()

	// since this path is protected by the lock, we can increment the value
	if len(selfObject.sessions) == 0 {
		// already have created the required number of sessions, and they all have been exhausted now
		// can't proceed, either call IncreasePoolSize with requested params or
		// better retry in some time
		if selfObject.connectionsAllocated >= selfObject.connectionsToAllocate {
			return nil, errors.New("Pool doesn't have any session to return")
		}

		// fmt.Println("Pool doesn't have any sessions in it. Going to create a new session in the pool.")
		session, err := CreateSession(selfObject.cluster)
		if err != nil {
			return nil, fmt.Errorf("failed to created a new session. more info: %v", err)
		}
		selfObject.sessions = append(selfObject.sessions, session)
		selfObject.connectionsAllocated++
	}

	session, sessions := selfObject.sessions[len(selfObject.sessions)-1], selfObject.sessions[:len(selfObject.sessions)-1]
	selfObject.sessions = sessions
	selfObject.connectionsAllocated--
	return session, nil
}

// ReturnSessionToPool :
func (selfObject *CustomPool) ReturnSessionToPool(session *gocql.Session) {
	selfObject.poolLock.Lock()
	defer selfObject.poolLock.Unlock()
	selfObject.sessions = append(selfObject.sessions, session)
	selfObject.connectionsAllocated++
}

// Connect :
func (selfObject *CustomPool) Connect(poolSize int) error {
	selfObject.poolLock.Lock()
	defer selfObject.poolLock.Unlock()
	selfObject.connectionsToAllocate = poolSize
	selfObject.sessions = make([]*gocql.Session, poolSize)
	for i := 0; i < poolSize; i++ {
		session, err := CreateSession(selfObject.cluster)
		if err != nil {
			return err
		}
		selfObject.sessions[i] = session
		selfObject.connectionsAllocated++
	}
	return nil
}

// Disconnect :
// make sure that you return all of the sessions back to pool
// before calling this func
func (selfObject *CustomPool) Disconnect() {
	selfObject.poolLock.Lock()
	defer selfObject.poolLock.Unlock()
	for _, session := range selfObject.sessions {
		session.Close()
	}
}
