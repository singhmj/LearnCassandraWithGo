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
type CustomPool struct {
	cluster               *gocql.ClusterConfig
	sessions              []*gocql.Session
	poolLock              *sync.Mutex
	connectionsAllocated  int // these are atomic values
	connectionsToAllocate int // these are atomic values
}

// Init :
func (helper *CustomPool) Init(ip string, keyspace string) {
	helper.cluster = gocql.NewCluster(ip) // "127.0.0.1"
	helper.cluster.Keyspace = keyspace
	helper.cluster.Consistency = gocql.Quorum
	helper.poolLock = &sync.Mutex{}
	helper.connectionsAllocated = 0
	helper.connectionsToAllocate = 0
}

// IncreasePoolSize :
func (helper *CustomPool) IncreasePoolSize(newPoolSize int) error {
	helper.poolLock.Lock()
	defer helper.poolLock.Unlock()

	if newPoolSize < helper.connectionsAllocated {
		return errors.New("you cannot scale down the database pool")
	}
	helper.connectionsToAllocate++
	return nil
}

// GetNewSession :
func (helper *CustomPool) GetNewSession() (*gocql.Session, error) {
	helper.poolLock.Lock()
	defer helper.poolLock.Unlock()
	return CreateSession(helper.cluster)
}

// GetSessionFromPool :
func (helper *CustomPool) GetSessionFromPool() (*gocql.Session, error) {
	helper.poolLock.Lock()
	defer helper.poolLock.Unlock()

	// since this path is protected by the lock, we can increment the value
	if len(helper.sessions) == 0 {
		// already have created the required number of sessions, and they all have been exhausted now
		// can't proceed, either call IncreasePoolSize with requested params or
		// better retry in some time
		if helper.connectionsAllocated >= helper.connectionsToAllocate {
			return nil, errors.New("Pool doesn't have any session to return")
		}

		fmt.Println("Pool doesn't have any sessions in it. Going to create a new session in the pool.")
		session, err := CreateSession(helper.cluster)
		if err != nil {
			return nil, fmt.Errorf("failed to created a new session. more info: %v", err)
		}
		helper.sessions = append(helper.sessions, session)
		helper.connectionsAllocated++
	}

	session, sessions := helper.sessions[len(helper.sessions)-1], helper.sessions[:len(helper.sessions)-1]
	helper.sessions = sessions
	helper.connectionsAllocated--
	return session, nil
}

// ReturnSessionToPool :
func (helper *CustomPool) ReturnSessionToPool(session *gocql.Session) {
	helper.poolLock.Lock()
	defer helper.poolLock.Unlock()
	helper.sessions = append(helper.sessions, session)
	helper.connectionsAllocated++
}

// Connect :
func (helper *CustomPool) Connect(poolSize int) error {
	helper.poolLock.Lock()
	defer helper.poolLock.Unlock()
	helper.connectionsToAllocate = poolSize
	helper.sessions = make([]*gocql.Session, poolSize)
	for i := 0; i < poolSize; i++ {
		session, err := CreateSession(helper.cluster)
		if err != nil {
			return err
		}
		helper.sessions = append(helper.sessions, session)
		helper.connectionsAllocated--
	}
	return nil
}

// Disconnect :
// make sure that you return all of the sessions back to pool
// before calling this func
func (helper *CustomPool) Disconnect() {
	helper.poolLock.Lock()
	defer helper.poolLock.Unlock()
	for _, session := range helper.sessions {
		session.Close()
	}
}
