package db

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gocql/gocql"
)

// This pool implementation is really naive, there's a room for a lot of improvements
type CustomPool struct {
	cluster  *gocql.ClusterConfig
	sessions []*gocql.Session
	poolLock *sync.Mutex
}

// Init :
func (helper *CustomPool) Init(ip string, keyspace string) {
	helper.cluster = gocql.NewCluster(ip) // "127.0.0.1"
	helper.cluster.Keyspace = keyspace
	helper.cluster.Consistency = gocql.Quorum
	helper.poolLock = &sync.Mutex{}
}

// GetNewSession :
func (helper *CustomPool) GetNewSession() (*gocql.Session, error) {
	return CreateSession(helper.cluster)
}

// GetSessionFromPool :
func (helper *CustomPool) GetSessionFromPool() (*gocql.Session, error) {
	if helper.poolLock == nil {
		fmt.Println("The pool lock is an empty object")
		return nil, errors.New("jasdjasnjd")
	}
	helper.poolLock.Lock()
	defer helper.poolLock.Unlock()

	if len(helper.sessions) == 0 {
		fmt.Println("Pool doesn't have any sessions in it. Going to create a new session in the pool.")
		session, err := CreateSession(helper.cluster)
		if err != nil {
			return nil, err
		}
		helper.sessions = append(helper.sessions, session)
	}

	session, sessions := helper.sessions[len(helper.sessions)-1], helper.sessions[:len(helper.sessions)-1]
	helper.sessions = sessions
	return session, nil
}

// ReturnSessionToPool :
func (helper *CustomPool) ReturnSessionToPool(session *gocql.Session) {
	helper.poolLock.Lock()
	defer helper.poolLock.Unlock()
	helper.sessions = append(helper.sessions, session)
}

// Connect :
func (helper *CustomPool) Connect(poolSize int) error {
	helper.sessions = make([]*gocql.Session, poolSize)
	for i := 0; i < poolSize; i++ {
		session, err := CreateSession(helper.cluster)
		if err != nil {
			return err
		}
		helper.sessions = append(helper.sessions, session)
	}

	return nil
}

// Disconnect :
// make sure that you return all of the sessions back to pool
// before calling this func
func (helper *CustomPool) Disconnect() {
	for _, session := range helper.sessions {
		session.Close()
	}
}
