package db

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gocql/gocql"
)

// StandardPool : Uses sync.Pool under the hood, it lets you create a static sized session pool
// new connections to database should be made very restrictively, so added some custom logic to keep the pool static
// it's intentional
// TODO: Add function to resize the pool if required
// TODO: Add function to query the allocated pool size
type StandardPool struct {
	cluster               *gocql.ClusterConfig
	sessionPool           *sync.Pool
	connectionsAllocated  int
	connectionsToAllocate int
}

// Init :
func (selfObject *StandardPool) Init(ip string, keyspace string) {
	cluster, err := CreateCluster(ip, keyspace)
	if err != nil {
		panic(fmt.Errorf("Failed to initialise StandardPool. More info: %v", err))
	}
	selfObject.cluster = cluster
	selfObject.sessionPool = &sync.Pool{
		New: func() interface{} {
			// if the the requried number of connections have already been allocated
			// then don't created any new connection
			// this behaviour is required where connections should be made in restrictive manner
			fmt.Printf("Creating a new session in the pool. Total ConnectionsAllocated: %v, ConnectionsToAllocate: %v", selfObject.connectionsAllocated, selfObject.connectionsToAllocate)
			if selfObject.connectionsAllocated > selfObject.connectionsToAllocate {
				return nil
			}

			session, err := CreateSession(cluster)
			if err != nil {
				return err
			}
			// REVIEW:!!!!!!!!!
			// WARNING: THIS CAN CAUSE DATA RACE
			// INSTEAD USE SOME (LOW LATENCY) ATOMIC VARIABLE
			selfObject.connectionsAllocated++
			return session
		},
	}
}

// GetPoolSize :
func (selfObject *StandardPool) GetPoolSize() int {
	return selfObject.connectionsAllocated
}

// IncreasePoolSize :
func (selfObject *StandardPool) IncreasePoolSize(newPoolSize int) error {
	if newPoolSize < selfObject.connectionsAllocated {
		return errors.New("you cannot scale down the database pool")
	}
	selfObject.connectionsToAllocate++
	return nil
}

// GetSessionWithoutUsingPool :
func (selfObject *StandardPool) GetSessionWithoutUsingPool() (*gocql.Session, error) {
	// TODO: Add lock this lock has been acquired to protect cluster
	return CreateSession(selfObject.cluster)
}

// Connect :
func (selfObject *StandardPool) Connect(poolSize int) error {
	selfObject.connectionsToAllocate = poolSize
	for i := 0; i < poolSize; i++ {
		session := selfObject.sessionPool.New()
		if session == nil {
			return errors.New("Failed to create sessions in the pool")
		}
		selfObject.ReturnSessionToPool(session.(*gocql.Session))
	}

	return nil
}

// GetSessionFromPool :
func (selfObject *StandardPool) GetSessionFromPool() (*gocql.Session, error) {
	conn := selfObject.sessionPool.Get()
	if conn != nil {
		return conn.(*gocql.Session), nil
	}
	return nil, errors.New("Failed to get session from the pool")
}

// ReturnSessionToPool :
func (selfObject *StandardPool) ReturnSessionToPool(session *gocql.Session) {
	selfObject.sessionPool.Put(session)
}

// Disconnect :
func (selfObject *StandardPool) Disconnect() {
	// get sessions from pool one by one
	for i := 0; i < selfObject.connectionsAllocated; i++ {
		session := selfObject.sessionPool.Get()
		selfObject.ReturnSessionToPool(session.(*gocql.Session))
	}
	// and call disconnect on them
	// then return them back to the pool
}
