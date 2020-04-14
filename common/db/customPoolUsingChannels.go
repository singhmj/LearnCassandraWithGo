package db

import (
	"errors"

	"github.com/gocql/gocql"
)

// CustomPoolWithChannels : This pool implementation is really naive, there's a room for a lot of improvements
type CustomPoolWithChannels struct {
	cluster         *gocql.ClusterConfig
	sessionsChannel chan *gocql.Session
}

// Init :
func (selfObject *CustomPoolWithChannels) Init(ip string, keyspace string) {
	selfObject.cluster = gocql.NewCluster(ip)
	selfObject.cluster.Keyspace = keyspace
	selfObject.cluster.Consistency = gocql.Quorum
}

// GetPoolSize :
func (selfObject *CustomPoolWithChannels) GetPoolSize() int {
	return len(selfObject.sessionsChannel)
}

// IncreasePoolSize :
func (selfObject *CustomPoolWithChannels) IncreasePoolSize(newPoolSize int) error {
	// TODO: pending
	return nil
}

// GetSessionWithoutUsingPool :
func (selfObject *CustomPoolWithChannels) GetSessionWithoutUsingPool() (*gocql.Session, error) {
	return CreateSession(selfObject.cluster)
}

// GetSessionFromPool :
func (selfObject *CustomPoolWithChannels) GetSessionFromPool() (*gocql.Session, error) {
	// if selfObject.GetPoolSize() == 0 {
	// 	return nil, errors.New("Not enough sessions in the pool")
	// }
	session, ok := <-selfObject.sessionsChannel
	if !ok {
		return nil, errors.New("No session on the channel")
	}
	return session, nil
}

// ReturnSessionToPool :
func (selfObject *CustomPoolWithChannels) ReturnSessionToPool(session *gocql.Session) {
	selfObject.sessionsChannel <- session
}

// Connect :
func (selfObject *CustomPoolWithChannels) Connect(poolSize int) error {
	selfObject.sessionsChannel = make(chan *gocql.Session, poolSize)
	for i := 0; i < poolSize; i++ {
		session, err := CreateSession(selfObject.cluster)
		if err != nil {
			return err
		}
		selfObject.sessionsChannel <- session
	}
	return nil
}

// Disconnect :
// make sure that you return all of the sessions back to pool
// before calling this func
func (selfObject *CustomPoolWithChannels) Disconnect() {
	select {
	case session, ok := <-selfObject.sessionsChannel:
		if !ok {
			break
		}
		session.Close()
	}

	close(selfObject.sessionsChannel)
}
