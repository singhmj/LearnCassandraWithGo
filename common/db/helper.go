package db

import (
	"fmt"
	"sync"

	"github.com/gocql/gocql"
)

type Helper struct {
	cluster  *gocql.ClusterConfig
	sessions []*gocql.Session
	poolLock sync.Mutex
}

// This pool implementation isn't good, it keeps on creating new sessions if gets short of them
func CreateNewDBHelper(ip string, keyspace string /*Consistency*/) *Helper {
	helper := &Helper{}
	helper.Init(ip, keyspace)
	return helper
}

func (helper *Helper) Init(ip string, keyspace string) {
	helper.cluster = gocql.NewCluster(ip) // "127.0.0.1"
	helper.cluster.Keyspace = keyspace
	helper.cluster.Consistency = gocql.Quorum
}

// TODO: This just directly returns a new session, and you are responsible to manage it
func (helper *Helper) GetNewSession() (*gocql.Session, error) {
	return helper.createANewSession()
}

// TODO: Add logic to fetch from pool
func (helper *Helper) GetSessionFromPool() (*gocql.Session, error) {
	helper.poolLock.Lock()

	if len(helper.sessions) == 0 {
		fmt.Println("Pool doesn't have any sessions in it. Going to create a new session in the pool.")
		session, err := helper.createANewSession()
		if err != nil {
			return nil, err
		}
		helper.sessions = append(helper.sessions, session)
	}

	session, sessions := helper.sessions[len(helper.sessions)-1], helper.sessions[:len(helper.sessions)-1]
	helper.sessions = sessions
	defer func() { helper.poolLock.Unlock() }()
	return session, nil
}

func (helper *Helper) ReturnSessionToPool(session *gocql.Session) {
	helper.poolLock.Lock()
	helper.sessions = append(helper.sessions, session)
	defer func() { helper.poolLock.Unlock() }()
}

func (helper *Helper) Connect(poolSize uint8) error {
	helper.sessions = make([]*gocql.Session, poolSize)
	for i := uint8(0); i < poolSize; i++ {
		session, err := helper.createANewSession()
		if err != nil {
			return err
		}
		helper.sessions = append(helper.sessions, session)
	}

	return nil
}

// make sure that you return all of the sessions back to pool
// before calling this func
func (helper *Helper) Disconnect() {
	for _, session := range helper.sessions {
		session.Close()
	}
}

func (helper *Helper) createANewSession() (*gocql.Session, error) {
	session, err := helper.cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("An error encountered while creating a new session. More info: %v", err)
	}
	return session, nil
}
