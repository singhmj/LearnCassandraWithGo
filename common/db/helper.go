package db

import (
	"fmt"

	"github.com/gocql/gocql"
)

type Helper struct {
	cluster  *gocql.ClusterConfig
	sessions []*gocql.Session
}

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

// TODO: Add logic to fetch from pool
func (helper *Helper) GetSessionFromPool() (*gocql.Session, error) {
	if len(helper.sessions) == 0 {
		session, err := helper.createANewSession()
		if err != nil {
			return nil, err
		}
		helper.sessions = append(session, helper.sessions)
	}

	session := helper.sessions[len(helper.sessions)-1:][0]
	return session, nil
}

func (helper *Helper) ReturnSessionToPool(session) error {
	helper.sessions = append(helper.sessions, session)
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
