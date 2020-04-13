package db

import (
	"fmt"

	"github.com/gocql/gocql"
)

func CreateCluster(ip string, keyspace string) (*gocql.ClusterConfig, error) {
	var cluster *gocql.ClusterConfig
	cluster = gocql.NewCluster(ip) // "127.0.0.1"
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	return cluster, nil
}

func CreateSession(cluster *gocql.ClusterConfig) (*gocql.Session, error) {
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("An error encountered while creating a new session. More info: %v", err)
	}
	return session, nil
}
