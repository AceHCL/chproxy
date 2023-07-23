package chtcp

import (
	"github.com/contentsquare/chproxy/config"
	"github.com/contentsquare/chproxy/internal/heartbeat"
	"time"
)

type ReverseProxy struct {
	Handler  HandlerFunc
	Users    map[string]*User
	Clusters map[string]*Cluster
	Conn     *ClientConn
}

type Scope struct {
	ChCluster  string
	ChUsername string
	ChPassword string
	Node       string
}

type User struct {
	Name     string
	Password string

	ToCluster string
	ToUser    string

	maxExecutionTime time.Duration

	allowedNetworks config.Networks

	denyTCP bool
}

type Cluster struct {
	Name string

	Replicas []*Replica
	TCPNodes []string

	Users map[string]*ClusterUser

	heartBeat heartbeat.HeartBeat
}
type Replica struct {
	Name string

	Nodes []string
}

type Host struct {
	Addr string
}

type ClusterUser struct {
	Name     string
	Password string

	maxConcurrentQueries uint32

	maxExecutionTime time.Duration
}
