package tcp

import (
	"github.com/contentsquare/chproxy/config"
	"github.com/contentsquare/chproxy/internal/heartbeat"
	"golang.org/x/time/rate"
	"net/url"
	"strings"
	"time"
)

type Scope struct {
	Cluster  string
	Username string
	PassWord string
	Node     string
}

func NewScope(username, password, node string) *Scope {
	return &Scope{
		Username: username,
		PassWord: password,
		Node:     node,
	}
}

type Cluster struct {
	name string

	replicas       []*replica
	nextReplicaIdx uint32

	users map[string]*clusterUser

	killQueryUserName     string
	killQueryUserPassword string

	heartBeat heartbeat.HeartBeat

	retryNumber int
}
type host struct {
	replica *replica

	// Counter of unsuccessful requests to decrease host priority.
	penalty uint32

	// Either the current host is alive.
	active uint32

	// Host address.
	addr *url.URL
}
type cluster struct {
	name string

	replicas       []*replica
	nextReplicaIdx uint32

	users map[string]*clusterUser

	killQueryUserName     string
	killQueryUserPassword string

	heartBeat heartbeat.HeartBeat

	retryNumber int
}
type clusterUser struct {
	name     string
	password string

	maxConcurrentQueries uint32

	maxExecutionTime time.Duration

	reqPerMin uint32

	queueCh      chan struct{}
	maxQueueTime time.Duration

	reqPacketSizeTokenLimiter *rate.Limiter
	reqPacketSizeTokensBurst  config.ByteSize
	reqPacketSizeTokensRate   config.ByteSize

	allowedNetworks config.Networks
	isWildcarded    bool
}
type replica struct {
	cluster *cluster

	name string

	hosts       []*host
	nextHostIdx uint32
}
type User struct {
}

func NewClusters(clusters []config.Cluster) map[string]*Cluster {
	for _, cluster := range clusters {
		if !strings.EqualFold("tcp", cluster.Scheme) {
			continue
		}

	}
}
func NewUsers(users []config.User) map[string]*User {

}

func newCluster(cfg *config.Cluster) {

}
func newUser(cfg *config.User) {

}
