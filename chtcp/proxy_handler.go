package chtcp

import (
	"fmt"
	"github.com/contentsquare/chproxy/config"
	"github.com/contentsquare/chproxy/log"
	"net"
	"strings"
)

func (p *ReverseProxy) ApplyConfig(cfg *config.Config) (err error) {
	if p.Clusters, err = p.newClusters(cfg.Clusters); err != nil {
		return err
	}
	if p.Users, err = p.newUsers(cfg.Users); err != nil {
		return err
	}
	return nil
}
func (p *ReverseProxy) newClusters(cfg []config.Cluster) (map[string]*Cluster, error) {
	clusters := make(map[string]*Cluster, len(cfg))
	for _, c := range cfg {
		if _, ok := clusters[c.Name]; ok {
			return nil, fmt.Errorf("duplicate config for cluster %q", c.Name)
		}
		tmpC, err := p.newCluster(c)
		if err != nil {
			return nil, fmt.Errorf("cannot initialize cluster %q: %w", c.Name, err)
		}
		clusters[c.Name] = tmpC
	}
	return clusters, nil
}
func (p *ReverseProxy) newCluster(cfg config.Cluster) (*Cluster, error) {
	replicas := make([]*Replica, len(cfg.Replicas))
	for i, r := range cfg.Replicas {
		replicas[i].Name = r.Name
		replicas[i].Nodes = r.Nodes
	}
	cUsers := make(map[string]*ClusterUser, len(cfg.ClusterUsers))
	for _, cu := range cfg.ClusterUsers {
		cUsers[cu.Name] = &ClusterUser{
			Name:     cu.Name,
			Password: cu.Password,
		}
	}
	nodes := make([]string, len(cfg.Nodes))
	for i, node := range cfg.Nodes {
		nodes[i] = node
	}
	return &Cluster{
		Name:     cfg.Name,
		Replicas: replicas,
		Users:    cUsers,
		TCPNodes: nodes,
	}, nil
}

func (p *ReverseProxy) newUsers(cfg []config.User) (map[string]*User, error) {
	users := make(map[string]*User, len(cfg))
	for _, c := range cfg {
		if _, ok := users[c.Name]; ok {
			return nil, fmt.Errorf("duplicate config for user %q", c.Name)
		}
		tmpUser, err := p.newUser(c)
		if err != nil {
			return nil, fmt.Errorf("cannot initialize user %q: %w", c.Name, err)
		}
		users[c.Name] = tmpUser
	}
	return users, nil
}
func (p *ReverseProxy) newUser(cfg config.User) (*User, error) {
	return &User{
		Name:      cfg.Name,
		Password:  cfg.Password,
		ToUser:    cfg.ToUser,
		ToCluster: cfg.ToCluster,
	}, nil
}

func (p *ReverseProxy) getScope(cliConn ClientConn) (*Scope, error) {
	proxyUserInfo, err := p.getAuth(cliConn)
	if err != nil {
		return nil, err
	}
	chInfo, err := p.getCluster(proxyUserInfo)
	if err != nil {
		return nil, err
	}
	chUserInfo, err := p.getClusterUser(chInfo, proxyUserInfo.ToUser)
	if err != nil {
		return nil, err
	}
	node, err := p.getRandomNode(chInfo)
	if err != nil {
		return nil, err
	}
	return &Scope{
		ChCluster:  chInfo.Name,
		ChUsername: chUserInfo.Name,
		ChPassword: chUserInfo.Password,
		Node:       node,
	}, nil
}
func (p *ReverseProxy) getAuth(cliConn ClientConn) (*User, error) {
	for proxyUser, proxyInfo := range p.Users {
		if strings.EqualFold(cliConn.Username, proxyUser) && strings.EqualFold(cliConn.Password, proxyInfo.Password) {
			return proxyInfo, nil
		}
	}
	return nil, fmt.Errorf("username or password not support")
}
func (p *ReverseProxy) getCluster(user *User) (*Cluster, error) {
	for name, clusterInfo := range p.Clusters {
		if strings.EqualFold(user.ToCluster, name) {
			return clusterInfo, nil
		}
	}
	return nil, fmt.Errorf("not found availale cluster")
}
func (p *ReverseProxy) getClusterUser(cluster *Cluster, chUsername string) (*ClusterUser, error) {
	for name, clusterUserInfo := range cluster.Users {
		if strings.EqualFold(name, chUsername) {
			return clusterUserInfo, nil
		}
	}
	return nil, fmt.Errorf("not found to user available")
}
func (p *ReverseProxy) getRandomNode(cluster *Cluster) (string, error) {
	if len(cluster.TCPNodes) > 0 {
		return cluster.TCPNodes[0], nil
	}
	if len(cluster.Replicas) > 0 {
		return cluster.Replicas[0].Nodes[0], nil
	}
	return "", fmt.Errorf("not found available node ")
}

func (p *ReverseProxy) handle(conn net.Conn, readTimeout, writeTime config.Duration) {
	clientCon := NewClientConn(conn, readTimeout, writeTime)
	for {
		query := clientCon.Query
		query.QueryID, query.Query = "", ""
		end, err := clientCon.requestPacket()
		if err != nil {
			if err := clientCon.UnexpectedException(err); err != nil {
				log.Errorf("request packet error")
			}
		}
		if !end {
			continue
		}
		if err = clientCon.processRequest(); err != nil {
			if err := clientCon.ResponseException(err); err != nil {
				log.Errorf("process request error")
			}
		}
	}
}
