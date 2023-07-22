package main

import (
	"fmt"
	"github.com/contentsquare/chproxy/config"
	"github.com/contentsquare/chproxy/tcp"
	"strings"
)

type ReverseProxy struct {
	Users    map[string]*tcp.User
	Clusters map[string]*tcp.Cluster
	Conn     *tcp.ClientConn
}

func (p *ReverseProxy) loadConfig(cfg *config.Config) (err error) {
	if p.Clusters, err = p.newClusters(cfg.Clusters); err != nil {
		return err
	}
	if p.Users, err = p.newUsers(cfg.Users); err != nil {
		return err
	}
	return nil
}
func (p *ReverseProxy) newClusters(cfg []config.Cluster) (map[string]*tcp.Cluster, error) {
	clusters := make(map[string]*tcp.Cluster, len(cfg))
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
func (p *ReverseProxy) newCluster(cfg config.Cluster) (*tcp.Cluster, error) {
	replicas := make([]*tcp.Replica, len(cfg.Replicas))
	for i, r := range cfg.Replicas {
		replicas[i].Name = r.Name
		replicas[i].Nodes = r.Nodes
	}
	cUsers := make(map[string]*tcp.ClusterUser, len(cfg.ClusterUsers))
	for _, cu := range cfg.ClusterUsers {
		cUsers[cu.Name] = &tcp.ClusterUser{
			Name:     cu.Name,
			Password: cu.Password,
		}
	}
	return &tcp.Cluster{
		Name:     cfg.Name,
		Replicas: replicas,
		Users:    cUsers,
	}, nil
}

func (p *ReverseProxy) newUsers(cfg []config.User) (map[string]*tcp.User, error) {
	users := make(map[string]*tcp.User, len(cfg))
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
func (p *ReverseProxy) newUser(cfg config.User) (*tcp.User, error) {
	return &tcp.User{
		Name:      cfg.Name,
		Password:  cfg.Password,
		ToUser:    cfg.ToUser,
		ToCluster: cfg.ToCluster,
	}, nil
}

func (p *ReverseProxy) getScope() (*tcp.Scope, error) {
	proxyUserInfo, err := p.getAuth()
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
	return &tcp.Scope{
		ChCluster:  chInfo.Name,
		ChUsername: chUserInfo.Name,
		ChPassword: chUserInfo.Password,
		Node:       node,
	}, nil
}
func (p *ReverseProxy) getAuth() (*tcp.User, error) {
	for proxyUser, proxyInfo := range p.Users {
		if strings.EqualFold(p.Conn.Username, proxyUser) && strings.EqualFold(p.Conn.Password, proxyInfo.Password) {
			return proxyInfo, nil
		}
	}
	return nil, fmt.Errorf("username or password not support")
}
func (p *ReverseProxy) getCluster(user *tcp.User) (*tcp.Cluster, error) {
	for name, clusterInfo := range p.Clusters {
		if strings.EqualFold(user.ToCluster, name) {
			return clusterInfo, nil
		}
	}
	return nil, fmt.Errorf("not found availale cluster")
}
func (p *ReverseProxy) getClusterUser(cluster *tcp.Cluster, chUsername string) (*tcp.ClusterUser, error) {
	for name, clusterUserInfo := range cluster.Users {
		if strings.EqualFold(name, chUsername) {
			return clusterUserInfo, nil
		}
	}
	return nil, fmt.Errorf("not found to user available")
}
func (p *ReverseProxy) getRandomNode(cluster *tcp.Cluster) (string, error) {
	return cluster.Replicas[0].Nodes[0], nil
}

func (p *ReverseProxy) Serve() (err error) {
	conn := p.Conn
	if conn.Scope, err = p.getScope(); err != nil {
		return err
	}
	for {
		query := conn.Query
		query.QueryID, query.Query = "", ""
		end, err := conn.ReceiveRequest()
		if err != nil {
			if err := conn.UnexpectedException(err); err != nil {
				return err
			}
		}
		if !end {
			continue
		}
		if err = conn.ProcessRequest(); err != nil {
			if err := conn.ResponseException(err); err != nil {
				return fmt.Errorf("response exception error: %w", err)
			}
		}
	}
}
