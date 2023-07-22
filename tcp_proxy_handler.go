package main

import (
	"fmt"
	"github.com/contentsquare/chproxy/tcp"
	"strings"
)

type ReverseProxy struct {
	Users    map[string]*tcp.User
	Clusters map[string]*tcp.Cluster
	Conn     *tcp.ClientConn
}

func (p *ReverseProxy) rp() error {
	conn := p.Conn
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
		if conn.Scope, err = p.getScope(); err != nil {
			return err
		}
		if err = conn.ProcessRequest(); err != nil {
			if err := conn.ResponseException(err); err != nil {
				return fmt.Errorf("response exception error: %w", err)
			}
		}
	}
}
func (p *ReverseProxy) getScope() (*tcp.Scope, error) {
	user, pass, node, err := p.getUser(p.Conn.Username, p.Conn.Password)
	if err != nil {
		return nil, err
	}
	return tcp.NewScope(user, pass, node), nil
}
func (p *ReverseProxy) getUser(username, password string) (string, string, string, error) {
	var (
		toCluster, toUser, toPassword, toNode string
	)
	for user, info := range p.Users {
		if !strings.EqualFold(user, username) {
			continue
		}
		if !strings.EqualFold(password, info.password) {
			return "", "", "", fmt.Errorf("not concrectly password")
		}
		toCluster = info.toCluster
		toUser = info.toUser
		break
	}
	for name, info := range p.Clusters {
		if !strings.EqualFold(name, toCluster) {
			continue
		}
		for s, cu := range info.users {
			if !strings.EqualFold(s, toUser) {
				continue
			}
			toPassword = cu.password
			break
		}
		toNode = info.replicas[0].hosts[0].addr.Host
		break
	}
	return toUser, toPassword, toNode, nil
}
