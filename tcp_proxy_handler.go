package main

import (
	"github.com/contentsquare/chproxy/log"
	"github.com/contentsquare/chproxy/tcp"
)

type ReverseProxy struct {
}

func (p *ReverseProxy) Serve(clientConn *tcp.ClientConn, scope *scope) {
	log.Errorf("啥也没干，结束吧！！！！")
}
