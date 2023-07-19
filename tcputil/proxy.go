package tcputil

import "github.com/contentsquare/chproxy/tcp"

type ReverseProxy struct {
}

func (p *ReverseProxy) ServeTCP(conn *tcp.Conn) {
	//processHello
	//run
}
