package tcp

import (
	"bufio"
	"github.com/contentsquare/chproxy/config"
	"net"
	"time"
)

type connect struct {
	net.Conn
	buffer              *bufio.Reader
	closed              bool
	readTimeout         config.Duration
	writeTimeout        config.Duration
	lastReadExpireTime  time.Time
	lastWriteExpireTime time.Time
}

func (conn *connect) Close() {
	if !conn.closed {
		conn.closed = true
		_ = conn.Conn.Close()
	}
}
