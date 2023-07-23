package chtcp

import (
	"bufio"
	"github.com/contentsquare/chproxy/config"
	"net"
	"time"
)

type ClientConnInfo struct {
	net.Conn
	buffer              *bufio.Reader
	closed              bool
	readTimeout         config.Duration
	writeTimeout        config.Duration
	lastReadExpireTime  time.Time
	lastWriteExpireTime time.Time
}
