package chtcp

import (
	"bufio"
	"database/sql/driver"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/contentsquare/chproxy/config"
	"net"
	"sync"
	"time"
)

type clientInfo struct {
	osUser          string
	hostName        string
	clientName      string
	versionMajor    uint64
	versionMinor    uint64
	versionRevision uint64
}

type ClientConn struct {
	sync.Mutex
	Username       string
	Password       string
	Database       string
	Query          *ClientQuery
	Scope          *Scope
	block          *data.Block
	cliConn        *ClientConnInfo
	decoder        *binary.Decoder
	encoder        *binary.Encoder
	querySettings  map[string]string
	chConn         driver.Conn
	clientRevision uint64
}

func NewClientConn(conn net.Conn, readTimeout, writeTimeout config.Duration) *ClientConn {
	buffer := bufio.NewWriter(conn)
	now := time.Now().Add(+time.Minute * 10)
	conn.SetReadDeadline(now)
	conn.SetWriteDeadline(now)
	cliConn := &ClientConnInfo{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		Conn:         conn,
		buffer:       bufio.NewReader(conn),
	}
	decoder := binary.NewDecoderWithCompress(cliConn)
	encoder := binary.NewEncoderWithCompress(buffer)
	return &ClientConn{
		cliConn: cliConn,
		decoder: decoder,
		encoder: encoder,
		Query:   &ClientQuery{},
	}
}

type ClientQuery struct {
	QueryID   string
	Query     string
	State     uint64
	Compress  bool
	queryType int
}
