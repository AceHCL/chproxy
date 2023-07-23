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
	connection := &ClientConnInfo{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		Conn:         conn,
		buffer:       bufio.NewReader(conn),
	}
	decoder := binary.NewDecoderWithCompress(connection)
	encoder := binary.NewEncoderWithCompress(buffer)
	return &ClientConn{
		cliConn: connection,
		decoder: decoder,
		encoder: encoder,
		Query:   &ClientQuery{},
	}
}

type ClientConnInfo struct {
	net.Conn
	buffer              *bufio.Reader
	closed              bool
	readTimeout         config.Duration
	writeTimeout        config.Duration
	lastReadExpireTime  time.Time
	lastWriteExpireTime time.Time
}

type ClientQuery struct {
	QueryID   string
	Query     string
	State     uint64
	Compress  bool
	queryType int
}
