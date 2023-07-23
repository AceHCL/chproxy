package chtcp

import (
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/contentsquare/chproxy/config"
	"net"
	"time"
)

const (
	VersionName  = "ChProxy"
	VersionMajor = 21
	VersionMinor = 3
)

type HandlerFunc func(conn net.Conn, readTimeout, writeTimeout config.Duration)

var ServerInfo = &data.ServerInfo{
	Name:         VersionName,
	MajorVersion: VersionMajor,
	MinorVersion: VersionMinor,
	Revision:     54447,
	Timezone:     time.Now().Local().Location(),
}
