package tcp

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/contentsquare/chproxy/config"
	"net"
	"time"
)

type HandlerFunc func(conn net.Conn, readTimeout, writeTimeout config.Duration)

func (h HandlerFunc) ServeTCP(conn net.Conn, readTimeout, writeTimeout config.Duration) {
	h(conn, readTimeout, writeTimeout)
}

type Handler interface {
	ServeTCP(conn net.Conn, readTimeout, writeTimeout config.Duration)
}

type Server struct {
	Handler      Handler
	ReadTimeout  config.Duration
	WriteTimeout config.Duration
}

func (srv *Server) Serve(ln net.Listener) (err error) {
	if ln == nil {
		return fmt.Errorf("listener is nil")
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go srv.Handler.ServeTCP(conn, srv.ReadTimeout, srv.WriteTimeout)
		}
	}()
	return nil
}

const (
	VersionName  = "chproxy"
	VersionMajor = 23
	VersionMinor = 7
)

const (
	DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372
)

var CHProxyServerInfo = &data.ServerInfo{
	Name:         VersionName,
	MajorVersion: VersionMajor,
	MinorVersion: VersionMinor,
	Revision:     54372,
	Timezone:     time.Now().Local().Location(),
}

func serverInfoWrite(encoder *binary.Encoder) error {
	if err := encoder.String(CHProxyServerInfo.Name); err != nil {
		return err
	}
	if err := encoder.Uvarint(CHProxyServerInfo.MajorVersion); err != nil {
		return err
	}
	if err := encoder.Uvarint(CHProxyServerInfo.MinorVersion); err != nil {
		return err
	}
	if err := encoder.Uvarint(CHProxyServerInfo.Revision); err != nil {
		return err
	}
	return nil
}
