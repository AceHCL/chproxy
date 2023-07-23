package chtcp

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/contentsquare/chproxy/config"
	"net"
)

func ServerInfoDecode(encoder *binary.Encoder, revision uint64) error {
	if err := encoder.String(ServerInfo.Name); err != nil {
		return err
	}
	if err := encoder.Uvarint(ServerInfo.MajorVersion); err != nil {
		return err
	}
	if err := encoder.Uvarint(ServerInfo.MinorVersion); err != nil {
		return err
	}
	if err := encoder.Uvarint(ServerInfo.Revision); err != nil {
		return err
	}
	return nil
}

func (srv *Server) Serve() (err error) {
	if srv.Listener == nil {
		return fmt.Errorf("listener is nil")
	}

	go func() {
		for {
			conn, err := srv.Accept()
			if err != nil {
				continue
			}
			go srv.Handler.ServeTCP(conn, srv.ReadTimeout, srv.WriteTimeout)
		}
	}()
	return nil
}

func (h HandlerFunc) DefaultHandler(conn net.Conn, readTimeout, writeTimeout config.Duration) {

}
