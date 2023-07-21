package main

import (
	"fmt"
	"github.com/contentsquare/chproxy/tcp"
)

type ReverseProxy struct {
}

func (p *ReverseProxy) Serve(conn *tcp.ClientConn, scope *scope) error {
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
		if err = conn.ProcessRequest(); err != nil {
			if err := conn.ResponseException(err); err != nil {
				return fmt.Errorf("response exception error: %w", err)
			}
		}
	}
}
