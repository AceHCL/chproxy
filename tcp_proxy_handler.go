package main

import (
	"fmt"
	"github.com/contentsquare/chproxy/tcp"
)

type ReverseProxy struct {
}

func (p *ReverseProxy) Serve(clientConn *tcp.ClientConn, scope *scope) error {
	for {
		clientConn.Query.QueryID, clientConn.Query.Query = "", ""
		end, err := clientConn.Receive()
		if err != nil {
			if err := clientConn.UnexpectedException(err); err != nil {
				return err
			}
		}
		if !end {
			continue
		}
		if err = clientConn.Process(); err != nil {
			if err := clientConn.ResponseException(err); err != nil {
				return fmt.Errorf("response exception error: %w", err)
			}
		}
	}
}
