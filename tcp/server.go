package tcp

import "net"

type HandlerFunc func(conn *Conn)

func (h HandlerFunc) ServeTCP(conn *Conn) {
	h(conn)
}

type Handler interface {
	ServeTCP(conn *Conn)
}

type Server struct {
	Handler Handler
}

func (srv *Server) Serve(ln net.Listener) error {
	clientConn := srv.newConn()
	//这个地方会调用main包定义的函数，明确意义
	srv.Handler.ServeTCP(clientConn)
	return nil
}

func (srv *Server) newConn() *Conn {
	return &Conn{}
}

type Conn struct {
	net.Conn
}
