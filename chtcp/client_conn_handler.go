package chtcp

func (cliConn *ClientConnInfo) Read(buffer []byte) (n int, err error) {
	return cliConn.Conn.Read(buffer)
}

func (cliConn *ClientConnInfo) Close() {
	if !cliConn.closed {
		cliConn.closed = true
		_ = cliConn.Conn.Close()
	}
}
