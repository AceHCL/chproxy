package chtcp

func (cliConn *ClientConnInfo) Read(buffer []byte) (n int, err error) {
	dstLen := len(buffer)
	total := 0
	for total < dstLen {
		n, err := cliConn.buffer.Read(buffer[total:])
		if err != nil {
			return n, err
		}
		total += n
	}
	return total, nil
}

func (cliConn *ClientConnInfo) Close() {
	if !cliConn.closed {
		cliConn.closed = true
		_ = cliConn.Conn.Close()
	}
}
