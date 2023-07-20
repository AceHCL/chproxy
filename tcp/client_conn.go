package tcp

import (
	"bufio"
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
	"github.com/contentsquare/chproxy/config"
	"github.com/contentsquare/chproxy/log"
	"io"
	"net"
)

type ClientConn struct {
	Username       string
	Password       string
	Database       string
	Query          *QueryInfo
	block          *data.Block
	connection     *connect
	decoder        *binary.Decoder
	encoder        *binary.Encoder
	chConn         driver.Conn
	clientRevision uint64
}

func NewClientConn(conn net.Conn, readTimeout, writeTimeout config.Duration) *ClientConn {
	buffer := bufio.NewWriter(conn)
	connection := &connect{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		Conn:         conn,
		buffer:       bufio.NewReader(conn),
	}
	decoder := binary.NewDecoderWithCompress(connection)
	encoder := binary.NewEncoderWithCompress(buffer)
	return &ClientConn{
		connection: connection,
		decoder:    decoder,
		encoder:    encoder,
		Query:      &QueryInfo{},
	}
}
func (conn *ClientConn) ResponseException(err error) error {
	exception := &Exception{}
	msg := err.Error()
	exception.Message = msg
	return exception.Write(conn.encoder, CHProxyServerInfo.Revision, conn.clientRevision)
}
func (conn *ClientConn) ResponseOK() error {
	if err := conn.encoder.Uvarint(protocol.ServerEndOfStream); err != nil {
		return err
	}
	return conn.encoder.Flush()
}
func (conn *ClientConn) Receive() (bool, error) {
	conn.decoder.SelectCompress(false)
	packet, err := conn.decoder.Uvarint()
	if err != nil {
		return false, err
	}
	switch packet {
	case protocol.ClientPing:
		if err := conn.processPing(); err != nil {
			return false, err
		}
	case protocol.ClientData:
		if err := conn.processData(); err != nil {
			return false, err
		}
	case protocol.ClientQuery:
		query, err := conn.processQuery()
		if err != nil {
			return false, err
		}
		conn.Query = query
		return true, nil
	default:
		return false, fmt.Errorf("received unexpect packet type")
	}
	return false, nil
}
func (conn *ClientConn) Process() error {

}
func (conn *ClientConn) Hello() error {
	packet, err := conn.decoder.Uvarint()
	if err != nil {
		return err
	}

	if packet != protocol.ClientHello {
		//exception
	}
	if err = conn.helloReceived(); err != nil {
		return err
	}
	if err = conn.helloSend(); err != nil {
		return err
	}
	return conn.encoder.Flush()

}
func (conn *ClientConn) processPing() error {
	if err := conn.encoder.Uvarint(protocol.ServerPong); err != nil {
		return err
	}
	return conn.encoder.Flush()
}
func (conn *ClientConn) processQuery() (*QueryInfo, error) {
	queryID, err := conn.decoder.String()
	if err != nil {
		return nil, err
	}
	clientInfo := &QueryClientInfo{}
	err = clientInfo.Read(conn.decoder, conn.clientRevision)
	if err != nil {
		return nil, err
	}
	settings := make(map[string]string)
	for {
		//name
		//empty name

	}
	return conn.Query, nil
}
func (conn *ClientConn) processData() error {

}
func (conn *ClientConn) helloReceived() error {

	clientName, err := conn.decoder.String()
	if err != nil {
		return fmt.Errorf("could not read client name: %w", err)
	}
	clientVersionMajor, err := conn.decoder.Uvarint()
	if err != nil {
		return fmt.Errorf("could not read client major version: %w", err)
	}
	clientVersionMinor, err := conn.decoder.Uvarint()
	if err != nil {
		return fmt.Errorf("could not read client minor version: %w", err)
	}
	clientRevision, err := conn.decoder.Uvarint()
	if err != nil {
		return fmt.Errorf("could not read client revision: %w", err)
	}
	log.Debugf("hello received,clientName: %s,clientMajorVersion: %d,clientMinorVersion: %d,clientRevision: %d", clientName, clientVersionMajor, clientVersionMinor, clientRevision)
	conn.clientRevision = clientRevision

	defaultDB, err := conn.decoder.String()
	username, err := conn.decoder.String()
	password, err := conn.decoder.String()
	conn.Username = username
	conn.Password = password
	conn.database = defaultDB
	return nil
}
func (conn *ClientConn) helloSend() error {
	if err := conn.encoder.Uvarint(protocol.ServerHello); err != nil {
		return err
	}
	if err := serverInfoWrite(conn.encoder); err != nil {
		return err
	}
	if conn.clientRevision > protocol.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		if err := conn.encoder.String("UTC"); err != nil {
			return err
		}
	}
	if conn.clientRevision > DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
		if err := conn.encoder.String(CHProxyServerInfo.Name); err != nil {
			return err
		}
	}
	return conn.encoder.Flush()
}
func (conn *ClientConn) UnexpectedException(err error) error {
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return fmt.Errorf("client conn timeout: %w", err)
	}
	if err != io.EOF {
		if err := conn.ResponseException(err); err != nil {
			return fmt.Errorf("response exception error: %w", err)
		}
	}
	return fmt.Errorf("unexpect error: %w", err)
}
func (conn *ClientConn) Close() {
	if conn.chConn != nil {
		_ = conn.chConn.Close()
	}
	conn.connection.Close()
}
