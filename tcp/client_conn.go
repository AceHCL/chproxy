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
	"net"
)

type ClientConn struct {
	Username       string
	Password       string
	database       string
	block          *data.Block
	connection     *connect
	decoder        *binary.Decoder
	encoder        *binary.Encoder
	chConn         driver.Conn
	query          *QueryInfo
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
		query:      &QueryInfo{},
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
func (conn *ClientConn) Close() {
	if conn.chConn != nil {
		_ = conn.chConn.Close()
	}
	conn.connection.Close()
}
