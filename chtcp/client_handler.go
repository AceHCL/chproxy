package chtcp

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/column"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
	"github.com/contentsquare/chproxy/log"
	"io"
	"net"
	"time"
)

func (client *clientInfo) decode(decoder *binary.Decoder, revision uint64) (err error) {
	clientQueryInitial, err := decoder.UInt8()
	fmt.Println(clientQueryInitial)
	initialUser, err := decoder.String()
	fmt.Println(initialUser)
	initialQueryID, err := decoder.String()
	fmt.Println(initialQueryID)
	initialAddr, err := decoder.String()
	fmt.Println(initialAddr)
	protocolType, err := decoder.UInt8() //chtcp -1,http -2
	fmt.Println(protocolType)

	if client.osUser, err = decoder.String(); err != nil {

	}

	if client.hostName, err = decoder.String(); err != nil {

	}

	if client.clientName, err = decoder.String(); err != nil {
		return err
	}
	if client.versionMajor, err = decoder.Uvarint(); err != nil {
		return err
	}
	if client.versionMinor, err = decoder.Uvarint(); err != nil {
		return err
	}
	if client.versionRevision, err = decoder.Uvarint(); err != nil {
		return err
	}
	if revision >= protocol.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		if _, err = decoder.String(); err != nil {
			return err
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		if _, err = decoder.Uvarint(); err != nil {
			return err
		}
	}

	if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
		decoder.UInt8()
	}

	return nil
}

func (conn *ClientConn) ResponseOK() error {
	if err := conn.encoder.Uvarint(protocol.ServerEndOfStream); err != nil {
		return err
	}
	return conn.encoder.Flush()
}

func (conn *ClientConn) ResponseException(err error) error {
	exception := &Exception{}
	msg := err.Error()
	exception.Message = msg
	return exception.Decode(conn.encoder, ServerInfo.Revision, conn.clientRevision)
}
func (conn *ClientConn) requestPacket() (bool, error) {
	decoder := conn.decoder
	decoder.SelectCompress(false)
	packet, err := decoder.UInt8()
	if err != nil {
		return false, nil
	}
	switch packet {
	case protocol.ClientHello:
		if err := conn.Hello(); err != nil {
			return false, err
		}
		return false, nil
	case protocol.ClientPing:
		if err := conn.receivePing(); err != nil {
			return false, err
		}
		return false, err
	case protocol.ClientData:
		if err := conn.receiveData(); err != nil {
			return false, err
		}
	case protocol.ClientQuery:
		query, err := conn.receiveQuery()
		if err != nil {
			return false, err
		}
		conn.Query = query
		return true, nil
	default:
		return false, fmt.Errorf("not support chtcp request type")
	}
	return false, nil
}
func (conn *ClientConn) processRequest() error {
	queryType := conn.Query.queryType
	switch queryType {
	case InsertType:
		if err := conn.processInsert(); err != nil {
			return err
		}
	case SelectType:
		if err := conn.processSelect(); err != nil {
			return err
		}
	case OtherType:
		if err := conn.processOther(); err != nil {
			return err
		}
	}
	return fmt.Errorf("not support qyery type error")
}
func (conn *ClientConn) Hello() (err error) {

	if err = conn.helloReceived(); err != nil {
		return err
	}
	if err = conn.helloSend(); err != nil {
		return err
	}
	return conn.encoder.Flush()

}
func (conn *ClientConn) readBlock(decoder *binary.Decoder) error {
	if _, err := decoder.UInt8(); err != nil {
		return err
	}
	if _, err := decoder.String(); err != nil {
		return err
	}
	conn.decoder.SelectCompress(conn.Query.Compress)
	if err := conn.block.Read(ServerInfo, decoder); err != nil {
		return err
	}
	return nil
}
func (conn *ClientConn) writeBlock() error {
	encoder := conn.encoder
	if err := encoder.Uvarint(protocol.ServerData); err != nil {
		return err
	}
	if err := encoder.String(""); err != nil {
		return err
	}
	encoder.SelectCompress(conn.Query.Compress)
	if err := conn.block.Write(ServerInfo, conn.encoder); err != nil {
		return err
	}
	encoder.SelectCompress(false)
	return encoder.Flush()
}
func (conn *ClientConn) processSelect() (err error) {
	dsn, err := conn.constructDsn()
	if err != nil {
		return err
	}
	if conn.chConn == nil {
		conn.chConn, err = clickhouse.Open(dsn)
		if err != nil {
			return err
		}
	}
	stmt, err := conn.chConn.Prepare(conn.Query.Query)
	if err != nil {
		return err
	}
	rows, err := stmt.(driver.StmtQueryContext).QueryContext(context.Background(), []driver.NamedValue{})
	if err != nil {
		return err
	}
	err = conn.responseMeta(&rows)
	if err != nil {
		return err
	}
	err = conn.responseData(&rows)
	if err != nil {
		return err
	}
	return conn.ResponseOK()
}
func (conn *ClientConn) responseMeta(rows *driver.Rows) error {
	meta := (*rows).(driver.RowsColumnTypeDatabaseTypeName)
	var columns []column.Column
	for i := 0; i < len(meta.Columns()); i++ {
		col, err := column.Factory(meta.Columns()[i], meta.ColumnTypeDatabaseTypeName(i), time.Local)
		if err != nil {
			return err
		}
		columns[i] = col
	}
	conn.block = &data.Block{
		NumColumns: uint64(len(meta.Columns())),
		Columns:    columns,
		Values:     make([][]interface{}, len(meta.Columns())),
	}
	conn.Lock()
	defer func() { conn.Unlock() }()
	if err := conn.writeBlock(); err != nil {
		return err
	}
	return nil
}
func (conn *ClientConn) constructDsn() (string, error) {
	scope := conn.Scope
	settings := conn.querySettings
	dsn := fmt.Sprintf("chtcp://%s?username=%s&password=%s", scope.Node, scope.ChPassword, scope.ChUsername)
	for name, value := range settings {
		dsn += dsn + fmt.Sprintf("&%s=%s", name, value)
	}
	return dsn, nil
}

func (conn *ClientConn) processInsert() error {
	return nil
}
func (conn *ClientConn) processOther() error {
	return nil
}
func (conn *ClientConn) responseData(rows *driver.Rows) error {
	encoder := conn.encoder
	if err := encoder.Uvarint(protocol.ServerData); err != nil {
		return err
	}
	values := (*rows).(driver.RowsColumnTypeDatabaseTypeName)
	dist := make([]driver.Value, len(conn.block.Columns))
	for {
		if err := values.Next(dist); err != nil {
			break
		}
		if err := conn.block.AppendRow(dist); err != nil {
			return err
		}
	}
	if err := conn.writeBlock(); err != nil {
		return err
	}
	return encoder.Flush()
}
func (conn *ClientConn) receivePing() error {
	if err := conn.encoder.UInt8(protocol.ServerPong); err != nil {
		return err
	}
	return conn.encoder.Flush()
}
func (conn *ClientConn) receiveQuery() (*ClientQuery, error) {
	query := &ClientQuery{}
	decoder := conn.decoder
	queryID, err := decoder.String()
	if err != nil {
		return nil, err
	}
	query.QueryID = queryID

	clientInfo := &clientInfo{}
	if err := clientInfo.decode(decoder, ServerInfo.Revision); err != nil {
		return nil, err
	}
	settings := &settingsInfo{}

	if conn.querySettings, err = settings.decode(decoder, ServerInfo.Revision); err != nil {
		return nil, err
	}

	if ServerInfo.Revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
		decoder.String()
	}

	if query.State, err = decoder.Uvarint(); err != nil {
		return nil, fmt.Errorf("get client query state error")
	}

	compress, err := decoder.Bool()
	if err != nil {
		return nil, fmt.Errorf("get client compress error")
	}
	query.Compress = compress

	length, err := decoder.Uvarint() //len
	buf := make([]byte, length)

	for i := 0; i < int(length); i++ {
		buf[i], err = decoder.ReadByte()
	}
	query.Query = string(buf)
	if query.queryType, err = getQueryType(query.Query); err != nil {
		return nil, err
	}
	if err = conn.readBlock(decoder); err != nil {
		return nil, err
	}
	return query, nil
}
func (conn *ClientConn) receiveData() error {
	return nil
}
func (conn *ClientConn) helloReceived() error {

	clientName, err := conn.decoder.String()
	if err != nil {
		return fmt.Errorf("could not decode client clientName: %w", err)
	}
	clientVersionMajor, err := conn.decoder.Uvarint()
	if err != nil {
		return fmt.Errorf("could not decode client major version: %w", err)
	}
	clientVersionMinor, err := conn.decoder.Uvarint()
	if err != nil {
		return fmt.Errorf("could not decode client minor version: %w", err)
	}
	clientRevision, err := conn.decoder.Uvarint()
	if err != nil {
		return fmt.Errorf("could not decode client revision: %w", err)
	}
	log.Debugf("hello <-[clientName: %s,clientMajorVersion: %d,clientMinorVersion: %d,clientRevision: %d]", clientName, clientVersionMajor, clientVersionMinor, clientRevision)
	conn.clientRevision = clientRevision

	defaultDB, err := conn.decoder.String()
	username, err := conn.decoder.String()
	password, err := conn.decoder.String()
	conn.Username = username
	conn.Password = password
	conn.Database = defaultDB
	return nil
}
func (conn *ClientConn) helloSend() error {
	if err := conn.encoder.UInt8(protocol.ServerHello); err != nil {
		return err
	}
	if err := ServerInfoDecode(conn.encoder, ServerInfo.Revision); err != nil {
		return err
	}
	if ServerInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		if err := conn.encoder.String("UTC"); err != nil {
			return err
		}
	}
	if ServerInfo.Revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
		if err := conn.encoder.String(ServerInfo.Name); err != nil {
			return err
		}
	}
	if ServerInfo.Revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		conn.encoder.Uvarint(20)
	}
	log.Debugf("hello -> [ServerInfo: %s,ServerMajorVersion: %d,ServerMinorVersion: %d,ServerRevision: %d]", ServerInfo.Name, ServerInfo.MajorVersion, ServerInfo.MinorVersion, ServerInfo.Revision)
	return conn.encoder.Flush()
}
func (conn *ClientConn) UnexpectedException(err error) error {
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return fmt.Errorf("client cliConn timeout: %w", err)
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
	conn.cliConn.Close()
}

func (conn *ClientConnInfo) Close() {
	if !conn.closed {
		conn.closed = true
		_ = conn.Conn.Close()
	}
}
