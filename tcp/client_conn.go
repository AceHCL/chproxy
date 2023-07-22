package tcp

import (
	"bufio"
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/column"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
	"github.com/contentsquare/chproxy/config"
	"github.com/contentsquare/chproxy/log"
	"io"
	"net"
	"sync"
	"time"
)

type ClientConn struct {
	sync.Mutex
	Username       string
	Password       string
	Database       string
	Query          *Query
	Scope          *Scope
	block          *data.Block
	connection     *connect
	decoder        *binary.Decoder
	encoder        *binary.Encoder
	querySettings  map[string]string
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
		Query:      &Query{},
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
func (conn *ClientConn) ReceiveRequest() (bool, error) {
	decoder := conn.decoder
	decoder.SelectCompress(false)
	packet, err := decoder.Uvarint()
	if err != nil {
		return false, err
	}
	switch packet {
	case protocol.ClientPing:
		if err := conn.receivePing(); err != nil {
			return false, err
		}
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
		return false, fmt.Errorf("not support tcp request type")
	}
	return false, nil
}
func (conn *ClientConn) ProcessRequest() error {
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
func (conn *ClientConn) readBlock(decoder *binary.Decoder) error {
	if _, err := decoder.Uvarint(); err != nil {
		return err
	}
	if _, err := decoder.String(); err != nil {
		return err
	}
	conn.decoder.SelectCompress(conn.Query.Compress)
	if err := conn.block.Read(CHProxyServerInfo, decoder); err != nil {
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
	if err := conn.block.Write(CHProxyServerInfo, conn.encoder); err != nil {
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
	dsn := fmt.Sprintf("tcp://%s?username=%s&password=%s", scope.Node, scope.Username, scope.PassWord)
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
	if err := conn.encoder.Uvarint(protocol.ServerPong); err != nil {
		return err
	}
	return conn.encoder.Flush()
}
func (conn *ClientConn) receiveQuery() (*Query, error) {
	var (
		query   *Query
		decoder *binary.Decoder
	)
	queryID, err := decoder.String()
	if err != nil {
		return nil, err
	}
	query.QueryID = queryID

	queryInfo := &queryInfo{}
	if err := queryInfo.Read(decoder); err != nil {
		return nil, err
	}
	clientInfo := &clientInfo{}
	if err := clientInfo.read(decoder); err != nil {
		return nil, err
	}
	settings := &settingsInfo{}

	if conn.querySettings, err = settings.deserialize(decoder); err != nil {
		return nil, fmt.Errorf("deserialize client settings error")
	}

	if query.State, err = decoder.Uvarint(); err != nil {
		return nil, fmt.Errorf("get client query state error")
	}

	compress, err := decoder.Uvarint()
	if err != nil {
		return nil, fmt.Errorf("get client compress error")
	}
	query.Compress = compress > 0

	if query.Query, err = decoder.String(); err != nil {
		return nil, fmt.Errorf("get client query error")
	}
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
	conn.Database = defaultDB
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
