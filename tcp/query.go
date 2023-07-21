package tcp

import (
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
)

type Query struct {
	QueryID   string
	Query     string
	State     uint64
	Compress  bool
	QueryInfo *queryInfo
	queryType int
}
type queryInfo struct {
}

func (client *queryInfo) Read(decoder *binary.Decoder) error {
	if _, err := decoder.Uvarint(); err != nil {
		return err
	}
	if _, err := decoder.String(); err != nil {
		return err
	}
	if _, err := decoder.String(); err != nil {
		return err
	}
	if _, err := decoder.String(); err != nil {
		return err
	}
	if _, err := decoder.Uvarint(); err != nil {
		return err
	}
	if _, err := decoder.String(); err != nil {
		return err
	}
	if _, err := decoder.String(); err != nil {
		return err
	}
	if CHProxyServerInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		if _, err := decoder.String(); err != nil {
			return err
		}
	}
	return nil
}
