package tcp

import "github.com/ClickHouse/clickhouse-go/lib/binary"

type QueryInfo struct {
	QueryID         string
	Query           string
	State           uint64
	Compress        uint64
	QueryClientInfo *QueryClientInfo
}
type QueryClientInfo struct {
}

func (client *QueryClientInfo) Read(decoder *binary.Decoder, revision uint64) error {

}
