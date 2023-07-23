package chtcp

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
)

type Exception struct {
	Code       int32
	Name       string
	Message    string
	StackTrace string
	nested     error
}

func (e *Exception) Error() string {
	return fmt.Sprintf("code: %d,message: %s", e.Code, e.Message)
}

func (e *Exception) Decode(encoder *binary.Encoder, serverRevision uint64, clientRevision uint64) error {
	if err := encoder.Uvarint(protocol.ServerException); err != nil {
		return err
	}
	if err := encoder.Int32(e.Code); err != nil {
		return err
	}
	if err := encoder.String(e.Name); err != nil {
		return err
	}
	if err := encoder.String(e.Message); err != nil {
		return err
	}
	if err := encoder.String(e.StackTrace); err != nil {
		return err
	}
	if e.nested != nil {
		ex, ok := e.nested.(*Exception)
		if ok {
			err := ex.Decode(encoder, serverRevision, clientRevision)
			if err != nil {
				return nil
			}
		}

	}
	return encoder.Flush()
}
