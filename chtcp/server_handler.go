package chtcp

import "github.com/ClickHouse/clickhouse-go/lib/binary"

func ServerInfoDecode(encoder *binary.Encoder, revision uint64) error {
	if err := encoder.String(ServerInfo.Name); err != nil {
		return err
	}
	if err := encoder.Uvarint(ServerInfo.MajorVersion); err != nil {
		return err
	}
	if err := encoder.Uvarint(ServerInfo.MinorVersion); err != nil {
		return err
	}
	if err := encoder.Uvarint(ServerInfo.Revision); err != nil {
		return err
	}
	return nil
}
