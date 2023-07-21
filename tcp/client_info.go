package tcp

import "github.com/ClickHouse/clickhouse-go/lib/binary"

type clientInfo struct {
	name            string
	versionMajor    uint64
	versionMinor    uint64
	versionRevision uint64
}

func (client *clientInfo) read(decoder *binary.Decoder) (err error) {
	if client.name, err = decoder.String(); err != nil {
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
	return nil
}
