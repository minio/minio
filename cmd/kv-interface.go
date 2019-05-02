package cmd

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"
)

type KVInterface interface {
	Put(keyStr string, value []byte) error
	Get(keyStr string, value []byte) ([]byte, error)
	Delete(keyStr string) error
	List(prefix string, buf []byte) ([]string, error)
}

const kvNSEntryPaddingMultiple = 4

type KVNSEntry struct {
	Key     string
	Size    int64
	ModTime time.Time
	IDs     []string
}

func KVNSEntryMarshal(entry KVNSEntry, buf []byte) ([]byte, error) {
	b, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	if !kvPadding {
		return b, nil
	}
	binary.PutVarint(buf[:4], int64(len(b)))
	copy(buf[4:], b)
	buf = buf[:4+len(b)]
	paddedLength := ceilFrac(int64(len(buf)), kvNSEntryPaddingMultiple) * kvNSEntryPaddingMultiple
	buf = buf[:paddedLength]
	return buf, nil
}

func KVNSEntryUnmarshal(b []byte, entry *KVNSEntry) error {
	if kvPadding {
		length, _ := binary.Varint(b[:4])
		b = b[4 : 4+length]
	}
	return json.Unmarshal(b, entry)
}

var errValueTooLong = errors.New("value too long")

const kvDataDir = "data/"
const kvMetaDir = "meta/"
