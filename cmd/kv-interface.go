package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type KVInterface interface {
	Put(keyStr string, value []byte) error
	Get(keyStr string, value []byte) ([]byte, error)
	Delete(keyStr string) error
	List(prefix string, buf []byte) ([]string, error)
	DiskInfo() (DiskInfo, error)
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
	length := len(b)
	lengthInBytes := []byte(fmt.Sprintf("%.8x", length))
	n := copy(buf, lengthInBytes)
	if n != 8 {
		panic("length is not 8")
	}
	n = copy(buf[8:], b)
	buf = buf[:8+n]
	paddedLength := ceilFrac(int64(len(buf)), kvNSEntryPaddingMultiple) * kvNSEntryPaddingMultiple
	for len(buf) < int(paddedLength) {
		buf = append(buf, '#')
	}
	return buf, nil
}

func KVNSEntryUnmarshal(b []byte, entry *KVNSEntry) error {
	var buf []byte
	buf = b
	var length int
	if kvPadding {
		_, err := fmt.Sscanf(string(buf[:8]), "%x", &length)
		if err != nil {
			return err
		}
		buf = buf[8 : 8+length]
	}
	err := json.Unmarshal(buf, entry)
	return err
}

var errValueTooLong = errors.New("value too long")

const kvDataDir = "data/"
const kvMetaDir = "meta/"
