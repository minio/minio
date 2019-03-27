package cmd

import (
	"encoding/json"
	"errors"
	"time"
)

type KVInterface interface {
	Put(keyStr string, value []byte) error
	Get(keyStr string, value []byte) ([]byte, error)
	Delete(keyStr string) error
}

const kvNSEntryPaddingMultiple = 4 * 1024

type KVNSEntry struct {
	Key     string
	Size    int64
	ModTime time.Time
	IDs     []string
}

func KVNSEntryMarshal(entry KVNSEntry) ([]byte, error) {
	b, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	if !kvPadding {
		return b, nil
	}
	padded := make([]byte, ceilFrac(int64(len(b)), kvNSEntryPaddingMultiple)*kvNSEntryPaddingMultiple)
	copy(padded, b)
	return padded, nil
}

func KVNSEntryUnmarshal(b []byte, entry *KVNSEntry) error {
	if kvPadding {
		for i := range b {
			if b[i] == '\x00' {
				b = b[:i]
				break
			}
		}
	}
	return json.Unmarshal(b, entry)
}

var errValueTooLong = errors.New("value too long")

const kvDataDir = "data/"
const kvMetaDir = "meta/"
