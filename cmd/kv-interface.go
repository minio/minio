package cmd

import (
	"errors"
	"time"
)

type KVInterface interface {
	Put(keyStr string, value []byte) error
	Get(keyStr string) ([]byte, error)
	Delete(keyStr string) error
}

type KVNSEntry struct {
	Size    int64
	ModTime time.Time
	IDs     []string
}

var errValueTooLong = errors.New("value too long")

const kvDataDir = ".minio.sys/.data"
const kvMaxValueSize = 2 * 1024 * 1024
