package cmd

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/minio/minio/cmd/logger"
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

var kvMaxValueSize = getKVMaxValueSize()

func getKVMaxValueSize() int {
	str := os.Getenv("MINIO_NKV_MAX_VALUE_SIZE")
	if str == "" {
		return 2 * 1024 * 1024
	}
	valSize, err := strconv.Atoi(str)
	logger.FatalIf(err, "parsing MINIO_NKV_MAX_VALUE_SIZE")
	return valSize
}
