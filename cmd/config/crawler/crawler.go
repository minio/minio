package crawler

import (
	"github.com/minio/minio/cmd/config"
)

// Compression environment variables
const (
	BitrotScan = "bitrot"
)

// Config represents the crawler settings.
type Config struct {
	// Bitrot will perform bitrot scan on local disk when checking objects.
	Bitrot bool `json:"bitrot"`
}

// DefaultKVS - default KV config for crawler settings
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   BitrotScan,
			Value: config.EnableOff,
		},
	}
)
