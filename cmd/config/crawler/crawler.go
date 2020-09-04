package crawler

import (
	"errors"

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

var (
	// DefaultKVS - default KV config for crawler settings
	DefaultKVS = config.KVS{
		config.KV{
			Key:   BitrotScan,
			Value: config.EnableOff,
		},
	}

	Help = config.HelpKVS{
		config.HelpKV{
			Key:         BitrotScan,
			Description: `perform bitrot scan on disks when checking objects during crawl`,
			Optional:    false,
			Type:        "on|off",
		},
	}
)

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.CrawlerSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}
	bitrot := kvs.Get(BitrotScan)
	if bitrot != config.EnableOn && bitrot != config.EnableOff {
		return cfg, errors.New(BitrotScan + ": must be 'on' or 'off'")
	}
	cfg.Bitrot = bitrot == config.EnableOn
	return cfg, nil
}
