/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package crawler

import (
	"errors"

	"github.com/minio/minio/cmd/config"
)

// Compression environment variables
const (
	BitrotScan = "bitrotscan"
)

// Config represents the crawler settings.
type Config struct {
	// Bitrot will perform bitrot scan on local disk when checking objects.
	Bitrot bool `json:"bitrotscan"`
}

var (
	// DefaultKVS - default KV config for crawler settings
	DefaultKVS = config.KVS{
		config.KV{
			Key:   BitrotScan,
			Value: config.EnableOff,
		},
	}

	// Help provides help for config values
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         BitrotScan,
			Description: `perform bitrot scan on disks when checking objects during crawl`,
			Optional:    true,
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
