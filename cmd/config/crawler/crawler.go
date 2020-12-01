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
	"strconv"

	"github.com/minio/minio/cmd/config"
)

// Compression environment variables
const (
	Delay = "delay"
)

// Config represents the heal settings.
type Config struct {
	// Bitrot will perform bitrot scan on local disk when checking objects.
	Delay float64 `json:"delay"`
}

var (
	// DefaultKVS - default KV config for heal settings
	DefaultKVS = config.KVS{
		config.KV{
			Key:     Delay,
			Value:   "10",
			Dynamic: true,
		},
	}

	// Help provides help for config values
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Delay,
			Description: `crawler delay multiplier, default 10`,
			Optional:    true,
			Type:        "float",
		},
	}
)

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.CrawlerSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}
	delay := kvs.Get(Delay)
	cfg.Delay, err = strconv.ParseFloat(delay, 64)
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}
