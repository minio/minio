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
	"time"

	"github.com/minio/minio/cmd/config"
)

// Compression environment variables
const (
	Delay   = "delay"
	MaxWait = "max_wait"
)

// Config represents the heal settings.
type Config struct {
	// Delay is the sleep multiplier.
	Delay float64 `json:"delay"`
	// MaxWait is maximum wait time between operations
	MaxWait time.Duration
}

var (
	// DefaultKVS - default KV config for heal settings
	DefaultKVS = config.KVS{
		config.KV{
			Key:   Delay,
			Value: "10",
		},
		config.KV{
			Key:   MaxWait,
			Value: "15s",
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
		config.HelpKV{
			Key:         MaxWait,
			Description: `maximum wait time between operations, default 15s`,
			Optional:    true,
			Type:        "duration",
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
	wait := kvs.Get(MaxWait)
	cfg.MaxWait, err = time.ParseDuration(wait)
	if err != nil {
		return cfg, err
	}
	return cfg, nil
}
