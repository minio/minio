/*
 * MinIO Cloud Storage, (C) 2020-2021 MinIO, Inc.
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

package scanner

import (
	"strconv"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// Compression environment variables
const (
	Delay   = "delay"
	MaxWait = "max_wait"
	Cycle   = "cycle"

	EnvDelay         = "MINIO_SCANNER_DELAY"
	EnvCycle         = "MINIO_SCANNER_CYCLE"
	EnvDelayLegacy   = "MINIO_CRAWLER_DELAY"
	EnvMaxWait       = "MINIO_SCANNER_MAX_WAIT"
	EnvMaxWaitLegacy = "MINIO_CRAWLER_MAX_WAIT"
)

// Config represents the heal settings.
type Config struct {
	// Delay is the sleep multiplier.
	Delay float64 `json:"delay"`
	// MaxWait is maximum wait time between operations
	MaxWait time.Duration
	// Cycle is the time.Duration between each scanner cycles
	Cycle time.Duration
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
		config.KV{
			Key:   Cycle,
			Value: "1m",
		},
	}

	// Help provides help for config values
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Delay,
			Description: `scanner delay multiplier, defaults to '10.0'`,
			Optional:    true,
			Type:        "float",
		},
		config.HelpKV{
			Key:         MaxWait,
			Description: `maximum wait time between operations, defaults to '15s'`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         Cycle,
			Description: `time duration between scanner cycles, defaults to '1m'`,
			Optional:    true,
			Type:        "duration",
		},
	}
)

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.ScannerSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}
	delay := env.Get(EnvDelayLegacy, "")
	if delay == "" {
		delay = env.Get(EnvDelay, kvs.Get(Delay))
	}
	cfg.Delay, err = strconv.ParseFloat(delay, 64)
	if err != nil {
		return cfg, err
	}
	maxWait := env.Get(EnvMaxWaitLegacy, "")
	if maxWait == "" {
		maxWait = env.Get(EnvMaxWait, kvs.Get(MaxWait))
	}
	cfg.MaxWait, err = time.ParseDuration(maxWait)
	if err != nil {
		return cfg, err
	}

	cfg.Cycle, err = time.ParseDuration(env.Get(EnvCycle, kvs.Get(Cycle)))
	if err != nil {
		return cfg, err
	}
	return cfg, nil
}
