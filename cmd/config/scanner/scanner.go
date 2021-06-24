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

package scanner

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// Scanner sub-system constants
const (
	scannerDelay = "delay"

	EnvScannerDelay = "MINIO_DISK_USAGE_CRAWL_DELAY"
)

// DefaultKVS - default scanner config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   scannerDelay,
			Value: "10.0",
		},
	}
)

type Config struct {
	Delay float64 `json:"delay"`
}

// UnmarshalJSON - Validate SS and RRS parity when unmarshalling JSON.
func (sCfg *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(sCfg),
	}
	return json.Unmarshal(data, &aux)
}

// LookupConfig - lookup api config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {

	if err = config.CheckValidKeys(config.ScannerSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	// Check environment variables parameters
	delay, err := strconv.ParseFloat(env.Get(EnvScannerDelay, kvs.Get(scannerDelay)), 64)
	if err != nil {
		return cfg, err
	}

	if delay <= 0 {
		return cfg, errors.New("invalid delay value")
	}

	return Config{
		Delay: delay,
	}, nil
}
