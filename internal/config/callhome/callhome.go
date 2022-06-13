// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package callhome

import (
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// Callhome related keys
const (
	Enable    = "enable"
	Frequency = "frequency"
)

// DefaultKVS - default KV config for subnet settings
var DefaultKVS = config.KVS{
	config.KV{
		Key:   Enable,
		Value: "off",
	},
	config.KV{
		Key:   Frequency,
		Value: "24h",
	},
}

// Config represents the subnet related configuration
type Config struct {
	// Flag indicating whether callhome is enabled.
	Enable bool `json:"enable"`

	// The interval between callhome cycles
	Frequency time.Duration `json:"frequency"`
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.CallhomeSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	cfg.Enable = env.Get(config.EnvMinIOCallhomeEnable,
		kvs.GetWithDefault(Enable, DefaultKVS)) == config.EnableOn
	cfg.Frequency, err = time.ParseDuration(env.Get(config.EnvMinIOCallhomeFrequency,
		kvs.GetWithDefault(Frequency, DefaultKVS)))
	return cfg, err
}
