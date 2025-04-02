// Copyright (c) 2015-2023 MinIO, Inc.
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

package drive

import (
	"sync"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

// Drive specific timeout environment variables
const (
	EnvMaxDriveTimeout       = "MINIO_DRIVE_MAX_TIMEOUT"
	EnvMaxDriveTimeoutLegacy = "_MINIO_DRIVE_MAX_TIMEOUT"
	EnvMaxDiskTimeoutLegacy  = "_MINIO_DISK_MAX_TIMEOUT"
)

// DefaultKVS - default KVS for drive
var DefaultKVS = config.KVS{
	config.KV{
		Key:   MaxTimeout,
		Value: "30s",
	},
}

var configLk sync.RWMutex

// Config represents the subnet related configuration
type Config struct {
	// MaxTimeout - maximum timeout for a drive operation
	MaxTimeout time.Duration `json:"maxTimeout"`
}

// Update - updates the config with latest values
func (c *Config) Update(updated Config) error {
	configLk.Lock()
	defer configLk.Unlock()
	c.MaxTimeout = getMaxTimeout(updated.MaxTimeout)
	return nil
}

// GetMaxTimeout - returns the per call drive operation timeout
func (c *Config) GetMaxTimeout() time.Duration {
	return c.GetOPTimeout()
}

// GetOPTimeout - returns the per call drive operation timeout
func (c *Config) GetOPTimeout() time.Duration {
	configLk.RLock()
	defer configLk.RUnlock()

	return getMaxTimeout(c.MaxTimeout)
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	cfg = Config{
		MaxTimeout: 30 * time.Second,
	}
	if err = config.CheckValidKeys(config.DriveSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	// if not set. Get default value from environment
	d := env.Get(EnvMaxDriveTimeout, env.Get(EnvMaxDriveTimeoutLegacy, env.Get(EnvMaxDiskTimeoutLegacy, kvs.GetWithDefault(MaxTimeout, DefaultKVS))))
	if d == "" {
		cfg.MaxTimeout = 30 * time.Second
	} else {
		dur, _ := time.ParseDuration(d)
		if dur < time.Second {
			cfg.MaxTimeout = 30 * time.Second
		} else {
			cfg.MaxTimeout = getMaxTimeout(dur)
		}
	}
	return cfg, err
}

func getMaxTimeout(t time.Duration) time.Duration {
	if t > time.Second {
		return t
	}
	// get default value
	d := env.Get(EnvMaxDriveTimeoutLegacy, env.Get(EnvMaxDiskTimeoutLegacy, ""))
	if d == "" {
		return 30 * time.Second
	}
	dur, _ := time.ParseDuration(d)
	if dur < time.Second {
		return 30 * time.Second
	}
	return dur
}
