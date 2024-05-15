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
	"github.com/minio/pkg/v2/env"
)

const (
	envMaxDriveTimeout = "MINIO_DRIVE_MAX_TIMEOUT"
)

// DefaultKVS - default KVS for drive
var DefaultKVS = config.KVS{
	config.KV{
		Key:   MaxTimeout,
		Value: "",
	},
}

var configLk sync.RWMutex

// Config represents the subnet related configuration
type Config struct {
	// MaxTimeout - maximum timeout for a drive operation
	MaxTimeout time.Duration `json:"maxTimeout"`
}

// Update - updates the config with latest values
func (c *Config) Update(new Config) error {
	configLk.Lock()
	defer configLk.Unlock()
	c.MaxTimeout = getMaxTimeout(new.MaxTimeout)
	return nil
}

// GetMaxTimeout - returns the max timeout value.
func (c *Config) GetMaxTimeout() time.Duration {
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
	d := env.Get(envMaxDriveTimeout, kvs.GetWithDefault(MaxTimeout, DefaultKVS))
	if d == "" {
		d = env.Get("_MINIO_DRIVE_MAX_TIMEOUT", "")
		if d == "" {
			d = env.Get("_MINIO_DISK_MAX_TIMEOUT", "")
		}
	}

	dur, _ := time.ParseDuration(d)
	if dur < time.Second {
		cfg.MaxTimeout = 30 * time.Second
	} else {
		cfg.MaxTimeout = getMaxTimeout(dur)
	}
	return cfg, err
}

func getMaxTimeout(t time.Duration) time.Duration {
	if t < time.Second {
		// get default value
		d := env.Get("_MINIO_DRIVE_MAX_TIMEOUT", "")
		if d == "" {
			d = env.Get("_MINIO_DISK_MAX_TIMEOUT", "")
		}
		dur, _ := time.ParseDuration(d)
		if dur < time.Second {
			return 30 * time.Second
		}
		return dur
	}
	return t
}
