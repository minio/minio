// Copyright (c) 2015-2021 MinIO, Inc.
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
	"github.com/minio/pkg/v2/env"
	"sync"
	"time"

	"github.com/minio/minio/internal/config"
)

var DefaultKVS = config.KVS{
	config.KV{
		Key:   MaxTimeout,
		Value: "",
	},
}

// Config represents the subnet related configuration
type Config struct {
	// Flag indicating whether callhome is enabled.
	MaxTimeout time.Duration `json:"maxTimeout"`
}

var mutex sync.RWMutex

func (c *Config) Update(new Config) error {
	mutex.Lock()
	defer mutex.Unlock()
	c.MaxTimeout = getMaxTimeout(new.MaxTimeout)
	return nil
}

func (c *Config) GetMaxTimeout() time.Duration {
	mutex.RLock()
	defer mutex.RUnlock()
	return getMaxTimeout(c.MaxTimeout)
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.DriveSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}
	d := kvs.GetWithDefault(MaxTimeout, DefaultKVS)
	if d == "" {
		d = env.Get("_MINIO_DRIVE_MAX_TIMEOUT", "")
		if d == "" {
			d = env.Get("_MINIO_DISK_MAX_TIMEOUT", "")
		}
	}
	if d == "" {
		d = "2m"
	}
	dur, _ := time.ParseDuration(d)
	cfg.MaxTimeout = getMaxTimeout(dur)
	return cfg, err
}

func getMaxTimeout(t time.Duration) time.Duration {
	if t < time.Second {
		return time.Minute * 2
	}
	return t
}
