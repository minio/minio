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

package heal

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// Compression environment variables
const (
	Bitrot  = "bitrotscan"
	Sleep   = "max_sleep"
	IOCount = "max_io"

	EnvBitrot  = "MINIO_HEAL_BITROTSCAN"
	EnvSleep   = "MINIO_HEAL_MAX_SLEEP"
	EnvIOCount = "MINIO_HEAL_MAX_IO"
)

var configMutex sync.RWMutex

// Config represents the heal settings.
type Config struct {
	// Bitrot will perform bitrot scan on local disk when checking objects.
	Bitrot bool `json:"bitrotscan"`
	// maximum sleep duration between objects to slow down heal operation.
	Sleep   time.Duration `json:"sleep"`
	IOCount int           `json:"iocount"`
}

// ScanMode returns configured scan mode
func (opts Config) ScanMode() madmin.HealScanMode {
	configMutex.RLock()
	defer configMutex.RUnlock()
	if opts.Bitrot {
		return madmin.HealDeepScan
	}
	return madmin.HealNormalScan
}

// Wait waits for IOCount to go down or max sleep to elapse before returning.
// usually used in healing paths to wait for specified amount of time to
// throttle healing.
func (opts Config) Wait(currentIO func() int, systemIO func() int) {
	configMutex.RLock()
	maxIO, maxWait := opts.IOCount, opts.Sleep
	configMutex.RUnlock()

	// No need to wait run at full speed.
	if maxIO <= 0 {
		return
	}

	// At max 10 attempts to wait with 100 millisecond interval before proceeding
	waitTick := 100 * time.Millisecond

	tmpMaxWait := maxWait

	if currentIO != nil {
		for currentIO() >= maxIO+systemIO() {
			if tmpMaxWait > 0 {
				if tmpMaxWait < waitTick {
					time.Sleep(tmpMaxWait)
				} else {
					time.Sleep(waitTick)
				}
				tmpMaxWait = tmpMaxWait - waitTick
			}
			if tmpMaxWait <= 0 {
				return
			}
		}
	}
}

// Update updates opts with nopts
func (opts *Config) Update(nopts Config) {
	configMutex.Lock()
	defer configMutex.Unlock()

	opts.Bitrot = nopts.Bitrot
	opts.IOCount = nopts.IOCount
	opts.Sleep = nopts.Sleep
}

var (
	// DefaultKVS - default KV config for heal settings
	DefaultKVS = config.KVS{
		config.KV{
			Key:   Bitrot,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   Sleep,
			Value: "1s",
		},
		config.KV{
			Key:   IOCount,
			Value: "100",
		},
	}

	// Help provides help for config values
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Bitrot,
			Description: `perform bitrot scan on disks when checking objects during scanner`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         Sleep,
			Description: `maximum sleep duration between objects to slow down heal operation. eg. 2s`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         IOCount,
			Description: `maximum IO requests allowed between objects to slow down heal operation. eg. 3`,
			Optional:    true,
			Type:        "int",
		},
	}
)

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.HealSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}
	cfg.Bitrot, err = config.ParseBool(env.Get(EnvBitrot, kvs.GetWithDefault(Bitrot, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'heal:bitrotscan' value invalid: %w", err)
	}
	cfg.Sleep, err = time.ParseDuration(env.Get(EnvSleep, kvs.GetWithDefault(Sleep, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'heal:max_sleep' value invalid: %w", err)
	}
	cfg.IOCount, err = strconv.Atoi(env.Get(EnvIOCount, kvs.GetWithDefault(IOCount, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'heal:max_io' value invalid: %w", err)
	}
	return cfg, nil
}
