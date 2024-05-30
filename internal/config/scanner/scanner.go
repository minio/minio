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

package scanner

import (
	"fmt"
	"strconv"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

// Compression environment variables
const (
	Speed    = "speed"
	EnvSpeed = "MINIO_SCANNER_SPEED"

	IdleSpeed    = "idle_speed"
	EnvIdleSpeed = "MINIO_SCANNER_IDLE_SPEED"

	ExcessVersions    = "alert_excess_versions"
	EnvExcessVersions = "MINIO_SCANNER_ALERT_EXCESS_VERSIONS"

	ExcessFolders    = "alert_excess_folders"
	EnvExcessFolders = "MINIO_SCANNER_ALERT_EXCESS_FOLDERS"

	// All below are deprecated in October 2022 and
	// replaced them with a single speed parameter
	Delay            = "delay"
	MaxWait          = "max_wait"
	Cycle            = "cycle"
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

	// Sleep always or based on incoming S3 requests.
	IdleMode int32 // 0 => on, 1 => off

	// Alert upon this many excess object versions
	ExcessVersions int64 // 100

	// Alert upon this many excess sub-folders per folder in an erasure set.
	ExcessFolders int64 // 50000

	// MaxWait is maximum wait time between operations
	MaxWait time.Duration
	// Cycle is the time.Duration between each scanner cycles
	Cycle time.Duration
}

// DefaultKVS - default KV config for heal settings
var DefaultKVS = config.KVS{
	config.KV{
		Key:   Speed,
		Value: "default",
	},
	config.KV{
		Key:           IdleSpeed,
		Value:         "",
		HiddenIfEmpty: true,
	},
	config.KV{
		Key:   ExcessVersions,
		Value: "100",
	},
	config.KV{
		Key:   ExcessFolders,
		Value: "50000",
	},

	// Deprecated Oct 2022
	config.KV{
		Key:           Delay,
		Value:         "",
		HiddenIfEmpty: true,
	},
	// Deprecated Oct 2022
	config.KV{
		Key:           MaxWait,
		Value:         "",
		HiddenIfEmpty: true,
	},
	// Deprecated Oct 2022
	config.KV{
		Key:           Cycle,
		Value:         "",
		HiddenIfEmpty: true,
	},
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	cfg = Config{
		ExcessVersions: 100,
		ExcessFolders:  50000,
		IdleMode:       0, // Default is on
	}

	if err = config.CheckValidKeys(config.ScannerSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	excessVersions, err := strconv.ParseInt(env.Get(EnvExcessVersions, kvs.GetWithDefault(ExcessVersions, DefaultKVS)), 10, 64)
	if err != nil {
		return cfg, err
	}
	cfg.ExcessVersions = excessVersions

	excessFolders, err := strconv.ParseInt(env.Get(EnvExcessFolders, kvs.GetWithDefault(ExcessFolders, DefaultKVS)), 10, 64)
	if err != nil {
		return cfg, err
	}
	cfg.ExcessFolders = excessFolders

	switch idleSpeed := env.Get(EnvIdleSpeed, kvs.GetWithDefault(IdleSpeed, DefaultKVS)); idleSpeed {
	case "", config.EnableOn:
		cfg.IdleMode = 0
	case config.EnableOff:
		cfg.IdleMode = 1
	default:
		return cfg, fmt.Errorf("unknown value: '%s'", idleSpeed)
	}

	// Stick to loading deprecated config/env if they are already set, and the Speed value
	// has not been changed from its "default" value, if it has been changed honor new settings.
	if kvs.GetWithDefault(Speed, DefaultKVS) == "default" {
		if kvs.Get(Delay) != "" && kvs.Get(MaxWait) != "" {
			if err = lookupDeprecatedScannerConfig(kvs, &cfg); err != nil {
				return cfg, err
			}
		}
	}

	switch speed := env.Get(EnvSpeed, kvs.GetWithDefault(Speed, DefaultKVS)); speed {
	case "fastest":
		cfg.Delay, cfg.MaxWait, cfg.Cycle = 0, 0, time.Second
	case "fast":
		cfg.Delay, cfg.MaxWait, cfg.Cycle = 1, 100*time.Millisecond, time.Minute
	case "default":
		cfg.Delay, cfg.MaxWait, cfg.Cycle = 2, time.Second, time.Minute
	case "slow":
		cfg.Delay, cfg.MaxWait, cfg.Cycle = 10, 15*time.Second, time.Minute
	case "slowest":
		cfg.Delay, cfg.MaxWait, cfg.Cycle = 100, 15*time.Second, 30*time.Minute
	default:
		return cfg, fmt.Errorf("unknown '%s' value", speed)
	}

	return cfg, nil
}

func lookupDeprecatedScannerConfig(kvs config.KVS, cfg *Config) (err error) {
	delay := env.Get(EnvDelayLegacy, "")
	if delay == "" {
		delay = env.Get(EnvDelay, kvs.GetWithDefault(Delay, DefaultKVS))
	}
	cfg.Delay, err = strconv.ParseFloat(delay, 64)
	if err != nil {
		return err
	}
	maxWait := env.Get(EnvMaxWaitLegacy, "")
	if maxWait == "" {
		maxWait = env.Get(EnvMaxWait, kvs.GetWithDefault(MaxWait, DefaultKVS))
	}
	cfg.MaxWait, err = time.ParseDuration(maxWait)
	if err != nil {
		return err
	}
	cycle := env.Get(EnvCycle, kvs.GetWithDefault(Cycle, DefaultKVS))
	if cycle == "" {
		cycle = "1m"
	}
	cfg.Cycle, err = time.ParseDuration(cycle)
	if err != nil {
		return err
	}
	return nil
}
