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

package cache

import (
	"errors"
	"strconv"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// Cache ENVs
const (
	Drives        = "drives"
	Exclude       = "exclude"
	Expiry        = "expiry"
	MaxUse        = "maxuse"
	Quota         = "quota"
	After         = "after"
	WatermarkLow  = "watermark_low"
	WatermarkHigh = "watermark_high"
	Range         = "range"
	Commit        = "commit"

	EnvCacheDrives        = "MINIO_CACHE_DRIVES"
	EnvCacheExclude       = "MINIO_CACHE_EXCLUDE"
	EnvCacheExpiry        = "MINIO_CACHE_EXPIRY"
	EnvCacheMaxUse        = "MINIO_CACHE_MAXUSE"
	EnvCacheQuota         = "MINIO_CACHE_QUOTA"
	EnvCacheAfter         = "MINIO_CACHE_AFTER"
	EnvCacheWatermarkLow  = "MINIO_CACHE_WATERMARK_LOW"
	EnvCacheWatermarkHigh = "MINIO_CACHE_WATERMARK_HIGH"
	EnvCacheRange         = "MINIO_CACHE_RANGE"
	EnvCacheCommit        = "MINIO_CACHE_COMMIT"

	EnvCacheEncryptionKey = "MINIO_CACHE_ENCRYPTION_SECRET_KEY"

	DefaultExpiry        = "90"
	DefaultQuota         = "80"
	DefaultAfter         = "0"
	DefaultWaterMarkLow  = "70"
	DefaultWaterMarkHigh = "80"
	DefaultCacheCommit   = "writethrough"
)

// DefaultKVS - default KV settings for caching.
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   Drives,
			Value: "",
		},
		config.KV{
			Key:   Exclude,
			Value: "",
		},
		config.KV{
			Key:   Expiry,
			Value: DefaultExpiry,
		},
		config.KV{
			Key:   Quota,
			Value: DefaultQuota,
		},
		config.KV{
			Key:   After,
			Value: DefaultAfter,
		},
		config.KV{
			Key:   WatermarkLow,
			Value: DefaultWaterMarkLow,
		},
		config.KV{
			Key:   WatermarkHigh,
			Value: DefaultWaterMarkHigh,
		},
		config.KV{
			Key:   Range,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   Commit,
			Value: DefaultCacheCommit,
		},
	}
)

const (
	cacheDelimiter = ","
)

// Enabled returns if cache is enabled.
func Enabled(kvs config.KVS) bool {
	drives := kvs.Get(Drives)
	return drives != ""
}

// LookupConfig - extracts cache configuration provided by environment
// variables and merge them with provided CacheConfiguration.
func LookupConfig(kvs config.KVS) (Config, error) {
	cfg := Config{}
	if err := config.CheckValidKeys(config.CacheSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	drives := env.Get(EnvCacheDrives, kvs.Get(Drives))
	if len(drives) == 0 {
		return cfg, nil
	}

	var err error
	cfg.Drives, err = parseCacheDrives(drives)
	if err != nil {
		return cfg, err
	}

	cfg.Enabled = true
	if excludes := env.Get(EnvCacheExclude, kvs.Get(Exclude)); excludes != "" {
		cfg.Exclude, err = parseCacheExcludes(excludes)
		if err != nil {
			return cfg, err
		}
	}

	if expiryStr := env.Get(EnvCacheExpiry, kvs.Get(Expiry)); expiryStr != "" {
		cfg.Expiry, err = strconv.Atoi(expiryStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheExpiryValue(err)
		}
	}

	if maxUseStr := env.Get(EnvCacheMaxUse, kvs.Get(MaxUse)); maxUseStr != "" {
		cfg.MaxUse, err = strconv.Atoi(maxUseStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheQuota(err)
		}
		// maxUse should be a valid percentage.
		if cfg.MaxUse < 0 || cfg.MaxUse > 100 {
			err := errors.New("config max use value should not be null or negative")
			return cfg, config.ErrInvalidCacheQuota(err)
		}
		cfg.Quota = cfg.MaxUse
	} else if quotaStr := env.Get(EnvCacheQuota, kvs.Get(Quota)); quotaStr != "" {
		cfg.Quota, err = strconv.Atoi(quotaStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheQuota(err)
		}
		// quota should be a valid percentage.
		if cfg.Quota < 0 || cfg.Quota > 100 {
			err := errors.New("config quota value should not be null or negative")
			return cfg, config.ErrInvalidCacheQuota(err)
		}
		cfg.MaxUse = cfg.Quota
	}

	if afterStr := env.Get(EnvCacheAfter, kvs.Get(After)); afterStr != "" {
		cfg.After, err = strconv.Atoi(afterStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheAfter(err)
		}
		// after should be a valid value >= 0.
		if cfg.After < 0 {
			err := errors.New("cache after value cannot be less than 0")
			return cfg, config.ErrInvalidCacheAfter(err)
		}
	}

	if lowWMStr := env.Get(EnvCacheWatermarkLow, kvs.Get(WatermarkLow)); lowWMStr != "" {
		cfg.WatermarkLow, err = strconv.Atoi(lowWMStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheWatermarkLow(err)
		}
		// WatermarkLow should be a valid percentage.
		if cfg.WatermarkLow < 0 || cfg.WatermarkLow > 100 {
			err := errors.New("config min watermark value should be between 0 and 100")
			return cfg, config.ErrInvalidCacheWatermarkLow(err)
		}
	}

	if highWMStr := env.Get(EnvCacheWatermarkHigh, kvs.Get(WatermarkHigh)); highWMStr != "" {
		cfg.WatermarkHigh, err = strconv.Atoi(highWMStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheWatermarkHigh(err)
		}

		// MaxWatermark should be a valid percentage.
		if cfg.WatermarkHigh < 0 || cfg.WatermarkHigh > 100 {
			err := errors.New("config high watermark value should be between 0 and 100")
			return cfg, config.ErrInvalidCacheWatermarkHigh(err)
		}
	}
	if cfg.WatermarkLow > cfg.WatermarkHigh {
		err := errors.New("config high watermark value should be greater than low watermark value")
		return cfg, config.ErrInvalidCacheWatermarkHigh(err)
	}

	cfg.Range = true // by default range caching is enabled.
	if rangeStr := env.Get(EnvCacheRange, kvs.Get(Range)); rangeStr != "" {
		rng, err := config.ParseBool(rangeStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheRange(err)
		}
		cfg.Range = rng
	}
	if commit := env.Get(EnvCacheCommit, kvs.Get(Commit)); commit != "" {
		cfg.CommitWriteback, err = parseCacheCommitMode(commit)
		if err != nil {
			return cfg, err
		}
		if cfg.After > 0 && cfg.CommitWriteback {
			err := errors.New("cache after cannot be used with commit writeback")
			return cfg, config.ErrInvalidCacheSetting(err)
		}
	}

	return cfg, nil
}
