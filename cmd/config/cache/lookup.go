/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package cache

import (
	"errors"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// Cache ENVs
const (
	Drives  = "drives"
	Exclude = "exclude"
	Expiry  = "expiry"
	MaxUse  = "maxuse"
	Quota   = "quota"

	EnvCacheState               = "MINIO_CACHE_STATE"
	EnvCacheDrives              = "MINIO_CACHE_DRIVES"
	EnvCacheExclude             = "MINIO_CACHE_EXCLUDE"
	EnvCacheExpiry              = "MINIO_CACHE_EXPIRY"
	EnvCacheMaxUse              = "MINIO_CACHE_MAXUSE"
	EnvCacheQuota               = "MINIO_CACHE_QUOTA"
	EnvCacheEncryptionMasterKey = "MINIO_CACHE_ENCRYPTION_MASTER_KEY"

	DefaultExpiry = "90"
	DefaultQuota  = "80"
)

// DefaultKVS - default KV settings for caching.
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   config.State,
			Value: config.StateOff,
		},
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
	}
)

const (
	cacheDelimiter = ","
)

// LookupConfig - extracts cache configuration provided by environment
// variables and merge them with provided CacheConfiguration.
func LookupConfig(kvs config.KVS) (Config, error) {
	cfg := Config{}

	if err := config.CheckValidKeys(config.CacheSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	// Check if cache is explicitly disabled
	stateBool, err := config.ParseBool(env.Get(EnvCacheState, kvs.Get(config.State)))
	if err != nil {
		// Parsing failures happen due to empty KVS, ignore it.
		if kvs.Empty() {
			return cfg, nil
		}
		return cfg, err
	}

	drives := env.Get(EnvCacheDrives, kvs.Get(Drives))
	if stateBool {
		if len(drives) == 0 {
			return cfg, config.Error("'drives' key cannot be empty if you wish to enable caching")
		}
	}
	if len(drives) == 0 {
		return cfg, nil
	}

	cfg.Drives, err = parseCacheDrives(strings.Split(drives, cacheDelimiter))
	if err != nil {
		cfg.Drives, err = parseCacheDrives(strings.Split(drives, cacheDelimiterLegacy))
		if err != nil {
			return cfg, err
		}
	}

	cfg.Enabled = true
	if excludes := env.Get(EnvCacheExclude, kvs.Get(Exclude)); excludes != "" {
		cfg.Exclude, err = parseCacheExcludes(strings.Split(excludes, cacheDelimiter))
		if err != nil {
			cfg.Exclude, err = parseCacheExcludes(strings.Split(excludes, cacheDelimiterLegacy))
			if err != nil {
				return cfg, err
			}
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
	}

	if quotaStr := env.Get(EnvCacheQuota, kvs.Get(Quota)); quotaStr != "" {
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

	return cfg, nil
}
