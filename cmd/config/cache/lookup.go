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
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// Cache ENVs
const (
	EnvCacheDrives              = "MINIO_CACHE_DRIVES"
	EnvCacheExclude             = "MINIO_CACHE_EXCLUDE"
	EnvCacheExpiry              = "MINIO_CACHE_EXPIRY"
	EnvCacheMaxUse              = "MINIO_CACHE_MAXUSE"
	EnvCacheEncryptionMasterKey = "MINIO_CACHE_ENCRYPTION_MASTER_KEY"
)

const (
	cacheEnvDelimiter = ";"
)

// LookupConfig - extracts cache configuration provided by environment
// variables and merge them with provided CacheConfiguration.
func LookupConfig(cfg Config) (Config, error) {
	if drives := env.Get(EnvCacheDrives, strings.Join(cfg.Drives, ",")); drives != "" {
		driveList, err := parseCacheDrives(strings.Split(drives, cacheEnvDelimiter))
		if err != nil {
			return cfg, err
		}
		cfg.Drives = driveList
	}

	if excludes := env.Get(EnvCacheExclude, strings.Join(cfg.Exclude, ",")); excludes != "" {
		excludeList, err := parseCacheExcludes(strings.Split(excludes, cacheEnvDelimiter))
		if err != nil {
			return cfg, err
		}
		cfg.Exclude = excludeList
	}

	if expiryStr := env.Get(EnvCacheExpiry, strconv.Itoa(cfg.Expiry)); expiryStr != "" {
		expiry, err := strconv.Atoi(expiryStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheExpiryValue(err)
		}
		cfg.Expiry = expiry
	}

	if maxUseStr := env.Get(EnvCacheMaxUse, strconv.Itoa(cfg.MaxUse)); maxUseStr != "" {
		maxUse, err := strconv.Atoi(maxUseStr)
		if err != nil {
			return cfg, config.ErrInvalidCacheMaxUse(err)
		}
		// maxUse should be a valid percentage.
		if maxUse > 0 && maxUse <= 100 {
			cfg.MaxUse = maxUse
		}
	}

	return cfg, nil
}
