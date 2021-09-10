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
	"fmt"
	"strings"

	"github.com/minio/minio/internal/config"
)

const (
	cacheDelimiterLegacy = ";"
)

// SetCacheConfig - One time migration code needed, for migrating from older config to new for Cache.
func SetCacheConfig(s config.Config, cfg Config) {
	if len(cfg.Drives) == 0 {
		// Do not save cache if no settings available.
		return
	}
	s[config.CacheSubSys][config.Default] = config.KVS{
		config.KV{
			Key:   Drives,
			Value: strings.Join(cfg.Drives, cacheDelimiter),
		},
		config.KV{
			Key:   Exclude,
			Value: strings.Join(cfg.Exclude, cacheDelimiter),
		},
		config.KV{
			Key:   Expiry,
			Value: fmt.Sprintf("%d", cfg.Expiry),
		},
		config.KV{
			Key:   Quota,
			Value: fmt.Sprintf("%d", cfg.MaxUse),
		},
	}
}
