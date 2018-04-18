/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package cmd

import (
	"encoding/json"
	"path/filepath"
)

// CacheConfig represents cache config settings
type CacheConfig struct {
	Drives  []string `json:"drives"`
	Expiry  int      `json:"expiry"`
	Exclude []string `json:"exclude"`
}

// UnmarshalJSON - implements JSON unmarshal interface for unmarshalling
// json entries for CacheConfig.
func (cfg *CacheConfig) UnmarshalJSON(data []byte) (err error) {
	type Alias CacheConfig
	var _cfg = &struct {
		*Alias
	}{
		Alias: (*Alias)(cfg),
	}
	if err = json.Unmarshal(data, _cfg); err != nil {
		return err
	}
	if _, err = parseCacheDrives(_cfg.Drives); err != nil {
		return err
	}
	if _, err = parseCacheExcludes(_cfg.Exclude); err != nil {
		return err
	}
	return nil
}

// Parses given cacheDrivesEnv and returns a list of cache drives.
func parseCacheDrives(drives []string) ([]string, error) {
	for _, d := range drives {
		if !filepath.IsAbs(d) {
			return nil, uiErrInvalidCacheDrivesValue(nil).Msg("cache dir should be absolute path: %s", d)
		}
	}
	return drives, nil
}

// Parses given cacheExcludesEnv and returns a list of cache exclude patterns.
func parseCacheExcludes(excludes []string) ([]string, error) {
	for _, e := range excludes {
		if len(e) == 0 {
			return nil, uiErrInvalidCacheExcludesValue(nil).Msg("cache exclude path (%s) cannot be empty", e)
		}
		if hasPrefix(e, slashSeparator) {
			return nil, uiErrInvalidCacheExcludesValue(nil).Msg("cache exclude pattern (%s) cannot start with / as prefix", e)
		}
	}
	return excludes, nil
}
