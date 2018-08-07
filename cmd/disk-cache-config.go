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
	"errors"
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/ellipses"
)

// CacheConfig represents cache config settings
type CacheConfig struct {
	Drives  []string `json:"drives"`
	Expiry  int      `json:"expiry"`
	MaxUse  int      `json:"maxuse"`
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

	if _cfg.Expiry < 0 {
		return errors.New("config expiry value should not be negative")
	}

	if _cfg.MaxUse < 0 {
		return errors.New("config max use value should not be null or negative")
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
	if len(drives) == 0 {
		return drives, nil
	}
	var endpoints []string
	for _, d := range drives {
		if ellipses.HasEllipses(d) {
			s, err := parseCacheDrivePaths(d)
			if err != nil {
				return nil, err
			}
			endpoints = append(endpoints, s...)
		} else {
			endpoints = append(endpoints, d)
		}
	}

	for _, d := range endpoints {
		if !filepath.IsAbs(d) {
			return nil, uiErrInvalidCacheDrivesValue(nil).Msg("cache dir should be absolute path: %s", d)
		}
	}
	return endpoints, nil
}

// Parses all arguments and returns a slice of drive paths following the ellipses pattern.
func parseCacheDrivePaths(arg string) (ep []string, err error) {
	patterns, perr := ellipses.FindEllipsesPatterns(arg)
	if perr != nil {
		return []string{}, uiErrInvalidCacheDrivesValue(nil).Msg(perr.Error())
	}

	for _, lbls := range patterns.Expand() {
		ep = append(ep, strings.Join(lbls, ""))
	}

	return ep, nil
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
