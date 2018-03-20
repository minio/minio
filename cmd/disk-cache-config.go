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
	"strconv"
	"strings"

	"errors"
)

// CacheConfig represents cache config settings
type CacheConfig struct {
	Drives  []string
	Expiry  int
	Exclude []string
}

// Parses given cacheDrivesEnv and returns a list of cache drives.
func parseCacheDrives(cacheDrivesEnv string) ([]string, error) {
	cacheDrivesEnv = strings.ToLower(cacheDrivesEnv)
	s := strings.Split(cacheDrivesEnv, ";")
	c2 := make([]string, 0)
	for _, d := range s {
		if len(d) > 0 {
			c2 = append(c2, d)
		}
	}
	return c2, nil
}

// Parses given cacheExcludesEnv and returns a list of cache exclude patterns.
func parseCacheExcludes(cacheExcludesEnv string) ([]string, error) {
	s := strings.Split(cacheExcludesEnv, ";")
	c2 := make([]string, 0)
	for _, e := range s {
		if len(e) > 0 {
			if strings.HasPrefix(e, "/") {
				return c2, errors.New("cache exclude patterns cannot start with / as prefix " + e)
			}
			c2 = append(c2, e)
		}
	}
	return c2, nil
}

// Parses given cacheExpiryEnv and returns cache expiry in days.
func parseCacheExpiry(cacheExpiryEnv string) (int, error) {
	return strconv.Atoi(cacheExpiryEnv)
}
