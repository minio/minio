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

package cmd

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/cmd/crypto"
)

type cacheControl struct {
	exclude  bool
	expiry   time.Time
	maxAge   int
	sMaxAge  int
	minFresh int
}

// cache exclude directives in cache-control header
var cacheExcludeDirectives = []string{
	"no-cache",
	"no-store",
	"must-revalidate",
}

// returns true if cache exclude directives are set.
func isCacheExcludeDirective(s string) bool {
	for _, directive := range cacheExcludeDirectives {
		if s == directive {
			return true
		}
	}
	return false
}

// returns struct with cache-control settings from user metadata.
func getCacheControlOpts(m map[string]string) (c cacheControl, err error) {
	var headerVal string
	for k, v := range m {
		if k == "cache-control" {
			headerVal = v
		}
		if k == "expires" {
			if e, err := http.ParseTime(v); err == nil {
				c.expiry = e
			}
		}
	}
	if headerVal == "" {
		return
	}
	headerVal = strings.ToLower(headerVal)
	headerVal = strings.TrimSpace(headerVal)

	vals := strings.Split(headerVal, ",")
	for _, val := range vals {
		val = strings.TrimSpace(val)
		p := strings.Split(val, "=")
		if isCacheExcludeDirective(p[0]) {
			c.exclude = true
			continue
		}

		if len(p) != 2 {
			continue
		}
		if p[0] == "max-age" ||
			p[0] == "s-maxage" ||
			p[0] == "min-fresh" {
			i, err := strconv.Atoi(p[1])
			if err != nil {
				return c, err
			}
			if p[0] == "max-age" {
				c.maxAge = i
			}
			if p[0] == "s-maxage" {
				c.sMaxAge = i
			}
			if p[0] == "min-fresh" {
				c.minFresh = i
			}
		}
	}
	return c, nil
}

// return true if metadata has a cache-control header
// directive to exclude object from cache.
func filterFromCache(m map[string]string) bool {
	c, err := getCacheControlOpts(m)
	if err != nil {
		return false
	}
	return c.exclude
}

// returns true if cache expiry conditions met in cache-control/expiry metadata.
func isStaleCache(objInfo ObjectInfo) bool {
	c, err := getCacheControlOpts(objInfo.UserDefined)
	if err != nil {
		return false
	}
	now := time.Now()
	if c.sMaxAge > 0 && c.sMaxAge > int(now.Sub(objInfo.ModTime).Seconds()) {
		return true
	}
	if c.maxAge > 0 && c.maxAge > int(now.Sub(objInfo.ModTime).Seconds()) {
		return true
	}
	if !c.expiry.Equal(time.Time{}) && c.expiry.Before(time.Now()) {
		return true
	}
	if c.minFresh > 0 && c.minFresh <= int(now.Sub(objInfo.ModTime).Seconds()) {
		return true
	}
	return false
}

// backendDownError returns true if err is due to backend failure or faulty disk if in server mode
func backendDownError(err error) bool {
	_, backendDown := err.(BackendDown)
	return backendDown || IsErr(err, baseErrs...)
}

// IsCacheable returns if the object should be saved in the cache.
func (o ObjectInfo) IsCacheable() bool {
	return !crypto.IsEncrypted(o.UserDefined)
}
