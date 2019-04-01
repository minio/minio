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
	"fmt"
	"strconv"
	"strings"

	config "github.com/minio/minio/pkg/server-config"
)

// Parses the given compression exclude list `extensions` or `content-types`.
func parseCompressIncludes(includes []string) ([]string, error) {
	uiErr := uiErrInvalidCompressionIncludesValue(nil).Msg("extension/mime-type cannot be empty")
	for _, e := range includes {
		if len(e) == 0 {
			return nil, uiErr
		}
	}
	return includes, nil
}

// parseBool returns the boolean value represented by the string.
// It accepts 1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False.
// Any other value returns an error.
func parseBool(str string) (bool, error) {
	switch str {
	case "1", "t", "T", "true", "TRUE", "True", "on", "ON", "On":
		return true, nil
	case "0", "f", "F", "false", "FALSE", "False", "off", "OFF", "Off":
		return false, nil
	}
	return false, fmt.Errorf("ParseBool: parsing '%s': %s", str, strconv.ErrSyntax)
}

func parseStorageClassKVS(kvs config.KVS) (storageClass, storageClass, error) {
	if kvs.Get(config.State) != config.StateEnabled {
		return storageClass{}, storageClass{}, nil
	}
	standardSC, err := parseStorageClass(kvs.Get(config.StorageClassStandard))
	if err != nil {
		return storageClass{}, storageClass{}, err
	}
	rrsSC, err := parseStorageClass(kvs.Get(config.StorageClassRRS))
	if err != nil {
		return storageClass{}, storageClass{}, err
	}
	return standardSC, rrsSC, nil
}

func parseCacheConfigKVS(kvs config.KVS) (CacheConfig, error) {
	if kvs.Get(config.State) != config.StateEnabled {
		return CacheConfig{}, nil
	}
	if len(kvs.Get(config.CacheDrives)) == 0 {
		return CacheConfig{}, errInvalidArgument
	}
	cconfig := CacheConfig{
		Drives:  strings.Split(kvs.Get(config.CacheDrives), ","),
		Exclude: strings.Split(kvs.Get(config.CacheExclude), ","),
	}
	var err error
	cconfig.Expiry, err = strconv.Atoi(kvs.Get(config.CacheExpiry))
	if err != nil {
		return cconfig, err
	}
	cconfig.Quota, err = strconv.Atoi(kvs.Get(config.CacheQuota))
	if err != nil {
		return cconfig, err
	}
	return cconfig, nil
}

func parseCompressionKVS(kvs config.KVS) (compressionConfig, error) {
	if kvs.Get(config.State) != config.StateEnabled {
		return compressionConfig{}, nil
	}
	extensions, err := parseCompressIncludes(strings.Split(kvs.Get(config.CompressionExtensions), ","))
	if err != nil {
		return compressionConfig{}, err
	}
	contenttypes, err := parseCompressIncludes(strings.Split(kvs.Get(config.CompressionMimeTypes), ","))
	if err != nil {
		return compressionConfig{}, err
	}
	return compressionConfig{
		Extensions: extensions,
		MimeTypes:  contenttypes,
		Enabled:    true,
	}, nil
}
