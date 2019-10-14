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

package compress

import (
	"fmt"
	"strings"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// Config represents the compression settings.
type Config struct {
	Enabled    bool     `json:"enabled"`
	Extensions []string `json:"extensions"`
	MimeTypes  []string `json:"mime-types"`
}

// Compression environment variables
const (
	Extensions = "extensions"
	MimeTypes  = "mime_types"

	EnvCompress           = "MINIO_COMPRESS"
	EnvCompressExtensions = "MINIO_COMPRESS_EXTENSIONS"
	EnvCompressMimeTypes  = "MINIO_COMPRESS_MIME_TYPES"
)

// Parses the given compression exclude list `extensions` or `content-types`.
func parseCompressIncludes(includes []string) ([]string, error) {
	for _, e := range includes {
		if len(e) == 0 {
			return nil, config.ErrInvalidCompressionIncludesValue(nil).Msg("extension/mime-type cannot be empty")
		}
	}
	return includes, nil
}

// LookupConfig - lookup compression config.
func LookupConfig(kvs config.KVS) (Config, error) {
	var err error
	cfg := Config{}
	if compress := env.Get(EnvCompress, kvs.Get(config.State)); compress != "" {
		cfg.Enabled, err = config.ParseBool(compress)
		if err != nil {
			return cfg, err
		}
	}

	compressExtensions := env.Get(EnvCompressExtensions, kvs.Get(Extensions))
	compressMimeTypes := env.Get(EnvCompressMimeTypes, kvs.Get(MimeTypes))
	compressMimeTypesLegacy := env.Get(EnvCompressMimeTypesLegacy, kvs.Get(MimeTypes))
	if compressExtensions != "" || compressMimeTypes != "" || compressMimeTypesLegacy != "" {
		if compressExtensions != "" {
			extensions, err := parseCompressIncludes(strings.Split(compressExtensions, config.ValueSeparator))
			if err != nil {
				return cfg, fmt.Errorf("%s: Invalid MINIO_COMPRESS_EXTENSIONS value (`%s`)", err, extensions)
			}
			cfg.Extensions = extensions
		}
		if compressMimeTypes != "" {
			mimeTypes, err := parseCompressIncludes(strings.Split(compressMimeTypes, config.ValueSeparator))
			if err != nil {
				return cfg, fmt.Errorf("%s: Invalid MINIO_COMPRESS_MIME_TYPES value (`%s`)", err, mimeTypes)
			}
			cfg.MimeTypes = mimeTypes
		}
		if compressMimeTypesLegacy != "" {
			mimeTypes, err := parseCompressIncludes(strings.Split(compressMimeTypesLegacy,
				config.ValueSeparator))
			if err != nil {
				return cfg, fmt.Errorf("%s: Invalid MINIO_COMPRESS_MIME_TYPES value (`%s`)", err, mimeTypes)
			}
			cfg.MimeTypes = mimeTypes
		}
	}

	return cfg, nil
}
