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
	"strconv"
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
	EnvMinioCompress           = "MINIO_COMPRESS"
	EnvMinioCompressExtensions = "MINIO_COMPRESS_EXTENSIONS"
	EnvMinioCompressMimeTypes  = "MINIO_COMPRESS_MIMETYPES"
)

// Parses the given compression exclude list `extensions` or `content-types`.
func parseCompressIncludes(includes []string) ([]string, error) {
	for _, e := range includes {
		if len(e) == 0 {
			return nil, config.ErrInvalidCompressionIncludesValue(nil).Msg("extension/mime-type (%s) cannot be empty", e)
		}
	}
	return includes, nil
}

// LookupConfig - lookup compression config.
func LookupConfig(cfg Config) (Config, error) {
	const compressEnvDelimiter = ","
	if compress := env.Get(EnvMinioCompress, strconv.FormatBool(cfg.Enabled)); compress != "" {
		cfg.Enabled = strings.EqualFold(compress, "true")
	}

	compressExtensions := env.Get(EnvMinioCompressExtensions, strings.Join(cfg.Extensions, ","))
	compressMimeTypes := env.Get(EnvMinioCompressMimeTypes, strings.Join(cfg.MimeTypes, ","))
	if compressExtensions != "" || compressMimeTypes != "" {
		if compressExtensions != "" {
			extensions, err := parseCompressIncludes(strings.Split(compressExtensions, compressEnvDelimiter))
			if err != nil {
				return cfg, fmt.Errorf("%s: Invalid MINIO_COMPRESS_EXTENSIONS value (`%s`)", err, extensions)
			}
			cfg.Extensions = extensions
		}
		if compressMimeTypes != "" {
			contenttypes, err := parseCompressIncludes(strings.Split(compressMimeTypes, compressEnvDelimiter))
			if err != nil {
				return cfg, fmt.Errorf("%s: Invalid MINIO_COMPRESS_MIMETYPES value (`%s`)", err, contenttypes)
			}
			cfg.MimeTypes = contenttypes
		}
	}

	return cfg, nil
}
