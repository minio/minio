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

package compress

import (
	"fmt"
	"strings"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// Config represents the compression settings.
type Config struct {
	Enabled        bool     `json:"enabled"`
	AllowEncrypted bool     `json:"allow_encryption"`
	Extensions     []string `json:"extensions"`
	MimeTypes      []string `json:"mime-types"`
}

// Compression environment variables
const (
	Extensions     = "extensions"
	AllowEncrypted = "allow_encryption"
	MimeTypes      = "mime_types"

	EnvCompressState           = "MINIO_COMPRESS_ENABLE"
	EnvCompressAllowEncryption = "MINIO_COMPRESS_ALLOW_ENCRYPTION"
	EnvCompressExtensions      = "MINIO_COMPRESS_EXTENSIONS"
	EnvCompressMimeTypes       = "MINIO_COMPRESS_MIME_TYPES"

	// Include-list for compression.
	DefaultExtensions = ".txt,.log,.csv,.json,.tar,.xml,.bin"
	DefaultMimeTypes  = "text/*,application/json,application/xml,binary/octet-stream"
)

// DefaultKVS - default KV config for compression settings
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   AllowEncrypted,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   Extensions,
			Value: DefaultExtensions,
		},
		config.KV{
			Key:   MimeTypes,
			Value: DefaultMimeTypes,
		},
	}
)

// Parses the given compression exclude list `extensions` or `content-types`.
func parseCompressIncludes(include string) ([]string, error) {
	includes := strings.Split(include, config.ValueSeparator)
	for _, e := range includes {
		if len(e) == 0 {
			return nil, config.ErrInvalidCompressionIncludesValue(nil).Msg("extension/mime-type cannot be empty")
		}
		if e == "/" {
			return nil, config.ErrInvalidCompressionIncludesValue(nil).Msg("extension/mime-type cannot be '/'")
		}
	}
	return includes, nil
}

// LookupConfig - lookup compression config.
func LookupConfig(kvs config.KVS) (Config, error) {
	var err error
	cfg := Config{}
	if err = config.CheckValidKeys(config.CompressionSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	compress := env.Get(EnvCompress, "")
	if compress == "" {
		compress = env.Get(EnvCompressState, kvs.Get(config.Enable))
	}
	cfg.Enabled, err = config.ParseBool(compress)
	if err != nil {
		// Parsing failures happen due to empty KVS, ignore it.
		if kvs.Empty() {
			return cfg, nil
		}
		return cfg, err
	}
	if !cfg.Enabled {
		return cfg, nil
	}

	allowEnc := env.Get(EnvCompressAllowEncryption, kvs.Get(AllowEncrypted))
	cfg.AllowEncrypted, err = config.ParseBool(allowEnc)
	if err != nil {
		return cfg, err
	}

	compressExtensions := env.Get(EnvCompressExtensions, kvs.Get(Extensions))
	compressMimeTypes := env.Get(EnvCompressMimeTypes, kvs.Get(MimeTypes))
	compressMimeTypesLegacy := env.Get(EnvCompressMimeTypesLegacy, kvs.Get(MimeTypes))
	if compressExtensions != "" || compressMimeTypes != "" || compressMimeTypesLegacy != "" {
		if compressExtensions != "" {
			extensions, err := parseCompressIncludes(compressExtensions)
			if err != nil {
				return cfg, fmt.Errorf("%s: Invalid MINIO_COMPRESS_EXTENSIONS value (`%s`)", err, extensions)
			}
			cfg.Extensions = extensions
		}
		if compressMimeTypes != "" {
			mimeTypes, err := parseCompressIncludes(compressMimeTypes)
			if err != nil {
				return cfg, fmt.Errorf("%s: Invalid MINIO_COMPRESS_MIME_TYPES value (`%s`)", err, mimeTypes)
			}
			cfg.MimeTypes = mimeTypes
		}
		if compressMimeTypesLegacy != "" {
			mimeTypes, err := parseCompressIncludes(compressMimeTypesLegacy)
			if err != nil {
				return cfg, fmt.Errorf("%s: Invalid MINIO_COMPRESS_MIME_TYPES value (`%s`)", err, mimeTypes)
			}
			cfg.MimeTypes = mimeTypes
		}
	}

	return cfg, nil
}
