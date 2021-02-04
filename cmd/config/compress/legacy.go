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
	"strings"

	"github.com/minio/minio/cmd/config"
)

// Legacy envs.
const (
	EnvCompress                = "MINIO_COMPRESS"
	EnvCompressMimeTypesLegacy = "MINIO_COMPRESS_MIMETYPES"
)

// SetCompressionConfig - One time migration code needed, for migrating from older config to new for Compression.
func SetCompressionConfig(s config.Config, cfg Config) {
	if !cfg.Enabled {
		// No need to save disabled settings in new config.
		return
	}
	s[config.CompressionSubSys][config.Default] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   Extensions,
			Value: strings.Join(cfg.Extensions, config.ValueSeparator),
		},
		config.KV{
			Key:   MimeTypes,
			Value: strings.Join(cfg.MimeTypes, config.ValueSeparator),
		},
	}
}
