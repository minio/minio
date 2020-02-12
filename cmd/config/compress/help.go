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

import "github.com/minio/minio/cmd/config"

// Help template for compress feature.
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Extensions,
			Description: `comma separated file extensions e.g. ".txt,.log,.csv"`,
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         MimeTypes,
			Description: `comma separated wildcard mime-types e.g. "text/*,application/json,application/xml"`,
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
