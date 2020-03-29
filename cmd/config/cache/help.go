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

package cache

import "github.com/minio/minio/cmd/config"

// Help template for caching feature.
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Drives,
			Description: `comma separated mountpoints e.g. "/optane1,/optane2"`,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         Expiry,
			Description: `cache expiry duration in days e.g. "90"`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         Quota,
			Description: `limit cache drive usage in percentage e.g. "90"`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         Exclude,
			Description: `comma separated wildcard exclusion patterns e.g. "bucket/*.tmp,*.exe"`,
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         After,
			Description: `minimum accesses before caching an object`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         WatermarkLow,
			Description: `% of cache use at which to stop cache eviction`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         WatermarkHigh,
			Description: `% of cache use at which to start cache eviction`,
			Optional:    true,
			Type:        "number",
		},
	}
)
