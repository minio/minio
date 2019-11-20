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
			Description: `List of mounted drives or directories delimited by ","`,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         Exclude,
			Description: `List of wildcard based cache exclusion patterns delimited by ","`,
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         Expiry,
			Description: `Cache expiry duration in days. eg: "90"`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         Quota,
			Description: `Maximum permitted usage of the cache in percentage (0-100)`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the 'cache' settings",
			Optional:    true,
			Type:        "sentence",
		},
	}
)
