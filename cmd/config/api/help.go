/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package api

import "github.com/minio/minio/cmd/config"

// Help template for storageclass feature.
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         apiRequestsMax,
			Description: `set the maximum number of concurrent requests, e.g. "1600"`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         apiRequestsDeadline,
			Description: `set the deadline for API requests waiting to be processed e.g. "1m"`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiReadyDeadline,
			Description: `set the deadline for health check API /minio/health/ready e.g. "1m"`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         apiCorsAllowOrigin,
			Description: `set comma separated list of origins allowed for CORS requests e.g. "https://example1.com,https://example2.com"`,
			Optional:    true,
			Type:        "csv",
		},
	}
)
