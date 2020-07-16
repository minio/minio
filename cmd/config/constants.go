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

package config

// Config value separator
const (
	ValueSeparator = ","
)

// Top level common ENVs
const (
	EnvAccessKey    = "MINIO_ACCESS_KEY"
	EnvSecretKey    = "MINIO_SECRET_KEY"
	EnvAccessKeyOld = "MINIO_ACCESS_KEY_OLD"
	EnvSecretKeyOld = "MINIO_SECRET_KEY_OLD"
	EnvBrowser      = "MINIO_BROWSER"
	EnvDomain       = "MINIO_DOMAIN"
	EnvRegionName   = "MINIO_REGION_NAME"
	EnvPublicIPs    = "MINIO_PUBLIC_IPS"
	EnvEndpoints    = "MINIO_ENDPOINTS"
	EnvFSOSync      = "MINIO_FS_OSYNC"

	EnvUpdate = "MINIO_UPDATE"

	EnvWorm   = "MINIO_WORM"   // legacy
	EnvRegion = "MINIO_REGION" // legacy
)
