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

package config

// Config value separator
const (
	ValueSeparator = ","
)

// Top level common ENVs
const (
	EnvAccessKey    = "MINIO_ACCESS_KEY"
	EnvSecretKey    = "MINIO_SECRET_KEY"
	EnvRootUser     = "MINIO_ROOT_USER"
	EnvRootPassword = "MINIO_ROOT_PASSWORD"

	EnvBrowser         = "MINIO_BROWSER"
	EnvBrowserRedirect = "MINIO_BROWSER_REDIRECT"
	EnvDomain          = "MINIO_DOMAIN"
	EnvRegionName      = "MINIO_REGION_NAME"
	EnvPublicIPs       = "MINIO_PUBLIC_IPS"
	EnvFSOSync         = "MINIO_FS_OSYNC"
	EnvArgs            = "MINIO_ARGS"
	EnvDNSWebhook      = "MINIO_DNS_WEBHOOK_ENDPOINT"

	EnvRootDiskThresholdSize = "MINIO_ROOTDISK_THRESHOLD_SIZE"

	EnvUpdate = "MINIO_UPDATE"

	EnvKMSSecretKey  = "MINIO_KMS_SECRET_KEY"
	EnvKESEndpoint   = "MINIO_KMS_KES_ENDPOINT"
	EnvKESKeyName    = "MINIO_KMS_KES_KEY_NAME"
	EnvKESClientKey  = "MINIO_KMS_KES_KEY_FILE"
	EnvKESClientCert = "MINIO_KMS_KES_CERT_FILE"
	EnvKESServerCA   = "MINIO_KMS_KES_CAPATH"

	EnvEndpoints = "MINIO_ENDPOINTS" // legacy
	EnvWorm      = "MINIO_WORM"      // legacy
	EnvRegion    = "MINIO_REGION"    // legacy
)
