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

	// Legacy files
	EnvAccessKeyFile = "MINIO_ACCESS_KEY_FILE"
	EnvSecretKeyFile = "MINIO_SECRET_KEY_FILE"

	// Current files
	EnvRootUserFile     = "MINIO_ROOT_USER_FILE"
	EnvRootPasswordFile = "MINIO_ROOT_PASSWORD_FILE"

	// Set all config environment variables from 'config.env'
	// if necessary. Overrides all previous settings and also
	// overrides all environment values passed from
	// 'podman run -e ENV=value'
	EnvConfigEnvFile = "MINIO_CONFIG_ENV_FILE"

	EnvBrowser    = "MINIO_BROWSER"
	EnvDomain     = "MINIO_DOMAIN"
	EnvPublicIPs  = "MINIO_PUBLIC_IPS"
	EnvFSOSync    = "MINIO_FS_OSYNC"
	EnvArgs       = "MINIO_ARGS"
	EnvVolumes    = "MINIO_VOLUMES"
	EnvDNSWebhook = "MINIO_DNS_WEBHOOK_ENDPOINT"

	EnvSiteName   = "MINIO_SITE_NAME"
	EnvSiteRegion = "MINIO_SITE_REGION"

	EnvMinIOSubnetLicense = "MINIO_SUBNET_LICENSE" // Deprecated Dec 2021
	EnvMinIOSubnetAPIKey  = "MINIO_SUBNET_API_KEY"
	EnvMinIOSubnetProxy   = "MINIO_SUBNET_PROXY"

	EnvMinIOCallhomeEnable    = "MINIO_CALLHOME_ENABLE"
	EnvMinIOCallhomeFrequency = "MINIO_CALLHOME_FREQUENCY"

	EnvMinIOServerURL             = "MINIO_SERVER_URL"
	EnvBrowserRedirect            = "MINIO_BROWSER_REDIRECT" // On by default
	EnvBrowserRedirectURL         = "MINIO_BROWSER_REDIRECT_URL"
	EnvRootDriveThresholdSize     = "MINIO_ROOTDRIVE_THRESHOLD_SIZE"
	EnvRootDiskThresholdSize      = "MINIO_ROOTDISK_THRESHOLD_SIZE" // Deprecated Sep 2023
	EnvBrowserLoginAnimation      = "MINIO_BROWSER_LOGIN_ANIMATION"
	EnvBrowserSessionDuration     = "MINIO_BROWSER_SESSION_DURATION" // Deprecated after November 2023
	EnvMinioStsDuration           = "MINIO_STS_DURATION"
	EnvMinIOLogQueryURL           = "MINIO_LOG_QUERY_URL"
	EnvMinIOLogQueryAuthToken     = "MINIO_LOG_QUERY_AUTH_TOKEN"
	EnvMinIOPrometheusURL         = "MINIO_PROMETHEUS_URL"
	EnvMinIOPrometheusJobID       = "MINIO_PROMETHEUS_JOB_ID"
	EnvMinIOPrometheusExtraLabels = "MINIO_PROMETHEUS_EXTRA_LABELS"
	EnvMinIOPrometheusAuthToken   = "MINIO_PROMETHEUS_AUTH_TOKEN"
	EnvConsoleDebugLogLevel       = "MINIO_CONSOLE_DEBUG_LOGLEVEL"

	EnvUpdate = "MINIO_UPDATE"

	EnvEndpoints  = "MINIO_ENDPOINTS"   // legacy
	EnvWorm       = "MINIO_WORM"        // legacy
	EnvRegion     = "MINIO_REGION"      // legacy
	EnvRegionName = "MINIO_REGION_NAME" // legacy

)

// Expiration Token durations
// These values are used to validate the expiration time range from
// either the exp claim or MINI_STS_DURATION value
const (
	MinExpiration = 900
	MaxExpiration = 31536000
)
