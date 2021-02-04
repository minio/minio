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

package logger

import "github.com/minio/minio/cmd/config"

// Legacy envs
const (
	legacyEnvAuditLoggerHTTPEndpoint = "MINIO_AUDIT_LOGGER_HTTP_ENDPOINT"
	legacyEnvLoggerHTTPEndpoint      = "MINIO_LOGGER_HTTP_ENDPOINT"
)

// SetLoggerHTTPAudit - helper for migrating older config to newer KV format.
func SetLoggerHTTPAudit(scfg config.Config, k string, args HTTP) {
	if !args.Enabled {
		// Do not enable audit targets, if not enabled
		return
	}
	scfg[config.AuditWebhookSubSys][k] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   Endpoint,
			Value: args.Endpoint,
		},
		config.KV{
			Key:   AuthToken,
			Value: args.AuthToken,
		},
	}
}

// SetLoggerHTTP helper for migrating older config to newer KV format.
func SetLoggerHTTP(scfg config.Config, k string, args HTTP) {
	if !args.Enabled {
		// Do not enable logger http targets, if not enabled
		return
	}

	scfg[config.LoggerWebhookSubSys][k] = config.KVS{
		config.KV{
			Key:   config.Enable,
			Value: config.EnableOn,
		},
		config.KV{
			Key:   Endpoint,
			Value: args.Endpoint,
		},
		config.KV{
			Key:   AuthToken,
			Value: args.AuthToken,
		},
	}
}
