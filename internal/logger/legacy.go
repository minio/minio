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

package logger

import (
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/logger/target/http"
)

// Legacy envs
const (
	legacyEnvAuditLoggerHTTPEndpoint = "MINIO_AUDIT_LOGGER_HTTP_ENDPOINT"
	legacyEnvLoggerHTTPEndpoint      = "MINIO_LOGGER_HTTP_ENDPOINT"
)

// SetLoggerHTTPAudit - helper for migrating older config to newer KV format.
func SetLoggerHTTPAudit(scfg config.Config, k string, args http.Config) {
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
			Value: args.Endpoint.String(),
		},
		config.KV{
			Key:   AuthToken,
			Value: args.AuthToken,
		},
	}
}

// SetLoggerHTTP helper for migrating older config to newer KV format.
func SetLoggerHTTP(scfg config.Config, k string, args http.Config) {
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
			Value: args.Endpoint.String(),
		},
		config.KV{
			Key:   AuthToken,
			Value: args.AuthToken,
		},
	}
}
