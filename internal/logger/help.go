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
)

// Help template for logger http and audit
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Endpoint,
			Description: `HTTP(s) endpoint e.g. "http://localhost:8080/minio/logs/server"`,
			Type:        "url",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         AuthToken,
			Description: `opaque string or JWT authorization token`,
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpAudit = config.HelpKVS{
		config.HelpKV{
			Key:         Endpoint,
			Description: `HTTP(s) endpoint e.g. "http://localhost:8080/minio/logs/audit"`,
			Type:        "url",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         AuthToken,
			Description: `opaque string or JWT authorization token`,
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         ClientCert,
			Description: "mTLS certificate for Audit Webhook authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         ClientKey,
			Description: "mTLS certificate key for Audit Webhook authentication",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
	}
)
