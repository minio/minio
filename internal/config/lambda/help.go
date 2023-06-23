// Copyright (c) 2015-2023 MinIO, Inc.
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

package lambda

import (
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/event/target"
)

// Help template inputs for all lambda targets
var (
	HelpWebhook = config.HelpKVS{
		config.HelpKV{
			Key:         target.WebhookEndpoint,
			Description: "webhook server endpoint e.g. http://localhost:8080/minio/lambda",
			Type:        "url",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.WebhookAuthToken,
			Description: "opaque string or JWT authorization token",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         target.WebhookClientCert,
			Description: "client cert for Webhook mTLS auth",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         target.WebhookClientKey,
			Description: "client cert key for Webhook mTLS auth",
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
	}
)
