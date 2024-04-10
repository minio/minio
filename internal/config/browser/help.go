// Copyright (c) 2015-2023 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
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

package browser

import "github.com/minio/minio/internal/config"

// Help template for browser feature.
var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	Help = config.HelpKVS{
		config.HelpKV{
			Key:         browserCSPPolicy,
			Description: `set Content-Security-Policy response header value` + defaultHelpPostfix(browserCSPPolicy),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         browserHSTSSeconds,
			Description: `set Strict-Transport-Security 'max-age' amount of seconds value` + defaultHelpPostfix(browserHSTSSeconds),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         browserHSTSIncludeSubdomains,
			Description: `turn 'on' to set Strict-Transport-Security 'includeSubDomains' directive` + defaultHelpPostfix(browserHSTSIncludeSubdomains),
			Optional:    true,
			Type:        "boolean",
		},
		config.HelpKV{
			Key:         browserHSTSPreload,
			Description: `turn 'on' to set Strict-Transport-Security 'preload' directive` + defaultHelpPostfix(browserHSTSPreload),
			Optional:    true,
			Type:        "boolean",
		},
		config.HelpKV{
			Key:         browserReferrerPolicy,
			Description: `set Referrer-Policy response header value` + defaultHelpPostfix(browserReferrerPolicy),
			Optional:    true,
			Type:        "string",
		},
	}
)
