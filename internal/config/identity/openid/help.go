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

package openid

import "github.com/minio/minio/internal/config"

// Help template for OpenID identity feature.
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         ConfigURL,
			Description: `openid discovery document e.g. "https://accounts.google.com/.well-known/openid-configuration"`,
			Type:        "url",
		},
		config.HelpKV{
			Key:         ClientID,
			Description: `unique public identifier for apps e.g. "292085223830.apps.googleusercontent.com"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ClientSecret,
			Description: `secret for the unique public identifier for apps e.g.`,
			Type:        "string",
			Optional:    true,
		},
		config.HelpKV{
			Key:         ClaimName,
			Description: `JWT canned policy claim name, defaults to "policy"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ClaimPrefix,
			Description: `JWT claim namespace prefix e.g. "customer1/"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         Scopes,
			Description: `Comma separated list of OpenID scopes for server, defaults to advertised scopes from discovery document e.g. "email,admin"`,
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
