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

package crypto

import "github.com/minio/minio/cmd/config"

// Help template for KMS vault
var (
	HelpVault = config.HelpKVS{
		config.HelpKV{
			Key:         KMSVaultEndpoint,
			Description: `API endpoint e.g. "http://vault-endpoint-ip:8200"`,
			Type:        "url",
		},
		config.HelpKV{
			Key:         KMSVaultKeyName,
			Description: `unique transit key name - e.g. "my-minio-key"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultAuthType,
			Description: `supported auth type(s) ["approle"], defaults to "approle"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultAppRoleID,
			Description: `unique role ID for approle`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultAppRoleSecret,
			Description: `unique secret ID for approle`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultNamespace,
			Description: `optional KMS namespace e.g. "customer1"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultKeyVersion,
			Description: `optional key version number`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         KMSVaultCAPath,
			Description: `optional path to PEM-encoded CA certs e.g. "/home/user/custom-certs"`,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpKes = config.HelpKVS{
		config.HelpKV{
			Key:         KMSKesEndpoint,
			Description: `API endpoint - e.g. "https://kes-endpoint:7373"`,
			Type:        "url",
		},
		config.HelpKV{
			Key:         KMSKesKeyName,
			Description: `unique key name - e.g. "my-minio-key"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSKesCertFile,
			Description: `path to client certificate for TLS auth - e.g. /etc/keys/public.crt`,
			Type:        "path",
		},
		config.HelpKV{
			Key:         KMSKesKeyFile,
			Description: `path to client private key for TLS auth - e.g. /etc/keys/private.key`,
			Type:        "path",
		},
		config.HelpKV{
			Key:         KMSKesCAPath,
			Description: `path to PEM-encoded cert(s) to verify kes server cert - e.g. /etc/keys/CAs`,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
