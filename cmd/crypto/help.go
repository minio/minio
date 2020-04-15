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
