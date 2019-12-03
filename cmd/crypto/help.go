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
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         KMSVaultEndpoint,
			Description: `HashiCorp Vault API endpoint e.g. "http://vault-endpoint-ip:8200"`,
			Type:        "url",
		},
		config.HelpKV{
			Key:         KMSVaultKeyName,
			Description: `transit key name used in vault policy, must be unique name e.g. "my-minio-key"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultAuthType,
			Description: `authentication type to Vault API endpoint e.g. "approle"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultAppRoleID,
			Description: `unique role ID created for AppRole`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultAppRoleSecret,
			Description: `unique secret ID created for AppRole`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultNamespace,
			Description: `only needed if AppRole engine is scoped to Vault Namespace e.g. "ns1"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KMSVaultKeyVersion,
			Description: `KMS Vault key version`,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         KMSVaultCAPath,
			Description: `path to PEM-encoded CA cert files to use mTLS authentication (optional) e.g. "/home/user/custom-certs"`,
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
