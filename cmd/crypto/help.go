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
	Help = config.HelpKV{
		KMSVaultEndpoint:      `Points to Vault API endpoint eg: "http://vault-endpoint-ip:8200"`,
		KMSVaultKeyName:       `Transit key name used in vault policy, must be unique name eg: "my-minio-key"`,
		KMSVaultAuthType:      `Authentication type to Vault API endpoint eg: "approle"`,
		KMSVaultAppRoleID:     `Unique role ID created for AppRole`,
		KMSVaultAppRoleSecret: `Unique secret ID created for AppRole`,
		KMSVaultNamespace:     `Only needed if AppRole engine is scoped to Vault Namespace eg: "ns1"`,
		KMSVaultKeyVersion:    `Key version (optional)`,
		KMSVaultCAPath:        `Path to PEM-encoded CA cert files to use mTLS authentication (optional) eg: "/home/user/custom-certs"`,
		config.State:          "Indicates if KMS Vault is enabled or not",
		config.Comment:        "A comment to describe the KMS Vault setting",
	}
)
