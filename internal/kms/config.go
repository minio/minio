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

package kms

// Top level config constants for KMS
const (
	EnvKMSSecretKey      = "MINIO_KMS_SECRET_KEY"
	EnvKMSSecretKeyFile  = "MINIO_KMS_SECRET_KEY_FILE"
	EnvKESEndpoint       = "MINIO_KMS_KES_ENDPOINT"     // One or multiple KES endpoints, separated by ','
	EnvKESKeyName        = "MINIO_KMS_KES_KEY_NAME"     // The default key name used for IAM data and when no key ID is specified on a bucket
	EnvKESAPIKey         = "MINIO_KMS_KES_API_KEY"      // Access credential for KES - API keys and private key / certificate are mutually exclusive
	EnvKESClientKey      = "MINIO_KMS_KES_KEY_FILE"     // Path to TLS private key for authenticating to KES with mTLS - usually prefer API keys
	EnvKESClientPassword = "MINIO_KMS_KES_KEY_PASSWORD" // Optional password to decrypt an encrypt TLS private key
	EnvKESClientCert     = "MINIO_KMS_KES_CERT_FILE"    // Path to TLS certificate for authenticating to KES with mTLS - usually prefer API keys
	EnvKESServerCA       = "MINIO_KMS_KES_CAPATH"       // Path to file/directory containing CA certificates to verify the KES server certificate
)
