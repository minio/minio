/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
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

package config

// UI errors
var (
	ErrInvalidBrowserValue = newErrFn(
		"Invalid browser value",
		"Please check the passed value",
		"Browser can only accept `on` and `off` values. To disable web browser access, set this value to `off`",
	)

	ErrInvalidFSOSyncValue = newErrFn(
		"Invalid O_SYNC value",
		"Please check the passed value",
		"Can only accept `on` and `off` values. To enable O_SYNC for fs backend, set this value to `on`",
	)

	ErrInvalidDomainValue = newErrFn(
		"Invalid domain value",
		"Please check the passed value",
		"Domain can only accept DNS compatible values",
	)

	ErrInvalidErasureSetSize = newErrFn(
		"Invalid erasure set size",
		"Please check the passed value",
		"Erasure set can only accept any of [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] values",
	)

	ErrInvalidWormValue = newErrFn(
		"Invalid WORM value",
		"Please check the passed value",
		"WORM can only accept `on` and `off` values. To enable WORM, set this value to `on`",
	)

	ErrInvalidCacheDrivesValue = newErrFn(
		"Invalid cache drive value",
		"Please check the value in this ENV variable",
		"MINIO_CACHE_DRIVES: Mounted drives or directories are delimited by `,`",
	)

	ErrInvalidCacheExcludesValue = newErrFn(
		"Invalid cache excludes value",
		"Please check the passed value",
		"MINIO_CACHE_EXCLUDE: Cache exclusion patterns are delimited by `,`",
	)

	ErrInvalidCacheExpiryValue = newErrFn(
		"Invalid cache expiry value",
		"Please check the passed value",
		"MINIO_CACHE_EXPIRY: Valid cache expiry duration must be in days",
	)

	ErrInvalidCacheQuota = newErrFn(
		"Invalid cache quota value",
		"Please check the passed value",
		"MINIO_CACHE_QUOTA: Valid cache quota value must be between 0-100",
	)

	ErrInvalidCacheAfter = newErrFn(
		"Invalid cache after value",
		"Please check the passed value",
		"MINIO_CACHE_AFTER: Valid cache after value must be 0 or greater",
	)

	ErrInvalidCacheWatermarkLow = newErrFn(
		"Invalid cache low watermark value",
		"Please check the passed value",
		"MINIO_CACHE_WATERMARK_LOW: Valid cache low watermark value must be between 0-100",
	)

	ErrInvalidCacheWatermarkHigh = newErrFn(
		"Invalid cache high watermark value",
		"Please check the passed value",
		"MINIO_CACHE_WATERMARK_HIGH: Valid cache high watermark value must be between 0-100",
	)

	ErrInvalidCacheEncryptionKey = newErrFn(
		"Invalid cache encryption master key value",
		"Please check the passed value",
		"MINIO_CACHE_ENCRYPTION_MASTER_KEY: For more information, please refer to https://docs.min.io/docs/minio-disk-cache-guide",
	)

	ErrInvalidRotatingCredentialsBackendEncrypted = newErrFn(
		"Invalid rotating credentials",
		"Please set correct rotating credentials in the environment for decryption",
		`Detected encrypted config backend, correct old access and secret keys should be specified via environment variables MINIO_ACCESS_KEY_OLD and MINIO_SECRET_KEY_OLD to be able to re-encrypt the MinIO config, user IAM and policies with new credentials`,
	)

	ErrInvalidCredentialsBackendEncrypted = newErrFn(
		"Invalid credentials",
		"Please set correct credentials in the environment for decryption",
		`Detected encrypted config backend, correct access and secret keys should be specified via environment variables MINIO_ACCESS_KEY and MINIO_SECRET_KEY to be able to decrypt the MinIO config, user IAM and policies`,
	)

	ErrMissingCredentialsBackendEncrypted = newErrFn(
		"Credentials missing",
		"Please set your credentials in the environment",
		`Detected encrypted config backend, access and secret keys should be specified via environment variables MINIO_ACCESS_KEY and MINIO_SECRET_KEY to be able to decrypt the MinIO config, user IAM and policies`,
	)

	ErrInvalidCredentials = newErrFn(
		"Invalid credentials",
		"Please provide correct credentials",
		`Access key length should be at least 3, and secret key length at least 8 characters`,
	)

	ErrEnvCredentialsMissingGateway = newErrFn(
		"Credentials missing",
		"Please set your credentials in the environment",
		`In Gateway mode, access and secret keys should be specified via environment variables MINIO_ACCESS_KEY and MINIO_SECRET_KEY respectively`,
	)

	ErrEnvCredentialsMissingDistributed = newErrFn(
		"Credentials missing",
		"Please set your credentials in the environment",
		`In distributed server mode, access and secret keys should be specified via environment variables MINIO_ACCESS_KEY and MINIO_SECRET_KEY respectively`,
	)

	ErrInvalidErasureEndpoints = newErrFn(
		"Invalid endpoint(s) in erasure mode",
		"Please provide correct combination of local/remote paths",
		"For more information, please refer to https://docs.min.io/docs/minio-erasure-code-quickstart-guide",
	)

	ErrInvalidNumberOfErasureEndpoints = newErrFn(
		"Invalid total number of endpoints for erasure mode",
		"Please provide an even number of endpoints greater or equal to 4",
		"For more information, please refer to https://docs.min.io/docs/minio-erasure-code-quickstart-guide",
	)

	ErrStorageClassValue = newErrFn(
		"Invalid storage class value",
		"Please check the value",
		`MINIO_STORAGE_CLASS_STANDARD: Format "EC:<Default_Parity_Standard_Class>" (e.g. "EC:3"). This sets the number of parity disks for MinIO server in Standard mode. Objects are stored in Standard mode, if storage class is not defined in Put request
MINIO_STORAGE_CLASS_RRS: Format "EC:<Default_Parity_Reduced_Redundancy_Class>" (e.g. "EC:3"). This sets the number of parity disks for MinIO server in Reduced Redundancy mode. Objects are stored in Reduced Redundancy mode, if Put request specifies RRS storage class
Refer to the link https://github.com/minio/minio/tree/master/docs/erasure/storage-class for more information`,
	)

	ErrUnexpectedBackendVersion = newErrFn(
		"Backend version seems to be too recent",
		"Please update to the latest MinIO version",
		"",
	)

	ErrInvalidAddressFlag = newErrFn(
		"--address input is invalid",
		"Please check --address parameter",
		`--address binds to a specific ADDRESS:PORT, ADDRESS can be an IPv4/IPv6 address or hostname (default port is ':9000')
	Examples: --address ':443'
		  --address '172.16.34.31:9000'
		  --address '[fe80::da00:a6c8:e3ae:ddd7]:9000'`,
	)

	ErrInvalidFSEndpoint = newErrFn(
		"Invalid endpoint for standalone FS mode",
		"Please check the FS endpoint",
		`FS mode requires only one writable disk path
Example 1:
   $ minio server /data/minio/`,
	)

	ErrUnsupportedBackend = newErrFn(
		"Unable to write to the backend",
		"Please ensure your disk supports O_DIRECT",
		"",
	)

	ErrCorruptedBackend = newErrFn(
		"Unable to use the specified backend, pre-existing content detected",
		"Please ensure your disk mount does not have any pre-existing content",
		"",
	)

	ErrUnableToWriteInBackend = newErrFn(
		"Unable to write to the backend",
		"Please ensure MinIO binary has write permissions for the backend",
		`Verify if MinIO binary is running as the same user who has write permissions for the backend`,
	)

	ErrPortAlreadyInUse = newErrFn(
		"Port is already in use",
		"Please ensure no other program uses the same address/port",
		"",
	)

	ErrPortAccess = newErrFn(
		"Unable to use specified port",
		"Please ensure MinIO binary has 'cap_net_bind_service=+ep' permissions",
		`Use 'sudo setcap cap_net_bind_service=+ep /path/to/minio' to provide sufficient permissions`,
	)

	ErrNoPermissionsToAccessDirFiles = newErrFn(
		"Missing permissions to access the specified path",
		"Please ensure the specified path can be accessed",
		"",
	)

	ErrSSLUnexpectedError = newErrFn(
		"Invalid TLS certificate",
		"Please check the content of your certificate data",
		`Only PEM (x.509) format is accepted as valid public & private certificates`,
	)

	ErrSSLUnexpectedData = newErrFn(
		"Invalid TLS certificate",
		"Please check your certificate",
		"",
	)

	ErrSSLNoPassword = newErrFn(
		"Missing TLS password",
		"Please set the password to environment variable `MINIO_CERT_PASSWD` so that the private key can be decrypted",
		"",
	)

	ErrNoCertsAndHTTPSEndpoints = newErrFn(
		"HTTPS specified in endpoints, but no TLS certificate is found on the local machine",
		"Please add TLS certificate or use HTTP endpoints only",
		"Refer to https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls for information about how to load a TLS certificate in your server",
	)

	ErrCertsAndHTTPEndpoints = newErrFn(
		"HTTP specified in endpoints, but the server in the local machine is configured with a TLS certificate",
		"Please remove the certificate in the configuration directory or switch to HTTPS",
		"",
	)

	ErrSSLWrongPassword = newErrFn(
		"Unable to decrypt the private key using the provided password",
		"Please set the correct password in environment variable `MINIO_CERT_PASSWD`",
		"",
	)

	ErrUnexpectedDataContent = newErrFn(
		"Unexpected data content",
		"Please contact MinIO at https://slack.min.io",
		"",
	)

	ErrUnexpectedError = newErrFn(
		"Unexpected error",
		"Please contact MinIO at https://slack.min.io",
		"",
	)

	ErrInvalidCompressionIncludesValue = newErrFn(
		"Invalid compression include value",
		"Please check the passed value",
		"Compress extensions/mime-types are delimited by `,`. For eg, MINIO_COMPRESS_MIME_TYPES=\"A,B,C\"",
	)

	ErrInvalidGWSSEValue = newErrFn(
		"Invalid gateway SSE value",
		"Please check the passed value",
		"MINIO_GATEWAY_SSE: Gateway SSE accepts only C and S3 as valid values. Delimit by `;` to set more than one value",
	)

	ErrInvalidGWSSEEnvValue = newErrFn(
		"Invalid gateway SSE configuration",
		"",
		"Refer to https://docs.min.io/docs/minio-kms-quickstart-guide.html for setting up SSE",
	)
)
