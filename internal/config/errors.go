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

package config

// UI errors
var (
	ErrInvalidXLValue = newErrFn(
		"Invalid drive path",
		"Please provide a fresh drive for single drive MinIO setup",
		"MinIO only supports fresh drive paths",
	)

	ErrInvalidBrowserValue = newErrFn(
		"Invalid console value",
		"Please check the passed value",
		"Environment can only accept `on` and `off` values. To disable Console access, set this value to `off`",
	)

	ErrInvalidFSOSyncValue = newErrFn(
		"Invalid O_SYNC value",
		"Please check the passed value",
		"Can only accept `on` and `off` values. To enable O_SYNC for fs backend, set this value to `on`",
	)

	ErrOverlappingDomainValue = newErrFn(
		"Overlapping domain values",
		"Please check the passed value",
		"MINIO_DOMAIN only accepts non-overlapping domain values",
	)

	ErrInvalidDomainValue = newErrFn(
		"Invalid domain value",
		"Please check the passed value",
		"Domain can only accept DNS compatible values",
	)

	ErrInvalidErasureSetSize = newErrFn(
		"Invalid erasure set size",
		"Please check the passed value",
		"Erasure set can only accept any of [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] values",
	)

	ErrInvalidWormValue = newErrFn(
		"Invalid WORM value",
		"Please check the passed value",
		"WORM can only accept `on` and `off` values. To enable WORM, set this value to `on`",
	)

	ErrInvalidConfigDecryptionKey = newErrFn(
		"Incorrect encryption key to decrypt internal data",
		"Please set the correct default KMS key value or the correct root credentials for older MinIO versions.",
		`Revert MINIO_KMS_KES_KEY_NAME or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD (for older MinIO versions) to be able to decrypt the internal data again.`,
	)

	ErrInvalidCredentials = newErrFn(
		"Invalid credentials",
		"Please provide correct credentials",
		`Access key length should be at least 3, and secret key length at least 8 characters`,
	)

	ErrInvalidRootUserCredentials = newErrFn(
		"Invalid credentials",
		"Please provide correct credentials",
		EnvRootUser+` length should be at least 3, and `+EnvRootPassword+` length at least 8 characters`,
	)

	ErrMissingEnvCredentialRootUser = newErrFn(
		"Missing credential environment variable, \""+EnvRootUser+"\"",
		"Environment variable \""+EnvRootUser+"\" is missing",
		`Root user name (access key) and root password (secret key) are expected to be specified via environment variables MINIO_ROOT_USER and MINIO_ROOT_PASSWORD respectively`,
	)

	ErrMissingEnvCredentialRootPassword = newErrFn(
		"Missing credential environment variable, \""+EnvRootPassword+"\"",
		"Environment variable \""+EnvRootPassword+"\" is missing",
		`Root user name (access key) and root password (secret key) are expected to be specified via environment variables MINIO_ROOT_USER and MINIO_ROOT_PASSWORD respectively`,
	)

	ErrMissingEnvCredentialAccessKey = newErrFn(
		"Missing credential environment variable, \""+EnvAccessKey+"\"",
		"Environment variables \""+EnvAccessKey+"\" and \""+EnvSecretKey+"\" are deprecated",
		`Root user name (access key) and root password (secret key) are expected to be specified via environment variables MINIO_ROOT_USER and MINIO_ROOT_PASSWORD respectively`,
	)

	ErrMissingEnvCredentialSecretKey = newErrFn(
		"Missing credential environment variable, \""+EnvSecretKey+"\"",
		"Environment variables \""+EnvSecretKey+"\" and \""+EnvAccessKey+"\" are deprecated",
		`Root user name (access key) and root password (secret key) are expected to be specified via environment variables MINIO_ROOT_USER and MINIO_ROOT_PASSWORD respectively`,
	)

	ErrInvalidErasureEndpoints = newErrFn(
		"Invalid endpoint(s) in erasure mode",
		"Please provide correct combination of local/remote paths",
		"For more information, please refer to https://docs.min.io/community/minio-object-store/operations/concepts/erasure-coding.html",
	)

	ErrInvalidNumberOfErasureEndpoints = newErrFn(
		"Invalid total number of endpoints for erasure mode",
		"Please provide number of endpoints greater or equal to 2",
		"For more information, please refer to https://docs.min.io/community/minio-object-store/operations/concepts/erasure-coding.html",
	)

	ErrStorageClassValue = newErrFn(
		"Invalid storage class value",
		"Please check the value",
		`MINIO_STORAGE_CLASS_STANDARD: Format "EC:<Default_Parity_Standard_Class>" (e.g. "EC:3"). This sets the number of parity drives for MinIO server in Standard mode. Objects are stored in Standard mode, if storage class is not defined in Put request
MINIO_STORAGE_CLASS_RRS: Format "EC:<Default_Parity_Reduced_Redundancy_Class>" (e.g. "EC:3"). This sets the number of parity drives for MinIO server in Reduced Redundancy mode. Objects are stored in Reduced Redundancy mode, if Put request specifies RRS storage class
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

	ErrInvalidEndpoint = newErrFn(
		"Invalid endpoint for single drive mode",
		"Please check the endpoint",
		`Single-Node modes requires absolute path without hostnames:
Examples:
   $ minio server /data/minio/ #Single Node Single Drive
   $ minio server /data-{1...4}/minio # Single Node Multi Drive`,
	)

	ErrUnsupportedBackend = newErrFn(
		"Unable to write to the backend",
		"Please ensure your drive supports O_DIRECT",
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

	ErrTLSReadError = newErrFn(
		"Cannot read the TLS certificate",
		"Please check if the certificate has the proper owner and read permissions",
		"",
	)

	ErrTLSUnexpectedData = newErrFn(
		"Invalid TLS certificate",
		"Please check your certificate",
		"",
	)

	ErrTLSNoPassword = newErrFn(
		"Missing TLS password",
		"Please set the password to environment variable `MINIO_CERT_PASSWD` so that the private key can be decrypted",
		"",
	)

	ErrNoCertsAndHTTPSEndpoints = newErrFn(
		"HTTPS specified in endpoints, but no TLS certificate is found on the local machine",
		"Please add TLS certificate or use HTTP endpoints only",
		"Refer to https://docs.min.io/community/minio-object-store/operations/network-encryption.html for information about how to load a TLS certificate in your server",
	)

	ErrCertsAndHTTPEndpoints = newErrFn(
		"HTTP specified in endpoints, but the server in the local machine is configured with a TLS certificate",
		"Please remove the certificate in the configuration directory or switch to HTTPS",
		"",
	)

	ErrTLSWrongPassword = newErrFn(
		"Unable to decrypt the private key using the provided password",
		"Please set the correct password in environment variable `MINIO_CERT_PASSWD`",
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

	ErrInvalidReplicationWorkersValue = newErrFn(
		"Invalid value for replication workers",
		"",
		"MINIO_API_REPLICATION_WORKERS: should be > 0",
	)

	ErrInvalidTransitionWorkersValue = newErrFn(
		"Invalid value for transition workers",
		"",
		"MINIO_API_TRANSITION_WORKERS: should be >= GOMAXPROCS/2",
	)
	ErrInvalidBatchKeyRotationWorkersWait = newErrFn(
		"Invalid value for batch key rotation workers wait",
		"Please input a non-negative duration",
		"keyrotation_workers_wait should be > 0ms",
	)
	ErrInvalidBatchReplicationWorkersWait = newErrFn(
		"Invalid value for batch replication workers wait",
		"Please input a non-negative duration",
		"replication_workers_wait should be > 0ms",
	)
	ErrInvalidBatchExpirationWorkersWait = newErrFn(
		"Invalid value for batch expiration workers wait",
		"Please input a non-negative duration",
		"expiration_workers_wait should be > 0ms",
	)
)
