/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package cmd

var (
	uiErrInvalidConfig = newUIErrFn(
		"Invalid value found in the configuration file",
		"Please ensure a valid value in the configuration file, for more details refer https://docs.minio.io/docs/minio-server-configuration-guide",
		"",
	)

	uiErrInvalidBrowserValue = newUIErrFn(
		"Invalid browser value",
		"Please check the passed value",
		"Browser can only accept `on` and `off` values. To disable web browser access, set this value to `off`",
	)

	uiErrInvalidCacheDrivesValue = newUIErrFn(
		"Invalid cache drive value",
		"Please check the value in this ENV variable",
		"MINIO_CACHE_DRIVES: Mounted drives or directories are delimited by `;`",
	)

	uiErrInvalidCacheExcludesValue = newUIErrFn(
		"Invalid cache excludes value",
		"Please check the passed value",
		"MINIO_CACHE_EXCLUDE: Cache exclusion patterns are delimited by `;`",
	)

	uiErrInvalidCacheExpiryValue = newUIErrFn(
		"Invalid cache expiry value",
		"Please check the passed value",
		"MINIO_CACHE_EXPIRY: Valid cache expiry duration is in days.",
	)

	uiErrInvalidCredentials = newUIErrFn(
		"Invalid credentials",
		"Please provide correct credentials",
		`Access key length should be between minimum 3 characters in length.
Secret key should be in between 8 and 40 characters.`,
	)

	uiErrInvalidErasureEndpoints = newUIErrFn(
		"Invalid endpoint(s) in erasure mode",
		"Please provide correct combination of local/remote paths",
		"For more information, please refer to https://docs.minio.io/docs/minio-erasure-code-quickstart-guide",
	)

	uiErrInvalidNumberOfErasureEndpoints = newUIErrFn(
		"Invalid total number of endpoints for erasure mode",
		"Please provide an even number of endpoints greater or equal to 4",
		"For more information, please refer to https://docs.minio.io/docs/minio-erasure-code-quickstart-guide",
	)

	uiErrStorageClassValue = newUIErrFn(
		"Invalid storage class value",
		"Please check the value",
		`MINIO_STORAGE_CLASS_STANDARD: Format "EC:<Default_Parity_Standard_Class>" (e.g. "EC:3"). This sets the number of parity disks for Minio server in Standard mode. Objects are stored in Standard mode, if storage class is not defined in Put request.
MINIO_STORAGE_CLASS_RRS: Format "EC:<Default_Parity_Reduced_Redundancy_Class>" (e.g. "EC:3"). This sets the number of parity disks for Minio server in Reduced Redundancy mode. Objects are stored in Reduced Redundancy mode, if Put request specifies RRS storage class.
Refer to the link https://github.com/minio/minio/tree/master/docs/erasure/storage-class for more information.`,
	)

	uiErrUnexpectedBackendVersion = newUIErrFn(
		"Backend version seems to be too recent",
		"Please update to the latest Minio version",
		"",
	)

	uiErrInvalidAddressFlag = newUIErrFn(
		"--address input is invalid",
		"Please check --address parameter",
		`--address binds to a specific ADDRESS:PORT, ADDRESS can be an  IP or hostname (default port is ':9000')
    Examples: --address ':443'
              --address '172.16.34.31:9000'`,
	)

	uiErrInvalidFSEndpoint = newUIErrFn(
		"Invalid endpoint for standalone FS mode",
		"Please check the FS endpoint",
		`FS mode requires only one writable disk path.
Example 1:
   $ minio server /data/minio/`,
	)

	uiErrUnableToWriteInBackend = newUIErrFn(
		"Unable to write to the backend",
		"Please ensure Minio binary has write permissions for the backend",
		"",
	)

	uiErrUnableToReadFromBackend = newUIErrFn(
		"Unable to read from the backend",
		"Please ensure Minio binary has read permissions for the backend",
		"",
	)

	uiErrPortAlreadyInUse = newUIErrFn(
		"Port is already in use",
		"Please ensure no other program uses the same address/port",
		"",
	)

	uiErrNoPermissionsToAccessDirFiles = newUIErrFn(
		"Missing permissions to access the specified path",
		"Please ensure the specified path can be accessed",
		"",
	)

	uiErrSSLUnexpectedError = newUIErrFn(
		"Invalid TLS certificate",
		"Please check the content of your certificate data",
		`Only PEM (x.509) format is accepted as valid public & private certificates.`,
	)

	uiErrSSLUnexpectedData = newUIErrFn(
		"Invalid TLS certificate",
		"Please check your certificate",
		"",
	)

	uiErrSSLNoPassword = newUIErrFn(
		"Missing TLS password",
		"Please set the password to environment variable `"+TLSPrivateKeyPassword+"` so that the private key can be decrypted",
		"",
	)

	uiErrNoCertsAndHTTPSEndpoints = newUIErrFn(
		"HTTPS specified in endpoints, but no TLS certificate is found on the local machine",
		"Please add a certificate or switch to HTTP.",
		"Refer to https://docs.minio.io/docs/how-to-secure-access-to-minio-server-with-tls for information about how to load a TLS certificate in your server.",
	)

	uiErrCertsAndHTTPEndpoints = newUIErrFn(
		"HTTP specified in endpoints, but the server in the local machine is configured with a TLS certificate",
		"Please remove the certificate in the configuration directory or switch to HTTPS",
		"",
	)

	uiErrSSLWrongPassword = newUIErrFn(
		"Unable to decrypt the private key using the provided password",
		"Please set the correct password in environment variable "+TLSPrivateKeyPassword,
		"",
	)

	uiErrUnexpectedDataContent = newUIErrFn(
		"Unexpected data content",
		"Please contact Minio at https://slack.minio.io",
		"",
	)

	uiErrUnexpectedError = newUIErrFn(
		"Unexpected error",
		"Please contact Minio at https://slack.minio.io",
		"",
	)
)
