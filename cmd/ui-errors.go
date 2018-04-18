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
		"Please ensure putting a valid value in the configuration file",
		"",
	)

	uiErrInvalidBrowserValue = newUIErrFn(
		"Invalid browser value",
		"Please check the passed value",
		"Browser can only accept `on` and `off` values. To disable web browser access, set this value to `off`",
	)

	uiErrInvalidCacheDrivesValue = newUIErrFn(
		"Invalid cache drive value",
		"Please check the passed value",
		"MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by `;`.",
	)

	uiErrInvalidCacheExcludesValue = newUIErrFn(
		"Invalid cache excludes value",
		"Please check the passed value",
		"MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by `;`.",
	)

	uiErrInvalidCacheExpiryValue = newUIErrFn(
		"Invalid cache expiry value",
		"Please check the passed value",
		"MINIO_CACHE_EXPIRY: Cache expiry duration in days.",
	)

	uiErrInvalidCredentials = newUIErrFn(
		"Passed credentials are not suitable for use",
		"Please provide correct credentials",
		`- Access key length should be between 3 and 20 characters.
- Secret key should not be between 8 and 40 characters.`,
	)

	uiErrInvalidErasureEndpoints = newUIErrFn(
		"Endpoints unsuitable to activate Erasure mode",
		"Please provide correct combinations of local/remote paths",
		"",
	)

	uiErrInvalidNumberOfErasureEndpoints = newUIErrFn(
		"Number of endpoints are not suitable for use in Erasure mode",
		"Please provide an even number of endpoints greater or equal to 4",
		"",
	)

	uiErrStorageClassValue = newUIErrFn(
		"Invalid storage class value",
		"Please check the passed value",
		`MINIO_STORAGE_CLASS_STANDARD: Format "EC:<Default_Parity_Standard_Class>" (e.g. "EC:3"). This sets the number of parity disks for Minio server in Standard mode. Object are stored in Standard mode, if storage class is not defined in Put request.
MINIO_STORAGE_CLASS_RRS: Format "EC:<Default_Parity_Reduced_Redundancy_Class>" (e.g. "EC:3"). This sets the number of parity disks for Minio server in reduced redundancy mode. Objects are stored in Reduced Redundancy mode, if Put request specifies RRS storage class.`,
	)

	uiErrUnexpectedBackendVersion = newUIErrFn(
		"Backend version seems to be too recent",
		"Please update to the latest Minio version",
		"",
	)

	uiErrInvalidAddressFlag = newUIErrFn(
		"--address parameter is not suitable for use",
		"Please check --address parameter",
		`--address binds a specific ADDRESS:PORT, ADDRESS can be an  IP or hostname (default:':9000')
    Examples: --address ':443'
              --address '172.16.34.31:9000'`,
	)

	uiErrInvalidFSEndpoint = newUIErrFn(
		"The given endpoint is not suitable to activate standalone mode",
		"Please check the given FS endpoint",
		`Standalone mode requires only one local disk path to be activated.
Example 1:
   $ minio server /var/data/minio/`,
	)

	uiErrUnableToWriteInBackend = newUIErrFn(
		"Unable to write on the specified endpoint",
		"Please ensure that Minio binary has enough permissions to write in the backend",
		"",
	)

	uiErrUnableToReadFromBackend = newUIErrFn(
		"Unable to read data from the backend",
		"Please ensure that Minio binary has enough permission to read data from the backend",
		"",
	)

	uiErrPortAlreadyInUse = newUIErrFn(
		"Port is already in use",
		"Please ensure no other program is reserving the same address/port",
		"",
	)

	uiErrNoPrivilegesToAccessDirFiles = newUIErrFn(
		"Missing privileges to access to the specified path",
		"Please ensure the specified path can be accessed with your privileges",
		"",
	)

	uiErrSSLUnexpectedError = newUIErrFn(
		"Something wrong with your TLS certificate",
		"Please check the content of your certificate data",
		`Only PEM (x.509) format is accepted as valid public & private certificates.`,
	)

	uiErrSSLUnexpectedData = newUIErrFn(
		"Something wrong with your TLS certificate",
		"Please check your provided certificate",
		"",
	)

	uiErrSSLNoPassword = newUIErrFn(
		"The private key is protected by a password but no password is specified",
		"Please set the password to this variable environment `"+TLSPrivateKeyPassword+"` so Minio can decrypt the private",
		"",
	)

	uiErrNoCertsAndHTTPSEndpoints = newUIErrFn(
		"HTTPS is specified in endpoint urls but no certificate is found in the local machine",
		"Please add a certificate or switch to HTTP",
		"",
	)

	uiErrCertsAndHTTPEndpoints = newUIErrFn(
		"HTTP is specified in endpoint urls but the server in the local machine is configured with a TLS certificate",
		"Please remove the certificate in the configuration directory or switch to HTTPS",
		"",
	)

	uiErrSSLWrongPassword = newUIErrFn(
		"Unable to decrypt the private key using the provided password",
		"Please set the correct password in "+TLSPrivateKeyPassword,
		"",
	)

	uiErrUnexpectedDataContent = newUIErrFn(
		"Unexpected data content",
		"Please contact the team at https://slack.minio.io",
		"",
	)

	uiErrUnexpectedError = newUIErrFn(
		"Unexpected error",
		"Please contact the team at https://slack.minio.io",
		"",
	)
)
