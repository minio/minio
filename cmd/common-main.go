/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/auth"
)

// Check for updates and print a notification message
func checkUpdate(mode string) {
	// Its OK to ignore any errors during doUpdate() here.
	if updateMsg, _, currentReleaseTime, latestReleaseTime, err := getUpdateInfo(2*time.Second, mode); err == nil {
		if globalInplaceUpdateDisabled {
			log.Println(updateMsg)
		} else {
			log.Println(prepareUpdateMessage("Run `minio update`", latestReleaseTime.Sub(currentReleaseTime)))
		}
	}
}

func initConfig() {
	// Config file does not exist, we create it fresh and return upon success.
	if isFile(getConfigFile()) {
		fatalIf(migrateConfig(), "Config migration failed.")
		fatalIf(loadConfig(), "Unable to load config version: '%s'.", serverConfigVersion)
	} else {
		fatalIf(newConfig(), "Unable to initialize minio config for the first time.")
		log.Println("Created minio configuration file successfully at " + getConfigDir())
	}
}

func handleCommonCmdArgs(ctx *cli.Context) {
	// Set configuration directory.
	{
		// Get configuration directory from command line argument.
		configDir := ctx.String("config-dir")
		if !ctx.IsSet("config-dir") && ctx.GlobalIsSet("config-dir") {
			configDir = ctx.GlobalString("config-dir")
		}
		if configDir == "" {
			fatalIf(errors.New("empty directory"), "Configuration directory cannot be empty.")
		}

		// Disallow relative paths, figure out absolute paths.
		configDirAbs, err := filepath.Abs(configDir)
		fatalIf(err, "Unable to fetch absolute path for config directory %s", configDir)

		setConfigDir(configDirAbs)
	}
}

func handleCommonEnvVars() {
	// Start profiler if env is set.
	if profiler := os.Getenv("_MINIO_PROFILER"); profiler != "" {
		globalProfiler = startProfiler(profiler)
	}

	// Check if object cache is disabled.
	globalXLObjCacheDisabled = strings.EqualFold(os.Getenv("_MINIO_CACHE"), "off")

	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if accessKey != "" && secretKey != "" {
		cred, err := auth.CreateCredentials(accessKey, secretKey)
		fatalIf(err, "Invalid access/secret Key set in environment.")

		// credential Envs are set globally.
		globalIsEnvCreds = true
		globalActiveCred = cred
	}

	if browser := os.Getenv("MINIO_BROWSER"); browser != "" {
		browserFlag, err := ParseBrowserFlag(browser)
		if err != nil {
			fatalIf(errors.New("invalid value"), "Unknown value ‘%s’ in MINIO_BROWSER environment variable.", browser)
		}

		// browser Envs are set globally, this does not represent
		// if browser is turned off or on.
		globalIsEnvBrowser = true
		globalIsBrowserEnabled = bool(browserFlag)
	}

	globalHTTPTrace = os.Getenv("MINIO_HTTP_TRACE") != ""

	globalDomainName = os.Getenv("MINIO_DOMAIN")
	if globalDomainName != "" {
		globalIsEnvDomainName = true
	}

	// In place update is true by default if the MINIO_UPDATE is not set
	// or is not set to 'off', if MINIO_UPDATE is set to 'off' then
	// in-place update is off.
	globalInplaceUpdateDisabled = strings.EqualFold(os.Getenv("MINIO_UPDATE"), "off")

	// Validate and store the storage class env variables only for XL/Dist XL setups
	if globalIsXL {
		var err error

		// Check for environment variables and parse into storageClass struct
		if ssc := os.Getenv(standardStorageClassEnv); ssc != "" {
			globalStandardStorageClass, err = parseStorageClass(ssc)
			fatalIf(err, "Invalid value set in environment variable %s.", standardStorageClassEnv)
		}

		if rrsc := os.Getenv(reducedRedundancyStorageClassEnv); rrsc != "" {
			globalRRStorageClass, err = parseStorageClass(rrsc)
			fatalIf(err, "Invalid value set in environment variable %s.", reducedRedundancyStorageClassEnv)
		}

		// Validation is done after parsing both the storage classes. This is needed because we need one
		// storage class value to deduce the correct value of the other storage class.
		if globalRRStorageClass.Scheme != "" {
			err := validateRRSParity(globalRRStorageClass.Parity, globalStandardStorageClass.Parity)
			fatalIf(err, "Invalid value set in environment variable %s.", reducedRedundancyStorageClassEnv)
			globalIsStorageClass = true
		}

		if globalStandardStorageClass.Scheme != "" {
			err := validateSSParity(globalStandardStorageClass.Parity, globalRRStorageClass.Parity)
			fatalIf(err, "Invalid value set in environment variable %s.", standardStorageClassEnv)
			globalIsStorageClass = true
		}
	}
}
