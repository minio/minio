/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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
	"strconv"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
)

// Check for updates and print a notification message
func checkUpdate(mode string) {
	// Its OK to ignore any errors during doUpdate() here.
	if updateMsg, _, currentReleaseTime, latestReleaseTime, err := getUpdateInfo(2*time.Second, mode); err == nil {
		if globalInplaceUpdateDisabled {
			logger.StartupMessage(updateMsg)
		} else {
			logger.StartupMessage(prepareUpdateMessage("Run `minio update`", latestReleaseTime.Sub(currentReleaseTime)))
		}
	}
}

func initConfig() {
	// Config file does not exist, we create it fresh and return upon success.
	if isFile(getConfigFile()) {
		logger.FatalIf(migrateConfig(), "Config migration failed.")
		logger.FatalIf(loadConfig(), "Unable to load config version: '%s'.", serverConfigVersion)
	} else {
		logger.FatalIf(newConfig(), "Unable to initialize minio config for the first time.")
		logger.Info("Created minio configuration file successfully at " + getConfigDir())
	}
}

func handleCommonCmdArgs(ctx *cli.Context) {

	var configDir string

	if ctx.IsSet("config-dir") {
		configDir = ctx.String("config-dir")
	} else if ctx.GlobalIsSet("config-dir") {
		configDir = ctx.GlobalString("config-dir")
		// cli package does not expose parent's "config-dir" option.  Below code is workaround.
		if configDir == "" || configDir == getConfigDir() {
			if ctx.Parent().GlobalIsSet("config-dir") {
				configDir = ctx.Parent().GlobalString("config-dir")
			}
		}
	} else {
		// Neither local nor global config-dir option is provided.  In this case, try to use
		// default config directory.
		configDir = getConfigDir()
		if configDir == "" {
			logger.FatalIf(errors.New("missing option"), "config-dir option must be provided.")
		}
	}

	if configDir == "" {
		logger.FatalIf(errors.New("empty directory"), "Configuration directory cannot be empty.")
	}

	// Disallow relative paths, figure out absolute paths.
	configDirAbs, err := filepath.Abs(configDir)
	logger.FatalIf(err, "Unable to fetch absolute path for config directory %s", configDir)
	setConfigDir(configDirAbs)
}

func handleCommonEnvVars() {
	// Start profiler if env is set.
	if profiler := os.Getenv("_MINIO_PROFILER"); profiler != "" {
		globalProfiler = startProfiler(profiler)
	}

	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if accessKey != "" && secretKey != "" {
		cred, err := auth.CreateCredentials(accessKey, secretKey)
		logger.FatalIf(err, "Invalid access/secret Key set in environment.")

		// credential Envs are set globally.
		globalIsEnvCreds = true
		globalActiveCred = cred
	}

	if browser := os.Getenv("MINIO_BROWSER"); browser != "" {
		browserFlag, err := ParseBrowserFlag(browser)
		if err != nil {
			logger.FatalIf(errors.New("invalid value"), "Unknown value ‘%s’ in MINIO_BROWSER environment variable.", browser)
		}

		// browser Envs are set globally, this does not represent
		// if browser is turned off or on.
		globalIsEnvBrowser = true
		globalIsBrowserEnabled = bool(browserFlag)
	}

	traceFile := os.Getenv("MINIO_HTTP_TRACE")
	if traceFile != "" {
		var err error
		globalHTTPTraceFile, err = os.OpenFile(traceFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
		logger.FatalIf(err, "error opening file %s", traceFile)
	}

	globalDomainName = os.Getenv("MINIO_DOMAIN")
	if globalDomainName != "" {
		globalIsEnvDomainName = true
	}

	if drives := os.Getenv("MINIO_CACHE_DRIVES"); drives != "" {
		driveList, err := parseCacheDrives(strings.Split(drives, cacheEnvDelimiter))
		logger.FatalIf(err, "Invalid value set in environment variable MINIO_CACHE_DRIVES %s.", drives)
		globalCacheDrives = driveList
		globalIsDiskCacheEnabled = true
	}
	if excludes := os.Getenv("MINIO_CACHE_EXCLUDE"); excludes != "" {
		excludeList, err := parseCacheExcludes(strings.Split(excludes, cacheEnvDelimiter))
		logger.FatalIf(err, "Invalid value set in environment variable MINIO_CACHE_EXCLUDE %s.", excludes)
		globalCacheExcludes = excludeList
	}
	if expiryStr := os.Getenv("MINIO_CACHE_EXPIRY"); expiryStr != "" {
		expiry, err := strconv.Atoi(expiryStr)
		logger.FatalIf(err, "Invalid value set in environment variable MINIO_CACHE_EXPIRY %s.", expiryStr)
		globalCacheExpiry = expiry
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
			logger.FatalIf(err, "Invalid value set in environment variable %s.", standardStorageClassEnv)
		}

		if rrsc := os.Getenv(reducedRedundancyStorageClassEnv); rrsc != "" {
			globalRRStorageClass, err = parseStorageClass(rrsc)
			logger.FatalIf(err, "Invalid value set in environment variable %s.", reducedRedundancyStorageClassEnv)
		}

		// Validation is done after parsing both the storage classes. This is needed because we need one
		// storage class value to deduce the correct value of the other storage class.
		if globalRRStorageClass.Scheme != "" {
			err = validateParity(globalStandardStorageClass.Parity, globalRRStorageClass.Parity)
			logger.FatalIf(err, "Invalid value set in environment variable %s.", reducedRedundancyStorageClassEnv)
			globalIsStorageClass = true
		}

		if globalStandardStorageClass.Scheme != "" {
			err = validateParity(globalStandardStorageClass.Parity, globalRRStorageClass.Parity)
			logger.FatalIf(err, "Invalid value set in environment variable %s.", standardStorageClassEnv)
			globalIsStorageClass = true
		}
	}

	// Get WORM environment variable.
	globalWORMEnabled = strings.EqualFold(os.Getenv("MINIO_WORM"), "on")
}
