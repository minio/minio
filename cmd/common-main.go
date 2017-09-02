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
)

// Check for updates and print a notification message
func checkUpdate(mode string) {
	// Its OK to ignore any errors during getUpdateInfo() here.
	if older, downloadURL, err := getUpdateInfo(1*time.Second, mode); err == nil {
		if updateMsg := computeUpdateMessage(downloadURL, older); updateMsg != "" {
			log.Println(updateMsg)
		}
	}
}

func enableLoggers() {
	fileLogTarget := serverConfig.Logger.GetFile()
	if fileLogTarget.Enable {
		err := InitFileLogger(&fileLogTarget)
		fatalIf(err, "Unable to initialize file logger")
		log.AddTarget(fileLogTarget)
	}

	consoleLogTarget := serverConfig.Logger.GetConsole()
	if consoleLogTarget.Enable {
		InitConsoleLogger(&consoleLogTarget)
	}

	log.SetConsoleTarget(consoleLogTarget)
}

func initConfig() {
	// Config file does not exist, we create it fresh and return upon success.
	if isFile(getConfigFile()) {
		fatalIf(migrateConfig(), "Config migration failed.")
		fatalIf(loadConfig(), "Unable to load config version: '%s'.", v19)
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
		cred, err := createCredential(accessKey, secretKey)
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

	if os.Getenv("MINIO_HTTP_TRACE") != "" {
		globalHTTPTrace = true
	}
}
