/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/probe"
)

func migrateConfig() {
	// Purge all configs with version '1'.
	purgeV1()
	// Migrate version '2' to '3'.
	migrateV2ToV3()
}

// Version '1' is not supported anymore and deprecated, safe to delete.
func purgeV1() {
	cv1, err := loadConfigV1()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			return
		}
	}
	fatalIf(err.Trace(), "Unable to load config version ‘1’.", nil)

	if cv1.Version == "1" {
		console.Println("Unsupported config version ‘1’ found, removed successfully.")
		/// Purge old fsUsers.json file
		configPath, err := getConfigPath()
		fatalIf(err.Trace(), "Unable to retrieve config path.", nil)

		configFile := filepath.Join(configPath, "fsUsers.json")
		os.RemoveAll(configFile)
	}
	fatalIf(probe.NewError(errors.New("")), "Unexpected version found ‘"+cv1.Version+"’, cannot migrate.", nil)
}

func migrateV2ToV3() {
	cv2, err := loadConfigV2()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			return
		}
	}
	fatalIf(err.Trace(), "Unable to load config version ‘2’.", nil)
	if cv2.Version != "2" {
		return
	}
	serverConfig.SetAddr(":9000")
	serverConfig.SetCredential(credential{
		AccessKeyID:     cv2.Credentials.AccessKeyID,
		SecretAccessKey: cv2.Credentials.SecretAccessKey,
	})
	serverConfig.SetRegion(cv2.Credentials.Region)
	serverConfig.SetConsoleLogger(consoleLogger{
		Enable: true,
		Level:  "fatal",
	})
	flogger := fileLogger{}
	flogger.Level = "error"
	if cv2.FileLogger.Filename != "" {
		flogger.Enable = true
		flogger.Filename = cv2.FileLogger.Filename
	}
	serverConfig.SetFileLogger(flogger)

	slogger := syslogLogger{}
	slogger.Level = "debug"
	if cv2.SyslogLogger.Addr != "" {
		slogger.Enable = true
		slogger.Addr = cv2.SyslogLogger.Addr
	}
	serverConfig.SetSyslogLogger(slogger)

	err = serverConfig.Save()
	fatalIf(err.Trace(), "Migrating from version ‘"+cv2.Version+"’ to ‘"+serverConfig.GetVersion()+"’ failed.", nil)

	console.Println("Migration from version ‘" + cv2.Version + "’ to ‘" + serverConfig.GetVersion() + "’ completed successfully.")
}
