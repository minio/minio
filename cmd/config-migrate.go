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

package cmd

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/quick"
)

func migrateConfig() {
	// Purge all configs with version '1'.
	purgeV1()
	// Migrate version '2' to '3'.
	migrateV2ToV3()
	// Migrate version '3' to '4'.
	migrateV3ToV4()
	// Migrate version '4' to '5'.
	migrateV4ToV5()
	// Migrate version '5' to '6.
	migrateV5ToV6()
}

// Version '1' is not supported anymore and deprecated, safe to delete.
func purgeV1() {
	cv1, err := loadConfigV1()
	if err != nil && os.IsNotExist(err) {
		return
	}
	fatalIf(err, "Unable to load config version ‘1’.")

	if cv1.Version == "1" {
		console.Println("Removed unsupported config version ‘1’.")
		/// Purge old fsUsers.json file
		configPath, err := getConfigPath()
		fatalIf(err, "Unable to retrieve config path.")

		configFile := filepath.Join(configPath, "fsUsers.json")
		removeAll(configFile)
	}
	fatalIf(errors.New(""), "Failed to migrate unrecognized config version ‘"+cv1.Version+"’.")
}

// Version '2' to '3' config migration adds new fields and re-orders
// previous fields. Simplifies config for future additions.
func migrateV2ToV3() {
	cv2, err := loadConfigV2()
	if err != nil && os.IsNotExist(err) {
		return
	}
	fatalIf(err, "Unable to load config version ‘2’.")
	if cv2.Version != "2" {
		return
	}
	srvConfig := &configV3{}
	srvConfig.Version = "3"
	srvConfig.Addr = ":9000"
	srvConfig.Credential = credential{
		AccessKeyID:     cv2.Credentials.AccessKeyID,
		SecretAccessKey: cv2.Credentials.SecretAccessKey,
	}
	srvConfig.Region = cv2.Credentials.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature V4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = consoleLogger{
		Enable: true,
		Level:  "fatal",
	}
	flogger := fileLogger{}
	flogger.Level = "error"
	if cv2.FileLogger.Filename != "" {
		flogger.Enable = true
		flogger.Filename = cv2.FileLogger.Filename
	}
	srvConfig.Logger.File = flogger

	slogger := syslogLogger{}
	slogger.Level = "debug"
	if cv2.SyslogLogger.Addr != "" {
		slogger.Enable = true
		slogger.Addr = cv2.SyslogLogger.Addr
	}
	srvConfig.Logger.Syslog = slogger

	qc, err := quick.New(srvConfig)
	fatalIf(err, "Unable to initialize config.")

	configFile, err := getConfigFile()
	fatalIf(err, "Unable to get config file.")

	// Migrate the config.
	err = qc.Save(configFile)
	fatalIf(err, "Failed to migrate config from ‘"+cv2.Version+"’ to ‘"+srvConfig.Version+"’ failed.")

	console.Println("Migration from version ‘" + cv2.Version + "’ to ‘" + srvConfig.Version + "’ completed successfully.")
}

// Version '3' to '4' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV3ToV4() {
	cv3, err := loadConfigV3()
	if err != nil && os.IsNotExist(err) {
		return
	}
	fatalIf(err, "Unable to load config version ‘3’.")
	if cv3.Version != "3" {
		return
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV4{}
	srvConfig.Version = "4"
	srvConfig.Credential = cv3.Credential
	srvConfig.Region = cv3.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv3.Logger.Console
	srvConfig.Logger.File = cv3.Logger.File
	srvConfig.Logger.Syslog = cv3.Logger.Syslog

	qc, err := quick.New(srvConfig)
	fatalIf(err, "Unable to initialize the quick config.")
	configFile, err := getConfigFile()
	fatalIf(err, "Unable to get config file.")

	err = qc.Save(configFile)
	fatalIf(err, "Failed to migrate config from ‘"+cv3.Version+"’ to ‘"+srvConfig.Version+"’ failed.")

	console.Println("Migration from version ‘" + cv3.Version + "’ to ‘" + srvConfig.Version + "’ completed successfully.")
}

// Version '5' to '6' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV5ToV6() {
	cv5, err := loadConfigV5()
	if err != nil && os.IsNotExist(err) {
		return
	}
	fatalIf(err, "Unable to load config version ‘5’.")
	if cv5.Version != "5" {
		return
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &serverConfigV6{}
	srvConfig.Version = globalMinioConfigVersion
	srvConfig.Credential = cv5.Credential
	srvConfig.Region = cv5.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv5.Logger.Console
	srvConfig.Logger.File = cv5.Logger.File
	srvConfig.Logger.Syslog = cv5.Logger.Syslog
	srvConfig.Notify.AMQP = map[string]amqpNotify{
		"1": {
			Enable:      cv5.Logger.AMQP.Enable,
			URL:         cv5.Logger.AMQP.URL,
			Exchange:    cv5.Logger.AMQP.Exchange,
			RoutingKey:  cv5.Logger.AMQP.RoutingKey,
			Mandatory:   cv5.Logger.AMQP.Mandatory,
			Immediate:   cv5.Logger.AMQP.Immediate,
			Durable:     cv5.Logger.AMQP.Durable,
			Internal:    cv5.Logger.AMQP.Internal,
			NoWait:      cv5.Logger.AMQP.NoWait,
			AutoDeleted: cv5.Logger.AMQP.AutoDeleted,
		},
	}
	srvConfig.Notify.ElasticSearch = map[string]elasticSearchNotify{
		"1": {
			Enable: cv5.Logger.ElasticSearch.Enable,
			URL:    cv5.Logger.ElasticSearch.URL,
			Index:  cv5.Logger.ElasticSearch.Index,
		},
	}
	srvConfig.Notify.Redis = map[string]redisNotify{
		"1": {
			Enable:   cv5.Logger.Redis.Enable,
			Addr:     cv5.Logger.Redis.Addr,
			Password: cv5.Logger.Redis.Password,
			Key:      cv5.Logger.Redis.Key,
		},
	}

	qc, err := quick.New(srvConfig)
	fatalIf(err, "Unable to initialize the quick config.")
	configFile, err := getConfigFile()
	fatalIf(err, "Unable to get config file.")

	err = qc.Save(configFile)
	fatalIf(err, "Failed to migrate config from ‘"+cv5.Version+"’ to ‘"+srvConfig.Version+"’ failed.")

	console.Println("Migration from version ‘" + cv5.Version + "’ to ‘" + srvConfig.Version + "’ completed successfully.")
}

// Version '4' to '5' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV4ToV5() {
	cv4, err := loadConfigV4()
	if err != nil && os.IsNotExist(err) {
		return
	}
	fatalIf(err, "Unable to load config version ‘4’.")
	if cv4.Version != "4" {
		return
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV5{}
	srvConfig.Version = globalMinioConfigVersion
	srvConfig.Credential = cv4.Credential
	srvConfig.Region = cv4.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = "us-east-1"
	}
	srvConfig.Logger.Console = cv4.Logger.Console
	srvConfig.Logger.File = cv4.Logger.File
	srvConfig.Logger.Syslog = cv4.Logger.Syslog
	srvConfig.Logger.AMQP.Enable = false
	srvConfig.Logger.ElasticSearch.Enable = false
	srvConfig.Logger.Redis.Enable = false

	qc, err := quick.New(srvConfig)
	fatalIf(err, "Unable to initialize the quick config.")
	configFile, err := getConfigFile()
	fatalIf(err, "Unable to get config file.")

	err = qc.Save(configFile)
	fatalIf(err, "Failed to migrate config from ‘"+cv4.Version+"’ to ‘"+srvConfig.Version+"’ failed.")

	console.Println("Migration from version ‘" + cv4.Version + "’ to ‘" + srvConfig.Version + "’ completed successfully.")
}
