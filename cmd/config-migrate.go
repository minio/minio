/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"fmt"
	"os"
	"path/filepath"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/quick"
)

// DO NOT EDIT following message template, please open a github issue to discuss instead.
var configMigrateMSGTemplate = "Configuration file %s migrated from version '%s' to '%s' successfully.\n"

// Migrates all config versions from "1" to "18".
func migrateConfig() error {
	// Purge all configs with version '1',
	// this is a special case since version '1' used
	// to be a filename 'fsUsers.json' not 'config.json'.
	if err := purgeV1(); err != nil {
		return err
	}

	// Load only config version information.
	version, err := quick.GetVersion(getConfigFile())
	if err != nil {
		return err
	}

	// Conditional to migrate only relevant config versions.
	// Upon success migration continues to the next version in sequence.
	switch version {
	case "2":
		// Migrate version '2' to '3'.
		if err = migrateV2ToV3(); err != nil {
			return err
		}
		fallthrough
	case "3":
		// Migrate version '3' to '4'.
		if err = migrateV3ToV4(); err != nil {
			return err
		}
		fallthrough
	case "4":
		// Migrate version '4' to '5'.
		if err = migrateV4ToV5(); err != nil {
			return err
		}
		fallthrough
	case "5":
		// Migrate version '5' to '6.
		if err = migrateV5ToV6(); err != nil {
			return err
		}
		fallthrough
	case "6":
		// Migrate version '6' to '7'.
		if err = migrateV6ToV7(); err != nil {
			return err
		}
		fallthrough
	case "7":
		// Migrate version '7' to '8'.
		if err = migrateV7ToV8(); err != nil {
			return err
		}
		fallthrough
	case "8":
		// Migrate version '8' to '9'.
		if err = migrateV8ToV9(); err != nil {
			return err
		}
		fallthrough
	case "9":
		// Migrate version '9' to '10'.
		if err = migrateV9ToV10(); err != nil {
			return err
		}
		fallthrough
	case "10":
		// Migrate version '10' to '11'.
		if err = migrateV10ToV11(); err != nil {
			return err
		}
		fallthrough
	case "11":
		// Migrate version '11' to '12'.
		if err = migrateV11ToV12(); err != nil {
			return err
		}
		fallthrough
	case "12":
		// Migrate version '12' to '13'.
		if err = migrateV12ToV13(); err != nil {
			return err
		}
		fallthrough
	case "13":
		// Migrate version '13' to '14'.
		if err = migrateV13ToV14(); err != nil {
			return err
		}
		fallthrough
	case "14":
		// Migrate version '14' to '15'.
		if err = migrateV14ToV15(); err != nil {
			return err
		}
		fallthrough
	case "15":
		// Migrate version '15' to '16'.
		if err = migrateV15ToV16(); err != nil {
			return err
		}
		fallthrough
	case "16":
		// Migrate version '16' to '17'.
		if err = migrateV16ToV17(); err != nil {
			return err
		}
		fallthrough
	case "17":
		// Migrate version '17' to '18'.
		if err = migrateV17ToV18(); err != nil {
			return err
		}
		fallthrough
	case "18":
		// Migrate version '18' to '19'.
		if err = migrateV18ToV19(); err != nil {
			return err
		}
		fallthrough
	case "19":
		if err = migrateV19ToV20(); err != nil {
			return err
		}
		fallthrough
	case "20":
		if err = migrateV20ToV21(); err != nil {
			return err
		}
		fallthrough
	case "21":
		if err = migrateV21ToV22(); err != nil {
			return err
		}
	case serverConfigVersion:
		// No migration needed. this always points to current version.
		err = nil
	}
	return err
}

// Version '1' is not supported anymore and deprecated, safe to delete.
func purgeV1() error {
	configFile := filepath.Join(getConfigDir(), "fsUsers.json")

	cv1 := &configV1{}
	_, err := quick.Load(configFile, cv1)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘1’. %v", err)
	}
	if cv1.Version != "1" {
		return fmt.Errorf("unrecognized config version ‘%s’", cv1.Version)
	}

	os.RemoveAll(configFile)
	log.Println("Removed unsupported config version ‘1’.")
	return nil
}

// Version '2' to '3' config migration adds new fields and re-orders
// previous fields. Simplifies config for future additions.
func migrateV2ToV3() error {
	configFile := getConfigFile()

	cv2 := &configV2{}
	_, err := quick.Load(configFile, cv2)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘2’. %v", err)
	}
	if cv2.Version != "2" {
		return nil
	}

	cred, err := auth.CreateCredentials(cv2.Credentials.AccessKey, cv2.Credentials.SecretKey)
	if err != nil {
		return fmt.Errorf("Invalid credential in V2 configuration file. %v", err)
	}

	srvConfig := &configV3{}
	srvConfig.Version = "3"
	srvConfig.Addr = ":9000"
	srvConfig.Credential = cred
	srvConfig.Region = cv2.Credentials.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature V4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = consoleLoggerV1{
		Enable: true,
		Level:  "fatal",
	}
	flogger := fileLoggerV1{}
	flogger.Level = "error"
	if cv2.FileLogger.Filename != "" {
		flogger.Enable = true
		flogger.Filename = cv2.FileLogger.Filename
	}
	srvConfig.Logger.File = flogger

	slogger := syslogLoggerV3{}
	slogger.Level = "debug"
	if cv2.SyslogLogger.Addr != "" {
		slogger.Enable = true
		slogger.Addr = cv2.SyslogLogger.Addr
	}
	srvConfig.Logger.Syslog = slogger

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv2.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv2.Version, srvConfig.Version)
	return nil
}

// Version '3' to '4' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV3ToV4() error {
	configFile := getConfigFile()

	cv3 := &configV3{}
	_, err := quick.Load(configFile, cv3)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘3’. %v", err)
	}
	if cv3.Version != "3" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV4{}
	srvConfig.Version = "4"
	srvConfig.Credential = cv3.Credential
	srvConfig.Region = cv3.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv3.Logger.Console
	srvConfig.Logger.File = cv3.Logger.File
	srvConfig.Logger.Syslog = cv3.Logger.Syslog

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv3.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv3.Version, srvConfig.Version)
	return nil
}

// Version '4' to '5' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV4ToV5() error {
	configFile := getConfigFile()

	cv4 := &configV4{}
	_, err := quick.Load(configFile, cv4)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘4’. %v", err)
	}
	if cv4.Version != "4" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV5{}
	srvConfig.Version = "5"
	srvConfig.Credential = cv4.Credential
	srvConfig.Region = cv4.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv4.Logger.Console
	srvConfig.Logger.File = cv4.Logger.File
	srvConfig.Logger.Syslog = cv4.Logger.Syslog
	srvConfig.Logger.AMQP.Enable = false
	srvConfig.Logger.ElasticSearch.Enable = false
	srvConfig.Logger.Redis.Enable = false

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv4.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv4.Version, srvConfig.Version)
	return nil
}

// Version '5' to '6' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV5ToV6() error {
	configFile := getConfigFile()

	cv5 := &configV5{}
	_, err := quick.Load(configFile, cv5)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘5’. %v", err)
	}
	if cv5.Version != "5" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &configV6{}
	srvConfig.Version = "6"
	srvConfig.Credential = cv5.Credential
	srvConfig.Region = cv5.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
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

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv5.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv5.Version, srvConfig.Version)
	return nil
}

// Version '6' to '7' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV6ToV7() error {
	configFile := getConfigFile()

	cv6 := &configV6{}
	_, err := quick.Load(configFile, cv6)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘6’. %v", err)
	}
	if cv6.Version != "6" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &serverConfigV7{}
	srvConfig.Version = "7"
	srvConfig.Credential = cv6.Credential
	srvConfig.Region = cv6.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv6.Logger.Console
	srvConfig.Logger.File = cv6.Logger.File
	srvConfig.Logger.Syslog = cv6.Logger.Syslog
	srvConfig.Notify.AMQP = make(map[string]amqpNotify)
	srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
	srvConfig.Notify.Redis = make(map[string]redisNotify)
	if len(cv6.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv6.Notify.AMQP
	}
	if len(cv6.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv6.Notify.ElasticSearch
	}
	if len(cv6.Notify.Redis) == 0 {
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv6.Notify.Redis
	}

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv6.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv6.Version, srvConfig.Version)
	return nil
}

// Version '7' to '8' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV7ToV8() error {
	configFile := getConfigFile()

	cv7 := &serverConfigV7{}
	_, err := quick.Load(configFile, cv7)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘7’. %v", err)
	}
	if cv7.Version != "7" {
		return nil
	}

	// Save only the new fields, ignore the rest.
	srvConfig := &serverConfigV8{}
	srvConfig.Version = "8"
	srvConfig.Credential = cv7.Credential
	srvConfig.Region = cv7.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv7.Logger.Console
	srvConfig.Logger.File = cv7.Logger.File
	srvConfig.Logger.Syslog = cv7.Logger.Syslog
	srvConfig.Notify.AMQP = make(map[string]amqpNotify)
	srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
	srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
	srvConfig.Notify.Redis = make(map[string]redisNotify)
	srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
	if len(cv7.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv7.Notify.AMQP
	}
	if len(cv7.Notify.NATS) == 0 {
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv7.Notify.NATS
	}
	if len(cv7.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv7.Notify.ElasticSearch
	}
	if len(cv7.Notify.Redis) == 0 {
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv7.Notify.Redis
	}

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv7.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv7.Version, srvConfig.Version)
	return nil
}

// Version '8' to '9' migration. Adds postgresql notifier
// configuration, but it's otherwise the same as V8.
func migrateV8ToV9() error {
	configFile := getConfigFile()

	cv8 := &serverConfigV8{}
	_, err := quick.Load(configFile, cv8)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘8’. %v", err)
	}
	if cv8.Version != "8" {
		return nil
	}

	// Copy over fields from V8 into V9 config struct
	srvConfig := &serverConfigV9{}
	srvConfig.Version = "9"
	srvConfig.Credential = cv8.Credential
	srvConfig.Region = cv8.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv8.Logger.Console
	srvConfig.Logger.Console.Level = "error"
	srvConfig.Logger.File = cv8.Logger.File
	srvConfig.Logger.Syslog = cv8.Logger.Syslog

	// check and set notifiers config
	if len(cv8.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv8.Notify.AMQP
	}
	if len(cv8.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv8.Notify.NATS
	}
	if len(cv8.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv8.Notify.ElasticSearch
	}
	if len(cv8.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv8.Notify.Redis
	}
	if len(cv8.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv8.Notify.PostgreSQL
	}

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv8.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv8.Version, srvConfig.Version)
	return nil
}

// Version '9' to '10' migration. Remove syslog config
// but it's otherwise the same as V9.
func migrateV9ToV10() error {
	configFile := getConfigFile()

	cv9 := &serverConfigV9{}
	_, err := quick.Load(configFile, cv9)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘9’. %v", err)
	}
	if cv9.Version != "9" {
		return nil
	}

	// Copy over fields from V9 into V10 config struct
	srvConfig := &serverConfigV10{}
	srvConfig.Version = "10"
	srvConfig.Credential = cv9.Credential
	srvConfig.Region = cv9.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv9.Logger.Console
	srvConfig.Logger.File = cv9.Logger.File

	// check and set notifiers config
	if len(cv9.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv9.Notify.AMQP
	}
	if len(cv9.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv9.Notify.NATS
	}
	if len(cv9.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv9.Notify.ElasticSearch
	}
	if len(cv9.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv9.Notify.Redis
	}
	if len(cv9.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv9.Notify.PostgreSQL
	}

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv9.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv9.Version, srvConfig.Version)
	return nil
}

// Version '10' to '11' migration. Add support for Kafka
// notifications.
func migrateV10ToV11() error {
	configFile := getConfigFile()

	cv10 := &serverConfigV10{}
	_, err := quick.Load(configFile, cv10)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘10’. %v", err)
	}
	if cv10.Version != "10" {
		return nil
	}

	// Copy over fields from V10 into V11 config struct
	srvConfig := &serverConfigV11{}
	srvConfig.Version = "11"
	srvConfig.Credential = cv10.Credential
	srvConfig.Region = cv10.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv10.Logger.Console
	srvConfig.Logger.File = cv10.Logger.File

	// check and set notifiers config
	if len(cv10.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv10.Notify.AMQP
	}
	if len(cv10.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv10.Notify.NATS
	}
	if len(cv10.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv10.Notify.ElasticSearch
	}
	if len(cv10.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv10.Notify.Redis
	}
	if len(cv10.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv10.Notify.PostgreSQL
	}
	// V10 will not have a Kafka config. So we initialize one here.
	srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
	srvConfig.Notify.Kafka["1"] = kafkaNotify{}

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv10.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv10.Version, srvConfig.Version)
	return nil
}

// Version '11' to '12' migration. Add support for NATS streaming
// notifications.
func migrateV11ToV12() error {
	configFile := getConfigFile()

	cv11 := &serverConfigV11{}
	_, err := quick.Load(configFile, cv11)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘11’. %v", err)
	}
	if cv11.Version != "11" {
		return nil
	}

	// Copy over fields from V11 into V12 config struct
	srvConfig := &serverConfigV12{}
	srvConfig.Version = "12"
	srvConfig.Credential = cv11.Credential
	srvConfig.Region = cv11.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv11.Logger.Console
	srvConfig.Logger.File = cv11.Logger.File

	// check and set notifiers config
	if len(cv11.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv11.Notify.AMQP
	}
	if len(cv11.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv11.Notify.ElasticSearch
	}
	if len(cv11.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv11.Notify.Redis
	}
	if len(cv11.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv11.Notify.PostgreSQL
	}
	if len(cv11.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv11.Notify.Kafka
	}

	// V12 will have an updated config of nats. So we create a new one or we
	// update the old one if found.
	if len(cv11.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		for k, v := range cv11.Notify.NATS {
			n := natsNotify{}
			n.Enable = v.Enable
			n.Address = v.Address
			n.Subject = v.Subject
			n.Username = v.Username
			n.Password = v.Password
			n.Token = v.Token
			n.Secure = v.Secure
			n.PingInterval = v.PingInterval
			srvConfig.Notify.NATS[k] = n
		}
	}

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv11.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv11.Version, srvConfig.Version)
	return nil
}

// Version '12' to '13' migration. Add support for custom webhook endpoint.
func migrateV12ToV13() error {
	configFile := getConfigFile()

	cv12 := &serverConfigV12{}
	_, err := quick.Load(configFile, cv12)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘12’. %v", err)
	}
	if cv12.Version != "12" {
		return nil
	}

	// Copy over fields from V12 into V13 config struct
	srvConfig := &serverConfigV13{
		Logger: &loggerV7{},
		Notify: &notifier{},
	}
	srvConfig.Version = "13"
	srvConfig.Credential = cv12.Credential
	srvConfig.Region = cv12.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv12.Logger.Console
	srvConfig.Logger.File = cv12.Logger.File

	// check and set notifiers config
	if len(cv12.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv12.Notify.AMQP
	}
	if len(cv12.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv12.Notify.ElasticSearch
	}
	if len(cv12.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv12.Notify.Redis
	}
	if len(cv12.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv12.Notify.PostgreSQL
	}
	if len(cv12.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv12.Notify.Kafka
	}
	if len(cv12.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv12.Notify.NATS
	}

	// V12 will not have a webhook config. So we initialize one here.
	srvConfig.Notify.Webhook = make(map[string]webhookNotify)
	srvConfig.Notify.Webhook["1"] = webhookNotify{}

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv12.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv12.Version, srvConfig.Version)
	return nil
}

// Version '13' to '14' migration. Add support for browser param.
func migrateV13ToV14() error {
	configFile := getConfigFile()

	cv13 := &serverConfigV13{}
	_, err := quick.Load(configFile, cv13)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘13’. %v", err)
	}
	if cv13.Version != "13" {
		return nil
	}

	// Copy over fields from V13 into V14 config struct
	srvConfig := &serverConfigV14{
		Logger: &loggerV7{},
		Notify: &notifier{},
	}
	srvConfig.Version = "14"
	srvConfig.Credential = cv13.Credential
	srvConfig.Region = cv13.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv13.Logger.Console
	srvConfig.Logger.File = cv13.Logger.File

	// check and set notifiers config
	if len(cv13.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv13.Notify.AMQP
	}
	if len(cv13.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv13.Notify.ElasticSearch
	}
	if len(cv13.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv13.Notify.Redis
	}
	if len(cv13.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv13.Notify.PostgreSQL
	}
	if len(cv13.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv13.Notify.Kafka
	}
	if len(cv13.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv13.Notify.NATS
	}
	if len(cv13.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv13.Notify.Webhook
	}

	// Set the new browser parameter to true by default
	srvConfig.Browser = true

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv13.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv13.Version, srvConfig.Version)
	return nil
}

// Version '14' to '15' migration. Add support for MySQL notifications.
func migrateV14ToV15() error {
	configFile := getConfigFile()

	cv14 := &serverConfigV14{}
	_, err := quick.Load(configFile, cv14)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘14’. %v", err)
	}
	if cv14.Version != "14" {
		return nil
	}

	// Copy over fields from V14 into V15 config struct
	srvConfig := &serverConfigV15{
		Logger: &loggerV7{},
		Notify: &notifier{},
	}
	srvConfig.Version = "15"
	srvConfig.Credential = cv14.Credential
	srvConfig.Region = cv14.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}
	srvConfig.Logger.Console = cv14.Logger.Console
	srvConfig.Logger.File = cv14.Logger.File

	// check and set notifiers config
	if len(cv14.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv14.Notify.AMQP
	}
	if len(cv14.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv14.Notify.ElasticSearch
	}
	if len(cv14.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv14.Notify.Redis
	}
	if len(cv14.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv14.Notify.PostgreSQL
	}
	if len(cv14.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv14.Notify.Kafka
	}
	if len(cv14.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv14.Notify.NATS
	}
	if len(cv14.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv14.Notify.Webhook
	}

	// V14 will not have mysql support, so we add that here.
	srvConfig.Notify.MySQL = make(map[string]mySQLNotify)
	srvConfig.Notify.MySQL["1"] = mySQLNotify{}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv14.Browser

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv14.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv14.Version, srvConfig.Version)
	return nil
}

// Version '15' to '16' migration. Remove log level in loggers
// and rename 'fileName' filed in File logger to 'filename'
func migrateV15ToV16() error {
	configFile := getConfigFile()

	cv15 := &serverConfigV15{}
	_, err := quick.Load(configFile, cv15)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘15’. %v", err)
	}
	if cv15.Version != "15" {
		return nil
	}

	// Copy over fields from V15 into V16 config struct
	srvConfig := &serverConfigV16{
		Logger: &loggers{},
		Notify: &notifier{},
	}
	srvConfig.Version = "16"
	srvConfig.Credential = cv15.Credential
	srvConfig.Region = cv15.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// check and set notifiers config
	if len(cv15.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv15.Notify.AMQP
	}
	if len(cv15.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		srvConfig.Notify.ElasticSearch = cv15.Notify.ElasticSearch
	}
	if len(cv15.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		srvConfig.Notify.Redis = cv15.Notify.Redis
	}
	if len(cv15.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		srvConfig.Notify.PostgreSQL = cv15.Notify.PostgreSQL
	}
	if len(cv15.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv15.Notify.Kafka
	}
	if len(cv15.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv15.Notify.NATS
	}
	if len(cv15.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv15.Notify.Webhook
	}
	if len(cv15.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]mySQLNotify)
		srvConfig.Notify.MySQL["1"] = mySQLNotify{}
	} else {
		srvConfig.Notify.MySQL = cv15.Notify.MySQL
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv15.Browser

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv15.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv15.Version, srvConfig.Version)
	return nil
}

// Version '16' to '17' migration. Adds "format" configuration
// parameter for database targets.
func migrateV16ToV17() error {
	configFile := getConfigFile()

	cv16 := &serverConfigV16{}
	_, err := quick.Load(configFile, cv16)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘16’. %v", err)
	}
	if cv16.Version != "16" {
		return nil
	}

	// Copy over fields from V16 into V17 config struct
	srvConfig := &serverConfigV17{
		Logger: &loggers{},
		Notify: &notifier{},
	}
	srvConfig.Version = "17"
	srvConfig.Credential = cv16.Credential
	srvConfig.Region = cv16.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	srvConfig.Logger.Console = cv16.Logger.Console
	srvConfig.Logger.File = cv16.Logger.File

	// check and set notifiers config
	if len(cv16.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		srvConfig.Notify.AMQP = cv16.Notify.AMQP
	}
	if len(cv16.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{}
	} else {
		// IMPORTANT NOTE: Future migrations should remove
		// this as existing configuration will already contain
		// a value for the "format" parameter.
		for k, v := range cv16.Notify.ElasticSearch.Clone() {
			v.Format = formatNamespace
			cv16.Notify.ElasticSearch[k] = v
		}
		srvConfig.Notify.ElasticSearch = cv16.Notify.ElasticSearch
	}
	if len(cv16.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{}
	} else {
		// IMPORTANT NOTE: Future migrations should remove
		// this as existing configuration will already contain
		// a value for the "format" parameter.
		for k, v := range cv16.Notify.Redis.Clone() {
			v.Format = formatNamespace
			cv16.Notify.Redis[k] = v
		}
		srvConfig.Notify.Redis = cv16.Notify.Redis
	}
	if len(cv16.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{}
	} else {
		// IMPORTANT NOTE: Future migrations should remove
		// this as existing configuration will already contain
		// a value for the "format" parameter.
		for k, v := range cv16.Notify.PostgreSQL.Clone() {
			v.Format = formatNamespace
			cv16.Notify.PostgreSQL[k] = v
		}
		srvConfig.Notify.PostgreSQL = cv16.Notify.PostgreSQL
	}
	if len(cv16.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv16.Notify.Kafka
	}
	if len(cv16.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv16.Notify.NATS
	}
	if len(cv16.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv16.Notify.Webhook
	}
	if len(cv16.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]mySQLNotify)
		srvConfig.Notify.MySQL["1"] = mySQLNotify{}
	} else {
		// IMPORTANT NOTE: Future migrations should remove
		// this as existing configuration will already contain
		// a value for the "format" parameter.
		for k, v := range cv16.Notify.MySQL.Clone() {
			v.Format = formatNamespace
			cv16.Notify.MySQL[k] = v
		}
		srvConfig.Notify.MySQL = cv16.Notify.MySQL
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv16.Browser

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv16.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv16.Version, srvConfig.Version)
	return nil
}

// Version '17' to '18' migration. Adds "deliveryMode" configuration
// parameter for AMQP notification target
func migrateV17ToV18() error {
	configFile := getConfigFile()

	cv17 := &serverConfigV17{}
	_, err := quick.Load(configFile, cv17)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘17’. %v", err)
	}
	if cv17.Version != "17" {
		return nil
	}

	// Copy over fields from V17 into V18 config struct
	srvConfig := &serverConfigV17{
		Logger: &loggers{},
		Notify: &notifier{},
	}
	srvConfig.Version = "18"
	srvConfig.Credential = cv17.Credential
	srvConfig.Region = cv17.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	srvConfig.Logger.Console = cv17.Logger.Console
	srvConfig.Logger.File = cv17.Logger.File

	// check and set notifiers config
	if len(cv17.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		// New deliveryMode parameter is added for AMQP,
		// default value is already 0, so nothing to
		// explicitly migrate here.
		srvConfig.Notify.AMQP = cv17.Notify.AMQP
	}
	if len(cv17.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.ElasticSearch = cv17.Notify.ElasticSearch
	}
	if len(cv17.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.Redis = cv17.Notify.Redis
	}
	if len(cv17.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv17.Notify.PostgreSQL
	}
	if len(cv17.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv17.Notify.Kafka
	}
	if len(cv17.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv17.Notify.NATS
	}
	if len(cv17.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv17.Notify.Webhook
	}
	if len(cv17.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]mySQLNotify)
		srvConfig.Notify.MySQL["1"] = mySQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.MySQL = cv17.Notify.MySQL
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv17.Browser

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv17.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv17.Version, srvConfig.Version)
	return nil
}

func migrateV18ToV19() error {
	configFile := getConfigFile()

	cv18 := &serverConfigV18{}
	_, err := quick.Load(configFile, cv18)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘18’. %v", err)
	}
	if cv18.Version != "18" {
		return nil
	}

	// Copy over fields from V18 into V19 config struct
	srvConfig := &serverConfigV18{
		Logger: &loggers{},
		Notify: &notifier{},
	}
	srvConfig.Version = "19"
	srvConfig.Credential = cv18.Credential
	srvConfig.Region = cv18.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	srvConfig.Logger.Console = cv18.Logger.Console
	srvConfig.Logger.File = cv18.Logger.File

	// check and set notifiers config
	if len(cv18.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		// New deliveryMode parameter is added for AMQP,
		// default value is already 0, so nothing to
		// explicitly migrate here.
		srvConfig.Notify.AMQP = cv18.Notify.AMQP
	}
	if len(cv18.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.ElasticSearch = cv18.Notify.ElasticSearch
	}
	if len(cv18.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.Redis = cv18.Notify.Redis
	}
	if len(cv18.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv18.Notify.PostgreSQL
	}
	if len(cv18.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv18.Notify.Kafka
	}
	if len(cv18.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv18.Notify.NATS
	}
	if len(cv18.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv18.Notify.Webhook
	}
	if len(cv18.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]mySQLNotify)
		srvConfig.Notify.MySQL["1"] = mySQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.MySQL = cv18.Notify.MySQL
	}

	// V18 will not have mqtt support, so we add that here.
	srvConfig.Notify.MQTT = make(map[string]mqttNotify)
	srvConfig.Notify.MQTT["1"] = mqttNotify{}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv18.Browser

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv18.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv18.Version, srvConfig.Version)
	return nil
}

func migrateV19ToV20() error {
	configFile := getConfigFile()

	cv19 := &serverConfigV19{}
	_, err := quick.Load(configFile, cv19)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘18’. %v", err)
	}
	if cv19.Version != "19" {
		return nil
	}

	// Copy over fields from V19 into V20 config struct
	srvConfig := &serverConfigV20{
		Logger: &loggers{},
		Notify: &notifier{},
	}
	srvConfig.Version = "20"
	srvConfig.Credential = cv19.Credential
	srvConfig.Region = cv19.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	srvConfig.Logger.Console = cv19.Logger.Console
	srvConfig.Logger.File = cv19.Logger.File

	// check and set notifiers config
	if len(cv19.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		// New deliveryMode parameter is added for AMQP,
		// default value is already 0, so nothing to
		// explicitly migrate here.
		srvConfig.Notify.AMQP = cv19.Notify.AMQP
	}
	if len(cv19.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.ElasticSearch = cv19.Notify.ElasticSearch
	}
	if len(cv19.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.Redis = cv19.Notify.Redis
	}
	if len(cv19.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv19.Notify.PostgreSQL
	}
	if len(cv19.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv19.Notify.Kafka
	}
	if len(cv19.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv19.Notify.NATS
	}
	if len(cv19.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv19.Notify.Webhook
	}
	if len(cv19.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]mySQLNotify)
		srvConfig.Notify.MySQL["1"] = mySQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.MySQL = cv19.Notify.MySQL
	}
	if len(cv19.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]mqttNotify)
		srvConfig.Notify.MQTT["1"] = mqttNotify{}
	} else {
		srvConfig.Notify.MQTT = cv19.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv19.Browser

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv19.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv19.Version, srvConfig.Version)
	return nil
}

func migrateV20ToV21() error {
	configFile := getConfigFile()

	cv20 := &serverConfigV20{}
	_, err := quick.Load(configFile, cv20)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘20’. %v", err)
	}
	if cv20.Version != "20" {
		return nil
	}

	// Copy over fields from V20 into V21 config struct
	srvConfig := &serverConfigV21{
		Notify: &notifier{},
	}
	srvConfig.Version = "21"
	srvConfig.Credential = cv20.Credential
	srvConfig.Region = cv20.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// check and set notifiers config
	if len(cv20.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		// New deliveryMode parameter is added for AMQP,
		// default value is already 0, so nothing to
		// explicitly migrate here.
		srvConfig.Notify.AMQP = cv20.Notify.AMQP
	}
	if len(cv20.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.ElasticSearch = cv20.Notify.ElasticSearch
	}
	if len(cv20.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.Redis = cv20.Notify.Redis
	}
	if len(cv20.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv20.Notify.PostgreSQL
	}
	if len(cv20.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv20.Notify.Kafka
	}
	if len(cv20.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv20.Notify.NATS
	}
	if len(cv20.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv20.Notify.Webhook
	}
	if len(cv20.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]mySQLNotify)
		srvConfig.Notify.MySQL["1"] = mySQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.MySQL = cv20.Notify.MySQL
	}
	if len(cv20.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]mqttNotify)
		srvConfig.Notify.MQTT["1"] = mqttNotify{}
	} else {
		srvConfig.Notify.MQTT = cv20.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv20.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv20.Domain

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv20.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv20.Version, srvConfig.Version)
	return nil
}

func migrateV21ToV22() error {
	configFile := getConfigFile()

	cv21 := &serverConfigV21{}
	_, err := quick.Load(configFile, cv21)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("Unable to load config version ‘21’. %v", err)
	}
	if cv21.Version != "21" {
		return nil
	}

	// Copy over fields from V21 into V22 config struct
	srvConfig := &serverConfigV22{
		Notify: notifier{},
	}
	srvConfig.Version = serverConfigVersion
	srvConfig.Credential = cv21.Credential
	srvConfig.Region = cv21.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	// check and set notifiers config
	if len(cv21.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]amqpNotify)
		srvConfig.Notify.AMQP["1"] = amqpNotify{}
	} else {
		// New deliveryMode parameter is added for AMQP,
		// default value is already 0, so nothing to
		// explicitly migrate here.
		srvConfig.Notify.AMQP = cv21.Notify.AMQP
	}
	if len(cv21.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvConfig.Notify.ElasticSearch["1"] = elasticSearchNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.ElasticSearch = cv21.Notify.ElasticSearch
	}
	if len(cv21.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]redisNotify)
		srvConfig.Notify.Redis["1"] = redisNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.Redis = cv21.Notify.Redis
	}
	if len(cv21.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvConfig.Notify.PostgreSQL["1"] = postgreSQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv21.Notify.PostgreSQL
	}
	if len(cv21.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]kafkaNotify)
		srvConfig.Notify.Kafka["1"] = kafkaNotify{}
	} else {
		srvConfig.Notify.Kafka = cv21.Notify.Kafka
	}
	if len(cv21.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]natsNotify)
		srvConfig.Notify.NATS["1"] = natsNotify{}
	} else {
		srvConfig.Notify.NATS = cv21.Notify.NATS
	}
	if len(cv21.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]webhookNotify)
		srvConfig.Notify.Webhook["1"] = webhookNotify{}
	} else {
		srvConfig.Notify.Webhook = cv21.Notify.Webhook
	}
	if len(cv21.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]mySQLNotify)
		srvConfig.Notify.MySQL["1"] = mySQLNotify{
			Format: formatNamespace,
		}
	} else {
		srvConfig.Notify.MySQL = cv21.Notify.MySQL
	}
	if len(cv21.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]mqttNotify)
		srvConfig.Notify.MQTT["1"] = mqttNotify{}
	} else {
		srvConfig.Notify.MQTT = cv21.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv21.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv21.Domain

	if err = quick.Save(configFile, srvConfig); err != nil {
		return fmt.Errorf("Failed to migrate config from ‘%s’ to ‘%s’. %v", cv21.Version, srvConfig.Version, err)
	}

	log.Printf(configMigrateMSGTemplate, configFile, cv21.Version, srvConfig.Version)
	return nil
}
