/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
	"github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/iam/validator"
	xnet "github.com/minio/minio/pkg/net"
)

var errIncorrectConfigVersion = errors.New("incorrect config version")

// DO NOT EDIT following message template, please open a github issue to discuss instead.
var configMigrateMSGTemplate = "Configuration file migrated from version '%s' to '%s' successfully."

// Migrates all config versions from "2" to latest.
func migrateConfig(configBytes []byte) ([]byte, error) {
	var err error
	switch configGetVersion(configBytes) {
	case "2":
		configBytes, err = migrateV2ToV3(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "3":
		configBytes, err = migrateV3ToV4(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "4":
		configBytes, err = migrateV4ToV5(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "5":
		configBytes, err = migrateV5ToV6(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "6":
		configBytes, err = migrateV6ToV7(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "7":
		configBytes, err = migrateV7ToV8(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "8":
		configBytes, err = migrateV8ToV9(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "9":
		configBytes, err = migrateV9ToV10(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "10":
		configBytes, err = migrateV10ToV11(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "11":
		configBytes, err = migrateV11ToV12(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "12":
		configBytes, err = migrateV12ToV13(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "13":
		configBytes, err = migrateV13ToV14(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "14":
		configBytes, err = migrateV14ToV15(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "15":
		configBytes, err = migrateV15ToV16(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "16":
		configBytes, err = migrateV16ToV17(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "17":
		configBytes, err = migrateV17ToV18(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "18":
		configBytes, err = migrateV18ToV19(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "19":
		configBytes, err = migrateV19ToV20(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "20":
		configBytes, err = migrateV20ToV21(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "21":
		configBytes, err = migrateV21ToV22(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "22":
		configBytes, err = migrateV22ToV23(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "23":
		configBytes, err = migrateV23ToV24(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "24":
		configBytes, err = migrateV24ToV25(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "25":
		configBytes, err = migrateV25ToV26(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "26":
		configBytes, err = migrateV26ToV27(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "27":
		configBytes, err = migrateV27ToV28(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "28":
		configBytes, err = migrateV28ToV29(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "29":
		configBytes, err = migrateV29ToV30(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "30":
		configBytes, err = migrateV30ToV31(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "31":
		configBytes, err = migrateV31ToV32(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case "32":
		configBytes, err = migrateV32ToV33(configBytes)
		if err != nil {
			return nil, err
		}
		fallthrough
	case serverConfigVersion:
		// No migration needed. this always points to current version.
		return configBytes, nil
	default:
		return nil, errIncorrectConfigVersion
	}
}

// Version '1' is not supported anymore and deprecated, safe to delete.
func purgeV1() error {
	configFile := filepath.Join(globalConfigDir.Get(), "fsUsers.json")

	if !isFile(configFile) {
		return nil
	}
	cv1 := &configV1{}
	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, cv1); err != nil {
		return err
	}
	if cv1.Version != "1" {
		return fmt.Errorf("unrecognized config version ‘%s’", cv1.Version)
	}
	os.RemoveAll(configFile)
	logger.Info("Removed unsupported config version ‘1’.")
	return nil
}

// Version '2' to '3' config migration adds new fields and re-orders
// previous fields. Simplifies config for future additions.
func migrateV2ToV3(configBytes []byte) ([]byte, error) {
	cv2 := &configV2{}
	if err := json.Unmarshal(configBytes, cv2); err != nil {
		return nil, err
	}
	if cv2.Version != "2" {
		return nil, errIncorrectConfigVersion
	}

	cred, err := auth.CreateCredentials(cv2.Credentials.AccessKey, cv2.Credentials.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("Invalid credential in V2 configuration file. %v", err)
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

	logger.Info(configMigrateMSGTemplate, cv2.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '3' to '4' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV3ToV4(configBytes []byte) ([]byte, error) {
	cv3 := &configV3{}
	if err := json.Unmarshal(configBytes, cv3); err != nil {
		return nil, err
	}
	if cv3.Version != "3" {
		return nil, errIncorrectConfigVersion
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

	logger.Info(configMigrateMSGTemplate, cv3.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '4' to '5' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV4ToV5(configBytes []byte) ([]byte, error) {
	cv4 := &configV4{}
	if err := json.Unmarshal(configBytes, cv4); err != nil {
		return nil, err
	}
	if cv4.Version != "4" {
		return nil, errIncorrectConfigVersion
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

	logger.Info(configMigrateMSGTemplate, cv4.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '5' to '6' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV5ToV6(configBytes []byte) ([]byte, error) {
	cv5 := &configV5{}
	if err := json.Unmarshal(configBytes, cv5); err != nil {
		return nil, err
	}
	if cv5.Version != "5" {
		return nil, errIncorrectConfigVersion
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

	var err error
	if cv5.Logger.AMQP.URL != "" {
		var url *xnet.URL
		if url, err = xnet.ParseURL(cv5.Logger.AMQP.URL); err != nil {
			return nil, err
		}
		srvConfig.Notify.AMQP = map[string]target.AMQPArgs{
			"1": {
				Enable:      cv5.Logger.AMQP.Enable,
				URL:         *url,
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
	}

	if cv5.Logger.ElasticSearch.URL != "" {
		var url *xnet.URL
		url, err = xnet.ParseURL(cv5.Logger.ElasticSearch.URL)
		if err != nil {
			return nil, err
		}
		srvConfig.Notify.ElasticSearch = map[string]target.ElasticsearchArgs{
			"1": {
				Enable: cv5.Logger.ElasticSearch.Enable,
				URL:    *url,
				Index:  cv5.Logger.ElasticSearch.Index,
			},
		}
	}

	if cv5.Logger.Redis.Addr != "" {
		var addr *xnet.Host
		if addr, err = xnet.ParseHost(cv5.Logger.Redis.Addr); err != nil {
			return nil, err
		}
		srvConfig.Notify.Redis = map[string]target.RedisArgs{
			"1": {
				Enable:   cv5.Logger.Redis.Enable,
				Addr:     *addr,
				Password: cv5.Logger.Redis.Password,
				Key:      cv5.Logger.Redis.Key,
			},
		}
	}

	logger.Info(configMigrateMSGTemplate, cv5.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '6' to '7' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV6ToV7(configBytes []byte) ([]byte, error) {
	cv6 := &configV6{}
	if err := json.Unmarshal(configBytes, cv6); err != nil {
		return nil, err
	}
	if cv6.Version != "6" {
		return nil, errIncorrectConfigVersion
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
	srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
	srvConfig.Notify.ElasticSearch = make(map[string]target.ElasticsearchArgs)
	srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
	if len(cv6.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv6.Notify.AMQP
	}
	if len(cv6.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.ElasticSearch = cv6.Notify.ElasticSearch
	}
	if len(cv6.Notify.Redis) == 0 {
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv6.Notify.Redis
	}

	logger.Info(configMigrateMSGTemplate, cv6.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '7' to '8' migrates config, removes previous fields related
// to backend types and server address. This change further simplifies
// the config for future additions.
func migrateV7ToV8(configBytes []byte) ([]byte, error) {
	cv7 := &serverConfigV7{}
	if err := json.Unmarshal(configBytes, cv7); err != nil {
		return nil, err
	}
	if cv7.Version != "7" {
		return nil, errIncorrectConfigVersion
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
	srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
	srvConfig.Notify.NATS = make(map[string]natsNotifyV1)
	srvConfig.Notify.ElasticSearch = make(map[string]target.ElasticsearchArgs)
	srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
	srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
	if len(cv7.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv7.Notify.AMQP
	}
	if len(cv7.Notify.NATS) == 0 {
		srvConfig.Notify.NATS["1"] = natsNotifyV1{}
	} else {
		srvConfig.Notify.NATS = cv7.Notify.NATS
	}
	if len(cv7.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.ElasticSearch = cv7.Notify.ElasticSearch
	}
	if len(cv7.Notify.Redis) == 0 {
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv7.Notify.Redis
	}

	logger.Info(configMigrateMSGTemplate, cv7.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '8' to '9' migration. Adds postgresql notifier
// configuration, but it's otherwise the same as V8.
func migrateV8ToV9(configBytes []byte) ([]byte, error) {
	cv8 := &serverConfigV8{}
	if err := json.Unmarshal(configBytes, cv8); err != nil {
		return nil, err
	}
	if cv8.Version != "8" {
		return nil, errIncorrectConfigVersion
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
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
		srvConfig.Notify.ElasticSearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.ElasticSearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.ElasticSearch = cv8.Notify.ElasticSearch
	}
	if len(cv8.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv8.Notify.Redis
	}
	if len(cv8.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		srvConfig.Notify.PostgreSQL = cv8.Notify.PostgreSQL
	}

	logger.Info(configMigrateMSGTemplate, cv8.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '9' to '10' migration. Remove syslog config
// but it's otherwise the same as V9.
func migrateV9ToV10(configBytes []byte) ([]byte, error) {
	cv9 := &serverConfigV9{}
	if err := json.Unmarshal(configBytes, cv9); err != nil {
		return nil, err
	}
	if cv9.Version != "9" {
		return nil, errIncorrectConfigVersion
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
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
		srvConfig.Notify.ElasticSearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.ElasticSearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.ElasticSearch = cv9.Notify.ElasticSearch
	}
	if len(cv9.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv9.Notify.Redis
	}
	if len(cv9.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		srvConfig.Notify.PostgreSQL = cv9.Notify.PostgreSQL
	}

	logger.Info(configMigrateMSGTemplate, cv9.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '10' to '11' migration. Add support for Kafka
// notifications.
func migrateV10ToV11(configBytes []byte) ([]byte, error) {
	cv10 := &serverConfigV10{}
	if err := json.Unmarshal(configBytes, cv10); err != nil {
		return nil, err
	}
	if cv10.Version != "10" {
		return nil, errIncorrectConfigVersion
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
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
		srvConfig.Notify.ElasticSearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.ElasticSearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.ElasticSearch = cv10.Notify.ElasticSearch
	}
	if len(cv10.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv10.Notify.Redis
	}
	if len(cv10.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		srvConfig.Notify.PostgreSQL = cv10.Notify.PostgreSQL
	}
	// V10 will not have a Kafka config. So we initialize one here.
	srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
	srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}

	logger.Info(configMigrateMSGTemplate, cv10.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '11' to '12' migration. Add support for NATS streaming
// notifications.
func migrateV11ToV12(configBytes []byte) ([]byte, error) {
	cv11 := &serverConfigV11{}
	if err := json.Unmarshal(configBytes, cv11); err != nil {
		return nil, err
	}
	if cv11.Version != "11" {
		return nil, errIncorrectConfigVersion
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv11.Notify.AMQP
	}
	if len(cv11.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.ElasticSearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.ElasticSearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.ElasticSearch = cv11.Notify.ElasticSearch
	}
	if len(cv11.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv11.Notify.Redis
	}
	if len(cv11.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		srvConfig.Notify.PostgreSQL = cv11.Notify.PostgreSQL
	}
	if len(cv11.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv11.Notify.Kafka
	}

	// V12 will have an updated config of nats. So we create a new one or we
	// update the old one if found.
	if len(cv11.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		for k, v := range cv11.Notify.NATS {
			if v.Address == "" {
				continue
			}

			addr, err := xnet.ParseHost(v.Address)
			if err != nil {
				return nil, err
			}
			n := target.NATSArgs{}
			n.Enable = v.Enable
			n.Address = *addr
			n.Subject = v.Subject
			n.Username = v.Username
			n.Password = v.Password
			n.Token = v.Token
			n.Secure = v.Secure
			n.PingInterval = v.PingInterval
			srvConfig.Notify.NATS[k] = n
		}
	}

	logger.Info(configMigrateMSGTemplate, cv11.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '12' to '13' migration. Add support for custom webhook endpoint.
func migrateV12ToV13(configBytes []byte) ([]byte, error) {
	cv12 := &serverConfigV12{}
	if err := json.Unmarshal(configBytes, cv12); err != nil {
		return nil, err
	}
	if cv12.Version != "12" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V12 into V13 config struct
	srvConfig := &serverConfigV13{
		Logger: &loggerV7{},
		Notify: &notifierV3{},
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv12.Notify.AMQP
	}
	if len(cv12.Notify.ElasticSearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.Elasticsearch = cv12.Notify.ElasticSearch
	}
	if len(cv12.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv12.Notify.Redis
	}
	if len(cv12.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		srvConfig.Notify.PostgreSQL = cv12.Notify.PostgreSQL
	}
	if len(cv12.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv12.Notify.Kafka
	}
	if len(cv12.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv12.Notify.NATS
	}

	// V12 will not have a webhook config. So we initialize one here.
	srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
	srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}

	logger.Info(configMigrateMSGTemplate, cv12.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '13' to '14' migration. Add support for browser param.
func migrateV13ToV14(configBytes []byte) ([]byte, error) {
	cv13 := &serverConfigV13{}
	if err := json.Unmarshal(configBytes, cv13); err != nil {
		return nil, err
	}
	if cv13.Version != "13" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V13 into V14 config struct
	srvConfig := &serverConfigV14{
		Logger: &loggerV7{},
		Notify: &notifierV3{},
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv13.Notify.AMQP
	}
	if len(cv13.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.Elasticsearch = cv13.Notify.Elasticsearch
	}
	if len(cv13.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv13.Notify.Redis
	}
	if len(cv13.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		srvConfig.Notify.PostgreSQL = cv13.Notify.PostgreSQL
	}
	if len(cv13.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv13.Notify.Kafka
	}
	if len(cv13.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv13.Notify.NATS
	}
	if len(cv13.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv13.Notify.Webhook
	}

	// Set the new browser parameter to true by default
	srvConfig.Browser = true

	logger.Info(configMigrateMSGTemplate, cv13.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '14' to '15' migration. Add support for MySQL notifications.
func migrateV14ToV15(configBytes []byte) ([]byte, error) {
	cv14 := &serverConfigV14{}
	if err := json.Unmarshal(configBytes, cv14); err != nil {
		return nil, err
	}
	if cv14.Version != "14" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V14 into V15 config struct
	srvConfig := &serverConfigV15{
		Logger: &loggerV7{},
		Notify: &notifierV3{},
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv14.Notify.AMQP
	}
	if len(cv14.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.Elasticsearch = cv14.Notify.Elasticsearch
	}
	if len(cv14.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv14.Notify.Redis
	}
	if len(cv14.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		srvConfig.Notify.PostgreSQL = cv14.Notify.PostgreSQL
	}
	if len(cv14.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv14.Notify.Kafka
	}
	if len(cv14.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv14.Notify.NATS
	}
	if len(cv14.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv14.Notify.Webhook
	}

	// V14 will not have mysql support, so we add that here.
	srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
	srvConfig.Notify.MySQL["1"] = target.MySQLArgs{}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv14.Browser

	logger.Info(configMigrateMSGTemplate, cv14.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '15' to '16' migration. Remove log level in loggers
// and rename 'fileName' filed in File logger to 'filename'
func migrateV15ToV16(configBytes []byte) ([]byte, error) {
	cv15 := &serverConfigV15{}
	if err := json.Unmarshal(configBytes, cv15); err != nil {
		return nil, err
	}
	if cv15.Version != "15" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V15 into V16 config struct
	srvConfig := &serverConfigV16{
		Logger: &loggers{},
		Notify: &notifierV3{},
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv15.Notify.AMQP
	}
	if len(cv15.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{}
	} else {
		srvConfig.Notify.Elasticsearch = cv15.Notify.Elasticsearch
	}
	if len(cv15.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		srvConfig.Notify.Redis = cv15.Notify.Redis
	}
	if len(cv15.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		srvConfig.Notify.PostgreSQL = cv15.Notify.PostgreSQL
	}
	if len(cv15.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv15.Notify.Kafka
	}
	if len(cv15.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv15.Notify.NATS
	}
	if len(cv15.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv15.Notify.Webhook
	}
	if len(cv15.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{}
	} else {
		srvConfig.Notify.MySQL = cv15.Notify.MySQL
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv15.Browser

	logger.Info(configMigrateMSGTemplate, cv15.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '16' to '17' migration. Adds "format" configuration
// parameter for database targets.
func migrateV16ToV17(configBytes []byte) ([]byte, error) {
	cv16 := &serverConfigV16{}
	if err := json.Unmarshal(configBytes, cv16); err != nil {
		return nil, err
	}
	if cv16.Version != "16" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V16 into V17 config struct
	srvConfig := &serverConfigV17{
		Logger: &loggers{},
		Notify: &notifierV3{},
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv16.Notify.AMQP
	}
	if len(cv16.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{}
	} else {
		// IMPORTANT NOTE: Future migrations should remove
		// this as existing configuration will already contain
		// a value for the "format" parameter.
		srvConfig.Notify.Elasticsearch = cv16.Notify.Elasticsearch
		for k, v := range srvConfig.Notify.Elasticsearch {
			v.Format = event.NamespaceFormat
			srvConfig.Notify.Elasticsearch[k] = v
		}
	}
	if len(cv16.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{}
	} else {
		// IMPORTANT NOTE: Future migrations should remove
		// this as existing configuration will already contain
		// a value for the "format" parameter.
		srvConfig.Notify.Redis = cv16.Notify.Redis
		for k, v := range srvConfig.Notify.Redis {
			v.Format = event.NamespaceFormat
			srvConfig.Notify.Redis[k] = v
		}
	}
	if len(cv16.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	} else {
		// IMPORTANT NOTE: Future migrations should remove
		// this as existing configuration will already contain
		// a value for the "format" parameter.
		srvConfig.Notify.PostgreSQL = cv16.Notify.PostgreSQL
		for k, v := range srvConfig.Notify.PostgreSQL {
			v.Format = event.NamespaceFormat
			srvConfig.Notify.PostgreSQL[k] = v
		}
	}
	if len(cv16.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv16.Notify.Kafka
	}
	if len(cv16.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv16.Notify.NATS
	}
	if len(cv16.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv16.Notify.Webhook
	}
	if len(cv16.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{}
	} else {
		// IMPORTANT NOTE: Future migrations should remove
		// this as existing configuration will already contain
		// a value for the "format" parameter.
		srvConfig.Notify.MySQL = cv16.Notify.MySQL
		for k, v := range srvConfig.Notify.MySQL {
			v.Format = event.NamespaceFormat
			srvConfig.Notify.MySQL[k] = v
		}
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv16.Browser

	logger.Info(configMigrateMSGTemplate, cv16.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

// Version '17' to '18' migration. Adds "deliveryMode" configuration
// parameter for AMQP notification target
func migrateV17ToV18(configBytes []byte) ([]byte, error) {
	cv17 := &serverConfigV17{}
	if err := json.Unmarshal(configBytes, cv17); err != nil {
		return nil, err
	}
	if cv17.Version != "17" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V17 into V18 config struct
	srvConfig := &serverConfigV17{
		Logger: &loggers{},
		Notify: &notifierV3{},
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		// New deliveryMode parameter is added for AMQP,
		// default value is already 0, so nothing to
		// explicitly migrate here.
		srvConfig.Notify.AMQP = cv17.Notify.AMQP
	}
	if len(cv17.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv17.Notify.Elasticsearch
	}
	if len(cv17.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv17.Notify.Redis
	}
	if len(cv17.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv17.Notify.PostgreSQL
	}
	if len(cv17.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv17.Notify.Kafka
	}
	if len(cv17.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv17.Notify.NATS
	}
	if len(cv17.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv17.Notify.Webhook
	}
	if len(cv17.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv17.Notify.MySQL
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv17.Browser

	logger.Info(configMigrateMSGTemplate, cv17.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV18ToV19(configBytes []byte) ([]byte, error) {
	cv18 := &serverConfigV18{}
	if err := json.Unmarshal(configBytes, cv18); err != nil {
		return nil, err
	}
	if cv18.Version != "18" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V18 into V19 config struct
	srvConfig := &serverConfigV18{
		Logger: &loggers{},
		Notify: &notifierV3{},
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
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		// New deliveryMode parameter is added for AMQP,
		// default value is already 0, so nothing to
		// explicitly migrate here.
		srvConfig.Notify.AMQP = cv18.Notify.AMQP
	}
	if len(cv18.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv18.Notify.Elasticsearch
	}
	if len(cv18.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv18.Notify.Redis
	}
	if len(cv18.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv18.Notify.PostgreSQL
	}
	if len(cv18.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv18.Notify.Kafka
	}
	if len(cv18.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv18.Notify.NATS
	}
	if len(cv18.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv18.Notify.Webhook
	}
	if len(cv18.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv18.Notify.MySQL
	}

	// V18 will not have mqtt support, so we add that here.
	srvConfig.Notify.MQTT = make(map[string]target.MQTTArgs)
	srvConfig.Notify.MQTT["1"] = target.MQTTArgs{}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv18.Browser

	logger.Info(configMigrateMSGTemplate, cv18.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV19ToV20(configBytes []byte) ([]byte, error) {
	cv19 := &serverConfigV19{}
	if err := json.Unmarshal(configBytes, cv19); err != nil {
		return nil, err
	}
	if cv19.Version != "19" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V19 into V20 config struct
	srvConfig := &serverConfigV20{
		Logger: &loggers{},
		Notify: &notifierV3{},
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

	if len(cv19.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv19.Notify.AMQP
	}
	if len(cv19.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv19.Notify.Elasticsearch
	}
	if len(cv19.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv19.Notify.Redis
	}
	if len(cv19.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv19.Notify.PostgreSQL
	}
	if len(cv19.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv19.Notify.Kafka
	}
	if len(cv19.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv19.Notify.NATS
	}
	if len(cv19.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv19.Notify.Webhook
	}
	if len(cv19.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv19.Notify.MySQL
	}

	if len(cv19.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]target.MQTTArgs)
		srvConfig.Notify.MQTT["1"] = target.MQTTArgs{}
	} else {
		srvConfig.Notify.MQTT = cv19.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv19.Browser

	logger.Info(configMigrateMSGTemplate, cv19.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV20ToV21(configBytes []byte) ([]byte, error) {
	cv20 := &serverConfigV20{}
	if err := json.Unmarshal(configBytes, cv20); err != nil {
		return nil, err
	}
	if cv20.Version != "20" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V20 into V21 config struct
	srvConfig := &serverConfigV21{
		Notify: &notifierV3{},
	}
	srvConfig.Version = "21"
	srvConfig.Credential = cv20.Credential
	srvConfig.Region = cv20.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	if len(cv20.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv20.Notify.AMQP
	}
	if len(cv20.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv20.Notify.Elasticsearch
	}
	if len(cv20.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv20.Notify.Redis
	}
	if len(cv20.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv20.Notify.PostgreSQL
	}
	if len(cv20.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv20.Notify.Kafka
	}
	if len(cv20.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv20.Notify.NATS
	}
	if len(cv20.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv20.Notify.Webhook
	}
	if len(cv20.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv20.Notify.MySQL
	}

	if len(cv20.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]target.MQTTArgs)
		srvConfig.Notify.MQTT["1"] = target.MQTTArgs{}
	} else {
		srvConfig.Notify.MQTT = cv20.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv20.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv20.Domain

	logger.Info(configMigrateMSGTemplate, cv20.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV21ToV22(configBytes []byte) ([]byte, error) {
	cv21 := &serverConfigV21{}
	if err := json.Unmarshal(configBytes, cv21); err != nil {
		return nil, err
	}
	if cv21.Version != "21" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V21 into V22 config struct
	srvConfig := &serverConfigV22{
		Notify: notifierV3{},
	}
	srvConfig.Version = "22"
	srvConfig.Credential = cv21.Credential
	srvConfig.Region = cv21.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	if len(cv21.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv21.Notify.AMQP
	}
	if len(cv21.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv21.Notify.Elasticsearch
	}
	if len(cv21.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv21.Notify.Redis
	}
	if len(cv21.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv21.Notify.PostgreSQL
	}
	if len(cv21.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv21.Notify.Kafka
	}
	if len(cv21.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv21.Notify.NATS
	}
	if len(cv21.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv21.Notify.Webhook
	}
	if len(cv21.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv21.Notify.MySQL
	}

	if len(cv21.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]target.MQTTArgs)
		srvConfig.Notify.MQTT["1"] = target.MQTTArgs{}
	} else {
		srvConfig.Notify.MQTT = cv21.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv21.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv21.Domain

	logger.Info(configMigrateMSGTemplate, cv21.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV22ToV23(configBytes []byte) ([]byte, error) {
	cv22 := &serverConfigV22{}
	if err := json.Unmarshal(configBytes, cv22); err != nil {
		return nil, err
	}
	if cv22.Version != "22" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V22 into V23 config struct
	srvConfig := &serverConfigV23{
		Notify: notifierV3{},
	}
	srvConfig.Version = "23"
	srvConfig.Credential = cv22.Credential
	srvConfig.Region = cv22.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	if len(cv22.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv22.Notify.AMQP
	}
	if len(cv22.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv22.Notify.Elasticsearch
	}
	if len(cv22.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv22.Notify.Redis
	}
	if len(cv22.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv22.Notify.PostgreSQL
	}
	if len(cv22.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv22.Notify.Kafka
	}
	if len(cv22.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv22.Notify.NATS
	}
	if len(cv22.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv22.Notify.Webhook
	}
	if len(cv22.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv22.Notify.MySQL
	}

	if len(cv22.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]target.MQTTArgs)
		srvConfig.Notify.MQTT["1"] = target.MQTTArgs{}
	} else {
		srvConfig.Notify.MQTT = cv22.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv22.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv22.Domain

	// Load storage class config from existing storage class config in the file
	srvConfig.StorageClass.RRS = cv22.StorageClass.RRS
	srvConfig.StorageClass.Standard = cv22.StorageClass.Standard

	// Init cache config.For future migration, Cache config needs to be copied over from previous version.
	srvConfig.Cache.Drives = []string{}
	srvConfig.Cache.Exclude = []string{}
	srvConfig.Cache.Expiry = globalCacheExpiry

	logger.Info(configMigrateMSGTemplate, cv22.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV23ToV24(configBytes []byte) ([]byte, error) {
	cv23 := &serverConfigV23{}
	if err := json.Unmarshal(configBytes, cv23); err != nil {
		return nil, err
	}
	if cv23.Version != "23" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V23 into V24 config struct
	srvConfig := &serverConfigV24{
		Notify: notifierV3{},
	}
	srvConfig.Version = "24"
	srvConfig.Credential = cv23.Credential
	srvConfig.Region = cv23.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	if len(cv23.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv23.Notify.AMQP
	}
	if len(cv23.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv23.Notify.Elasticsearch
	}
	if len(cv23.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv23.Notify.Redis
	}
	if len(cv23.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv23.Notify.PostgreSQL
	}
	if len(cv23.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv23.Notify.Kafka
	}
	if len(cv23.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv23.Notify.NATS
	}
	if len(cv23.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv23.Notify.Webhook
	}
	if len(cv23.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv23.Notify.MySQL
	}

	if len(cv23.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]target.MQTTArgs)
		srvConfig.Notify.MQTT["1"] = target.MQTTArgs{}
	} else {
		srvConfig.Notify.MQTT = cv23.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv23.Browser

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv23.Domain

	// Load storage class config from existing storage class config in the file.
	srvConfig.StorageClass.RRS = cv23.StorageClass.RRS
	srvConfig.StorageClass.Standard = cv23.StorageClass.Standard

	// Load cache config from existing cache config in the file.
	srvConfig.Cache.Drives = cv23.Cache.Drives
	srvConfig.Cache.Exclude = cv23.Cache.Exclude
	srvConfig.Cache.Expiry = cv23.Cache.Expiry

	logger.Info(configMigrateMSGTemplate, cv23.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV24ToV25(configBytes []byte) ([]byte, error) {
	cv24 := &serverConfigV24{}
	if err := json.Unmarshal(configBytes, cv24); err != nil {
		return nil, err
	}
	if cv24.Version != "24" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V24 into V25 config struct
	srvConfig := &serverConfigV25{
		Notify: notifierV3{},
	}
	srvConfig.Version = "25"
	srvConfig.Credential = cv24.Credential
	srvConfig.Region = cv24.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	if len(cv24.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv24.Notify.AMQP
	}
	if len(cv24.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv24.Notify.Elasticsearch
	}
	if len(cv24.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv24.Notify.Redis
	}
	if len(cv24.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv24.Notify.PostgreSQL
	}
	if len(cv24.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv24.Notify.Kafka
	}
	if len(cv24.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv24.Notify.NATS
	}
	if len(cv24.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv24.Notify.Webhook
	}
	if len(cv24.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv24.Notify.MySQL
	}

	if len(cv24.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]target.MQTTArgs)
		srvConfig.Notify.MQTT["1"] = target.MQTTArgs{}
	} else {
		srvConfig.Notify.MQTT = cv24.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv24.Browser

	// New field should be turned-off by default.
	srvConfig.Worm = false // cv25.Worm should be used here
	// for the next migration from v25 to v26 to persist
	// local config value.

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv24.Domain

	// Load storage class config from existing storage class config in the file.
	srvConfig.StorageClass.RRS = cv24.StorageClass.RRS
	srvConfig.StorageClass.Standard = cv24.StorageClass.Standard

	// Load cache config from existing cache config in the file.
	srvConfig.Cache.Drives = cv24.Cache.Drives
	srvConfig.Cache.Exclude = cv24.Cache.Exclude
	srvConfig.Cache.Expiry = cv24.Cache.Expiry

	logger.Info(configMigrateMSGTemplate, cv24.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV25ToV26(configBytes []byte) ([]byte, error) {
	cv25 := &serverConfigV25{}
	if err := json.Unmarshal(configBytes, cv25); err != nil {
		return nil, err
	}
	if cv25.Version != "25" {
		return nil, errIncorrectConfigVersion
	}

	// Copy over fields from V25 into V26 config struct
	srvConfig := &serverConfigV26{
		Notify: notifierV3{},
	}
	srvConfig.Version = "26"
	srvConfig.Credential = cv25.Credential
	srvConfig.Region = cv25.Region
	if srvConfig.Region == "" {
		// Region needs to be set for AWS Signature Version 4.
		srvConfig.Region = globalMinioDefaultRegion
	}

	if len(cv25.Notify.AMQP) == 0 {
		srvConfig.Notify.AMQP = make(map[string]target.AMQPArgs)
		srvConfig.Notify.AMQP["1"] = target.AMQPArgs{}
	} else {
		srvConfig.Notify.AMQP = cv25.Notify.AMQP
	}
	if len(cv25.Notify.Elasticsearch) == 0 {
		srvConfig.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
		srvConfig.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Elasticsearch = cv25.Notify.Elasticsearch
	}
	if len(cv25.Notify.Redis) == 0 {
		srvConfig.Notify.Redis = make(map[string]target.RedisArgs)
		srvConfig.Notify.Redis["1"] = target.RedisArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.Redis = cv25.Notify.Redis
	}
	if len(cv25.Notify.PostgreSQL) == 0 {
		srvConfig.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
		srvConfig.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.PostgreSQL = cv25.Notify.PostgreSQL
	}
	if len(cv25.Notify.Kafka) == 0 {
		srvConfig.Notify.Kafka = make(map[string]target.KafkaArgs)
		srvConfig.Notify.Kafka["1"] = target.KafkaArgs{}
	} else {
		srvConfig.Notify.Kafka = cv25.Notify.Kafka
	}
	if len(cv25.Notify.NATS) == 0 {
		srvConfig.Notify.NATS = make(map[string]target.NATSArgs)
		srvConfig.Notify.NATS["1"] = target.NATSArgs{}
	} else {
		srvConfig.Notify.NATS = cv25.Notify.NATS
	}
	if len(cv25.Notify.Webhook) == 0 {
		srvConfig.Notify.Webhook = make(map[string]target.WebhookArgs)
		srvConfig.Notify.Webhook["1"] = target.WebhookArgs{}
	} else {
		srvConfig.Notify.Webhook = cv25.Notify.Webhook
	}
	if len(cv25.Notify.MySQL) == 0 {
		srvConfig.Notify.MySQL = make(map[string]target.MySQLArgs)
		srvConfig.Notify.MySQL["1"] = target.MySQLArgs{
			Format: event.NamespaceFormat,
		}
	} else {
		srvConfig.Notify.MySQL = cv25.Notify.MySQL
	}

	if len(cv25.Notify.MQTT) == 0 {
		srvConfig.Notify.MQTT = make(map[string]target.MQTTArgs)
		srvConfig.Notify.MQTT["1"] = target.MQTTArgs{}
	} else {
		srvConfig.Notify.MQTT = cv25.Notify.MQTT
	}

	// Load browser config from existing config in the file.
	srvConfig.Browser = cv25.Browser

	// Load worm config from existing config in the file.
	srvConfig.Worm = cv25.Worm

	// Load domain config from existing config in the file.
	srvConfig.Domain = cv25.Domain

	// Load storage class config from existing storage class config in the file.
	srvConfig.StorageClass.RRS = cv25.StorageClass.RRS
	srvConfig.StorageClass.Standard = cv25.StorageClass.Standard

	// Load cache config from existing cache config in the file.
	srvConfig.Cache.Drives = cv25.Cache.Drives
	srvConfig.Cache.Exclude = cv25.Cache.Exclude
	srvConfig.Cache.Expiry = cv25.Cache.Expiry

	// Add predefined value to new server config.
	srvConfig.Cache.MaxUse = globalCacheMaxUse

	logger.Info(configMigrateMSGTemplate, cv25.Version, srvConfig.Version)
	return json.Marshal(srvConfig)
}

func migrateV26ToV27(configBytes []byte) ([]byte, error) {
	// config V27 is backward compatible with V26, load the old
	// config file in serverConfigV27 struct and put some examples
	// in the new `logger` field
	srvConfig := &serverConfigV27{}
	if err := json.Unmarshal(configBytes, srvConfig); err != nil {
		return nil, err
	}
	if srvConfig.Version != "26" {
		return nil, errIncorrectConfigVersion
	}

	srvConfig.Version = "27"
	// Enable console logging by default to avoid breaking users
	// current deployments
	srvConfig.Logger.Console.Enabled = true
	srvConfig.Logger.HTTP = make(map[string]loggerHTTP)
	srvConfig.Logger.HTTP["1"] = loggerHTTP{}

	logger.Info(configMigrateMSGTemplate, "26", "27")
	return json.Marshal(srvConfig)
}

func migrateV27ToV28(configBytes []byte) ([]byte, error) {
	// config V28 is backward compatible with V27, load the old
	// config file in serverConfigV28 struct and initialize KMSConfig
	srvConfig := &serverConfigV28{}
	if err := json.Unmarshal(configBytes, srvConfig); err != nil {
		return nil, err
	}
	if srvConfig.Version != "27" {
		return nil, errIncorrectConfigVersion
	}

	srvConfig.Version = "28"
	srvConfig.KMS = crypto.KMSConfig{}

	logger.Info(configMigrateMSGTemplate, "27", "28")
	return json.Marshal(srvConfig)
}

func migrateV28ToV29(configBytes []byte) ([]byte, error) {
	cfg := &serverConfigV29{}
	if err := json.Unmarshal(configBytes, cfg); err != nil {
		return nil, err
	}
	if cfg.Version != "28" {
		return nil, errIncorrectConfigVersion
	}

	cfg.Version = "29"
	logger.Info(configMigrateMSGTemplate, "28", "29")
	return json.Marshal(cfg)
}

func migrateV29ToV30(configBytes []byte) ([]byte, error) {
	cfg := &serverConfigV30{}
	if err := json.Unmarshal(configBytes, cfg); err != nil {
		return nil, err
	}
	if cfg.Version != "29" {
		return nil, errIncorrectConfigVersion
	}

	cfg.Version = "30"
	// Init compression config.For future migration, Compression config needs to be copied over from previous version.
	cfg.Compression.Enabled = false
	cfg.Compression.Extensions = globalCompressExtensions
	cfg.Compression.MimeTypes = globalCompressMimeTypes

	logger.Info(configMigrateMSGTemplate, "29", "30")
	return json.Marshal(cfg)
}

func migrateV30ToV31(configBytes []byte) ([]byte, error) {
	cfg := &serverConfigV31{}
	if err := json.Unmarshal(configBytes, cfg); err != nil {
		return nil, err
	}
	if cfg.Version != "30" {
		return nil, errIncorrectConfigVersion
	}

	cfg.Version = "31"
	cfg.OpenID.JWKS = validator.JWKSArgs{
		URL: &xnet.URL{},
	}
	cfg.Policy.OPA = iampolicy.OpaArgs{
		URL:       &xnet.URL{},
		AuthToken: "",
	}

	logger.Info(configMigrateMSGTemplate, "30", "31")
	return json.Marshal(cfg)
}

func migrateV31ToV32(configBytes []byte) ([]byte, error) {
	cfg := &serverConfigV32{}
	if err := json.Unmarshal(configBytes, cfg); err != nil {
		return nil, err
	}
	if cfg.Version != "31" {
		return nil, errIncorrectConfigVersion
	}

	cfg.Version = "32"
	cfg.Notify.NSQ = make(map[string]target.NSQArgs)
	cfg.Notify.NSQ["1"] = target.NSQArgs{}

	logger.Info(configMigrateMSGTemplate, "31", "32")
	return json.Marshal(cfg)
}

func migrateV32ToV33(configBytes []byte) ([]byte, error) {
	cfg := &serverConfigV33{}
	if err := json.Unmarshal(configBytes, cfg); err != nil {
		return nil, err
	}
	if cfg.Version != "32" {
		return nil, errIncorrectConfigVersion
	}

	cfg.Version = "33"

	logger.Info(configMigrateMSGTemplate, "32", "33")
	return json.Marshal(cfg)
}
