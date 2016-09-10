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
	"reflect"
	"testing"
)

func TestServerConfig(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	if serverConfig.GetRegion() != "us-east-1" {
		t.Errorf("Expecting region `us-east-1` found %s", serverConfig.GetRegion())
	}

	// Set new region and verify.
	serverConfig.SetRegion("us-west-1")
	if serverConfig.GetRegion() != "us-west-1" {
		t.Errorf("Expecting region `us-west-1` found %s", serverConfig.GetRegion())
	}

	// Set new amqp notification id.
	serverConfig.SetAMQPNotifyByID("2", amqpNotify{})
	savedNotifyCfg1 := serverConfig.GetAMQPNotifyByID("2")
	if !reflect.DeepEqual(savedNotifyCfg1, amqpNotify{}) {
		t.Errorf("Expecting AMQP config %#v found %#v", amqpNotify{}, savedNotifyCfg1)
	}

	// Set new elastic search notification id.
	serverConfig.SetElasticSearchNotifyByID("2", elasticSearchNotify{})
	savedNotifyCfg2 := serverConfig.GetElasticSearchNotifyByID("2")
	if !reflect.DeepEqual(savedNotifyCfg2, elasticSearchNotify{}) {
		t.Errorf("Expecting Elasticsearch config %#v found %#v", elasticSearchNotify{}, savedNotifyCfg2)
	}

	// Set new redis notification id.
	serverConfig.SetRedisNotifyByID("2", redisNotify{})
	savedNotifyCfg3 := serverConfig.GetRedisNotifyByID("2")
	if !reflect.DeepEqual(savedNotifyCfg3, redisNotify{}) {
		t.Errorf("Expecting Redis config %#v found %#v", redisNotify{}, savedNotifyCfg3)
	}

	// Set new console logger.
	serverConfig.SetConsoleLogger(consoleLogger{
		Enable: true,
	})
	consoleCfg := serverConfig.GetConsoleLogger()
	if !reflect.DeepEqual(consoleCfg, consoleLogger{Enable: true}) {
		t.Errorf("Expecting console logger config %#v found %#v", consoleLogger{Enable: true}, consoleCfg)
	}

	// Set new file logger.
	serverConfig.SetFileLogger(fileLogger{
		Enable: true,
	})
	fileCfg := serverConfig.GetFileLogger()
	if !reflect.DeepEqual(fileCfg, fileLogger{Enable: true}) {
		t.Errorf("Expecting file logger config %#v found %#v", fileLogger{Enable: true}, consoleCfg)
	}

	// Set new syslog logger.
	serverConfig.SetSyslogLogger(syslogLogger{
		Enable: true,
	})
	sysLogCfg := serverConfig.GetSyslogLogger()
	if !reflect.DeepEqual(sysLogCfg, syslogLogger{Enable: true}) {
		t.Errorf("Expecting syslog logger config %#v found %#v", syslogLogger{Enable: true}, sysLogCfg)
	}

	// Match version.
	if serverConfig.GetVersion() != globalMinioConfigVersion {
		t.Errorf("Expecting version %s found %s", serverConfig.GetVersion(), globalMinioConfigVersion)
	}

	// Attempt to save.
	if err := serverConfig.Save(); err != nil {
		t.Fatalf("Unable to save updated config file %s", err)
	}

	// Do this only once here.
	setGlobalConfigPath(rootPath)

	// Initialize server config.
	if err := initConfig(); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}
