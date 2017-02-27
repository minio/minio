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
	"os"
	"reflect"
	"testing"
)

func TestServerConfig(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	if serverConfig.GetRegion() != globalMinioDefaultRegion {
		t.Errorf("Expecting region `us-east-1` found %s", serverConfig.GetRegion())
	}

	// Set new region and verify.
	serverConfig.SetRegion("us-west-1")
	if serverConfig.GetRegion() != "us-west-1" {
		t.Errorf("Expecting region `us-west-1` found %s", serverConfig.GetRegion())
	}

	// Set new amqp notification id.
	serverConfig.Notify.SetAMQPByID("2", amqpNotify{})
	savedNotifyCfg1 := serverConfig.Notify.GetAMQPByID("2")
	if !reflect.DeepEqual(savedNotifyCfg1, amqpNotify{}) {
		t.Errorf("Expecting AMQP config %#v found %#v", amqpNotify{}, savedNotifyCfg1)
	}

	// Set new elastic search notification id.
	serverConfig.Notify.SetElasticSearchByID("2", elasticSearchNotify{})
	savedNotifyCfg2 := serverConfig.Notify.GetElasticSearchByID("2")
	if !reflect.DeepEqual(savedNotifyCfg2, elasticSearchNotify{}) {
		t.Errorf("Expecting Elasticsearch config %#v found %#v", elasticSearchNotify{}, savedNotifyCfg2)
	}

	// Set new redis notification id.
	serverConfig.Notify.SetRedisByID("2", redisNotify{})
	savedNotifyCfg3 := serverConfig.Notify.GetRedisByID("2")
	if !reflect.DeepEqual(savedNotifyCfg3, redisNotify{}) {
		t.Errorf("Expecting Redis config %#v found %#v", redisNotify{}, savedNotifyCfg3)
	}

	// Set new kafka notification id.
	serverConfig.Notify.SetKafkaByID("2", kafkaNotify{})
	savedNotifyCfg4 := serverConfig.Notify.GetKafkaByID("2")
	if !reflect.DeepEqual(savedNotifyCfg4, kafkaNotify{}) {
		t.Errorf("Expecting Kafka config %#v found %#v", kafkaNotify{}, savedNotifyCfg4)
	}

	// Set new Webhook notification id.
	serverConfig.Notify.SetWebhookByID("2", webhookNotify{})
	savedNotifyCfg5 := serverConfig.Notify.GetWebhookByID("2")
	if !reflect.DeepEqual(savedNotifyCfg5, webhookNotify{}) {
		t.Errorf("Expecting Webhook config %#v found %#v", webhookNotify{}, savedNotifyCfg3)
	}

	// Set new console logger.
	serverConfig.Logger.SetConsole(consoleLogger{
		Enable: true,
	})
	consoleCfg := serverConfig.Logger.GetConsole()
	if !reflect.DeepEqual(consoleCfg, consoleLogger{Enable: true}) {
		t.Errorf("Expecting console logger config %#v found %#v", consoleLogger{Enable: true}, consoleCfg)
	}
	// Set new console logger.
	serverConfig.Logger.SetConsole(consoleLogger{
		Enable: false,
	})

	// Set new file logger.
	serverConfig.Logger.SetFile(fileLogger{
		Enable: true,
	})
	fileCfg := serverConfig.Logger.GetFile()
	if !reflect.DeepEqual(fileCfg, fileLogger{Enable: true}) {
		t.Errorf("Expecting file logger config %#v found %#v", fileLogger{Enable: true}, consoleCfg)
	}
	// Set new file logger.
	serverConfig.Logger.SetFile(fileLogger{
		Enable: false,
	})

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
	if err := loadConfig(envParams{}); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}

func TestServerConfigWithEnvs(t *testing.T) {

	os.Setenv("MINIO_BROWSER", "off")
	defer os.Unsetenv("MINIO_BROWSER")

	os.Setenv("MINIO_ACCESS_KEY", "minio")
	defer os.Unsetenv("MINIO_ACCESS_KEY")

	os.Setenv("MINIO_SECRET_KEY", "minio123")
	defer os.Unsetenv("MINIO_SECRET_KEY")

	defer func() {
		globalIsEnvBrowser = false
		globalIsEnvCreds = false
	}()

	// Get test root.
	rootPath, err := getTestRoot()
	if err != nil {
		t.Error(err)
	}

	// Do this only once here.
	setGlobalConfigPath(rootPath)

	// Init config
	initConfig()

	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	// Check if serverConfig has
	if serverConfig.GetBrowser() != "off" {
		t.Errorf("Expecting browser `off` found %s", serverConfig.GetBrowser())
	}

	// Check if serverConfig has
	cred := serverConfig.GetCredential()

	if cred.AccessKey != "minio" {
		t.Errorf("Expecting access key to be `minio` found %s", cred.AccessKey)
	}

	if cred.SecretKey != "minio123" {
		t.Errorf("Expecting access key to be `minio123` found %s", cred.SecretKey)
	}
}
