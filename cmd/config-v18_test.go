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
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
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
		t.Errorf("Expecting Webhook config %#v found %#v", webhookNotify{}, savedNotifyCfg5)
	}

	// Set new console logger.
	// Set new Webhook notification id.
	serverConfig.Notify.SetMySQLByID("2", mySQLNotify{})
	savedNotifyCfg6 := serverConfig.Notify.GetMySQLByID("2")
	if !reflect.DeepEqual(savedNotifyCfg6, mySQLNotify{}) {
		t.Errorf("Expecting Webhook config %#v found %#v", mySQLNotify{}, savedNotifyCfg6)
	}

	consoleLogger := NewConsoleLogger()
	serverConfig.Logger.SetConsole(consoleLogger)
	consoleCfg := serverConfig.Logger.GetConsole()
	if !reflect.DeepEqual(consoleCfg, consoleLogger) {
		t.Errorf("Expecting console logger config %#v found %#v", consoleLogger, consoleCfg)
	}
	// Set new console logger.
	consoleLogger.Enable = false
	serverConfig.Logger.SetConsole(consoleLogger)

	// Set new file logger.
	fileLogger := NewFileLogger("test-log-file")
	serverConfig.Logger.SetFile(fileLogger)
	fileCfg := serverConfig.Logger.GetFile()
	if !reflect.DeepEqual(fileCfg, fileLogger) {
		t.Errorf("Expecting file logger config %#v found %#v", fileLogger, fileCfg)
	}
	// Set new file logger.
	fileLogger.Enable = false
	serverConfig.Logger.SetFile(fileLogger)

	// Match version.
	if serverConfig.GetVersion() != v18 {
		t.Errorf("Expecting version %s found %s", serverConfig.GetVersion(), v18)
	}

	// Attempt to save.
	if err := serverConfig.Save(); err != nil {
		t.Fatalf("Unable to save updated config file %s", err)
	}

	// Do this only once here.
	setConfigDir(rootPath)

	// Initialize server config.
	if err := loadConfig(); err != nil {
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

	os.Setenv("MINIO_REGION", "us-west-1")
	defer os.Unsetenv("MINIO_REGION")

	defer resetGlobalIsEnvs()

	// Get test root.
	rootPath, err := getTestRoot()
	if err != nil {
		t.Error(err)
	}

	serverHandleEnvVars()

	// Do this only once here.
	setConfigDir(rootPath)

	// Init config
	initConfig()

	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	// Check if serverConfig has
	if serverConfig.GetBrowser() {
		t.Errorf("Expecting browser is set to false found %v", serverConfig.GetBrowser())
	}

	// Check if serverConfig has
	if serverConfig.GetRegion() != "us-west-1" {
		t.Errorf("Expecting region to be \"us-west-1\" found %v", serverConfig.GetRegion())
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

func TestCheckDupJSONKeys(t *testing.T) {
	testCases := []struct {
		json       string
		shouldPass bool
	}{
		{`{}`, true},
		{`{"version" : "13"}`, true},
		{`{"version" : "13", "version": "14"}`, false},
		{`{"version" : "13", "credential": {"accessKey": "12345"}}`, true},
		{`{"version" : "13", "credential": {"accessKey": "12345", "accessKey":"12345"}}`, false},
		{`{"version" : "13", "notify": {"amqp": {"1"}, "webhook":{"3"}}}`, true},
		{`{"version" : "13", "notify": {"amqp": {"1"}, "amqp":{"3"}}}`, false},
		{`{"version" : "13", "notify": {"amqp": {"1":{}, "2":{}}}}`, true},
		{`{"version" : "13", "notify": {"amqp": {"1":{}, "1":{}}}}`, false},
	}

	for i, testCase := range testCases {
		err := doCheckDupJSONKeys(gjson.Result{}, gjson.Parse(testCase.json))
		if testCase.shouldPass && err != nil {
			t.Errorf("Test %d, should pass but it failed with err = %v", i+1, err)
		}
		if !testCase.shouldPass && err == nil {
			t.Errorf("Test %d, should fail but it succeed.", i+1)
		}
	}

}

// Tests config validator..
func TestValidateConfig(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}

	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	configPath := filepath.Join(rootPath, minioConfigFile)

	v := v18

	testCases := []struct {
		configData string
		shouldPass bool
	}{
		// Test 1 - wrong json
		{`{`, false},

		// Test 2 - empty json
		{`{}`, false},

		// Test 3 - wrong config version
		{`{"version": "10"}`, false},

		// Test 4 - wrong browser parameter
		{`{"version": "` + v + `", "browser": "foo"}`, false},

		// Test 5 - missing credential
		{`{"version": "` + v + `", "browser": "on"}`, false},

		// Test 6 - missing secret key
		{`{"version": "` + v + `", "browser": "on", "credential" : {"accessKey":"minio", "secretKey":""}}`, false},

		// Test 7 - missing region should pass, defaults to 'us-east-1'.
		{`{"version": "` + v + `", "browser": "on", "credential" : {"accessKey":"minio", "secretKey":"minio123"}}`, true},

		// Test 8 - missing browser should pass, defaults to 'on'.
		{`{"version": "` + v + `", "region": "us-east-1", "credential" : {"accessKey":"minio", "secretKey":"minio123"}}`, true},

		// Test 9 - success
		{`{"version": "` + v + `", "browser": "on", "region":"us-east-1", "credential" : {"accessKey":"minio", "secretKey":"minio123"}}`, true},

		// Test 10 - duplicated json keys
		{`{"version": "` + v + `", "browser": "on", "browser": "on", "region":"us-east-1", "credential" : {"accessKey":"minio", "secretKey":"minio123"}}`, false},

		// Test 11 - empty filename field in File
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "logger": { "file": { "enable": true, "filename": "" } }}`, false},

		// Test 12 - Test AMQP
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "amqp": { "1": { "enable": true, "url": "", "exchange": "", "routingKey": "", "exchangeType": "", "mandatory": false, "immediate": false, "durable": false, "internal": false, "noWait": false, "autoDeleted": false }}}}`, false},

		// Test 13 - Test NATS
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "nats": { "1": { "enable": true, "address": "", "subject": "", "username": "", "password": "", "token": "", "secure": false, "pingInterval": 0, "streaming": { "enable": false, "clusterID": "", "clientID": "", "async": false, "maxPubAcksInflight": 0 } } }}}`, false},

		// Test 14 - Test ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "url": "", "index": "" } }}}`, false},

		// Test 15 - Test Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "address": "", "password": "", "key": "" } }}}`, false},

		// Test 16 - Test PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "table": "", "host": "", "port": "", "user": "", "password": "", "database": "" }}}}`, false},

		// Test 17 - Test Kafka
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "kafka": { "1": { "enable": true, "brokers": null, "topic": "" } }}}`, false},

		// Test 18 - Test Webhook
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "webhook": { "1": { "enable": true, "endpoint": "" } }}}`, false},

		// Test 20 - Test MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "table": "", "host": "", "port": "", "user": "", "password": "", "database": "" }}}}`, false},

		// Test 21 - Test Format for MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "format": "invalid", "table": "xxx", "host": "10.0.0.1", "port": "3306", "user": "abc", "password": "pqr", "database": "test1" }}}}`, false},

		// Test 22 - Test valid Format for MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "format": "namespace", "table": "xxx", "host": "10.0.0.1", "port": "3306", "user": "abc", "password": "pqr", "database": "test1" }}}}`, true},

		// Test 23 - Test Format for PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "format": "invalid", "table": "xxx", "host": "myhost", "port": "5432", "user": "abc", "password": "pqr", "database": "test1" }}}}`, false},

		// Test 24 - Test valid Format for PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "format": "namespace", "table": "xxx", "host": "myhost", "port": "5432", "user": "abc", "password": "pqr", "database": "test1" }}}}`, true},

		// Test 25 - Test Format for ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "format": "invalid", "url": "example.com", "index": "myindex" } }}}`, false},

		// Test 26 - Test valid Format for ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "format": "namespace", "url": "example.com", "index": "myindex" } }}}`, true},

		// Test 27 - Test Format for Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "format": "invalid", "address": "example.com:80", "password": "xxx", "key": "key1" } }}}`, false},

		// Test 28 - Test valid Format for Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "format": "namespace", "address": "example.com:80", "password": "xxx", "key": "key1" } }}}`, true},
	}

	for i, testCase := range testCases {
		if werr := ioutil.WriteFile(configPath, []byte(testCase.configData), 0700); werr != nil {
			t.Fatal(werr)
		}
		_, verr := getValidConfig()
		if testCase.shouldPass && verr != nil {
			t.Errorf("Test %d, should pass but it failed with err = %v", i+1, verr)
		}
		if !testCase.shouldPass && verr == nil {
			t.Errorf("Test %d, should fail but it succeed.", i+1)
		}
	}

}
