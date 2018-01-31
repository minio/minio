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

	"github.com/minio/minio/pkg/auth"
	"github.com/tidwall/gjson"
)

func TestServerConfig(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	if globalServerConfig.GetRegion() != globalMinioDefaultRegion {
		t.Errorf("Expecting region `us-east-1` found %s", globalServerConfig.GetRegion())
	}

	// Set new region and verify.
	globalServerConfig.SetRegion("us-west-1")
	if globalServerConfig.GetRegion() != "us-west-1" {
		t.Errorf("Expecting region `us-west-1` found %s", globalServerConfig.GetRegion())
	}

	// Set new amqp notification id.
	globalServerConfig.Notify.SetAMQPByID("2", amqpNotify{})
	savedNotifyCfg1 := globalServerConfig.Notify.GetAMQPByID("2")
	if !reflect.DeepEqual(savedNotifyCfg1, amqpNotify{}) {
		t.Errorf("Expecting AMQP config %#v found %#v", amqpNotify{}, savedNotifyCfg1)
	}

	// Set new elastic search notification id.
	globalServerConfig.Notify.SetElasticSearchByID("2", elasticSearchNotify{})
	savedNotifyCfg2 := globalServerConfig.Notify.GetElasticSearchByID("2")
	if !reflect.DeepEqual(savedNotifyCfg2, elasticSearchNotify{}) {
		t.Errorf("Expecting Elasticsearch config %#v found %#v", elasticSearchNotify{}, savedNotifyCfg2)
	}

	// Set new redis notification id.
	globalServerConfig.Notify.SetRedisByID("2", redisNotify{})
	savedNotifyCfg3 := globalServerConfig.Notify.GetRedisByID("2")
	if !reflect.DeepEqual(savedNotifyCfg3, redisNotify{}) {
		t.Errorf("Expecting Redis config %#v found %#v", redisNotify{}, savedNotifyCfg3)
	}

	// Set new kafka notification id.
	globalServerConfig.Notify.SetKafkaByID("2", kafkaNotify{})
	savedNotifyCfg4 := globalServerConfig.Notify.GetKafkaByID("2")
	if !reflect.DeepEqual(savedNotifyCfg4, kafkaNotify{}) {
		t.Errorf("Expecting Kafka config %#v found %#v", kafkaNotify{}, savedNotifyCfg4)
	}

	// Set new Webhook notification id.
	globalServerConfig.Notify.SetWebhookByID("2", webhookNotify{})
	savedNotifyCfg5 := globalServerConfig.Notify.GetWebhookByID("2")
	if !reflect.DeepEqual(savedNotifyCfg5, webhookNotify{}) {
		t.Errorf("Expecting Webhook config %#v found %#v", webhookNotify{}, savedNotifyCfg5)
	}

	// Set new MySQL notification id.
	globalServerConfig.Notify.SetMySQLByID("2", mySQLNotify{})
	savedNotifyCfg6 := globalServerConfig.Notify.GetMySQLByID("2")
	if !reflect.DeepEqual(savedNotifyCfg6, mySQLNotify{}) {
		t.Errorf("Expecting Webhook config %#v found %#v", mySQLNotify{}, savedNotifyCfg6)
	}

	// Set new MQTT notification id.
	globalServerConfig.Notify.SetMQTTByID("2", mqttNotify{})
	savedNotifyCfg7 := globalServerConfig.Notify.GetMQTTByID("2")
	if !reflect.DeepEqual(savedNotifyCfg7, mqttNotify{}) {
		t.Errorf("Expecting Webhook config %#v found %#v", mqttNotify{}, savedNotifyCfg7)
	}

	// Match version.
	if globalServerConfig.GetVersion() != serverConfigVersion {
		t.Errorf("Expecting version %s found %s", globalServerConfig.GetVersion(), serverConfigVersion)
	}

	// Attempt to save.
	if err := globalServerConfig.Save(); err != nil {
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

	os.Setenv("MINIO_DOMAIN", "domain.com")
	defer os.Unsetenv("MINIO_DOMAIN")

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
	defer os.RemoveAll(rootPath)

	// Check if serverConfig has
	if globalServerConfig.GetBrowser() {
		t.Errorf("Expecting browser is set to false found %v", globalServerConfig.GetBrowser())
	}

	// Check if serverConfig has
	if globalServerConfig.GetRegion() != "us-west-1" {
		t.Errorf("Expecting region to be \"us-west-1\" found %v", globalServerConfig.GetRegion())
	}

	// Check if serverConfig has
	cred := globalServerConfig.GetCredential()

	if cred.AccessKey != "minio" {
		t.Errorf("Expecting access key to be `minio` found %s", cred.AccessKey)
	}

	if cred.SecretKey != "minio123" {
		t.Errorf("Expecting access key to be `minio123` found %s", cred.SecretKey)
	}

	if globalServerConfig.Domain != "domain.com" {
		t.Errorf("Expecting Domain to be `domain.com` found " + globalServerConfig.Domain)
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
	defer os.RemoveAll(rootPath)

	configPath := filepath.Join(rootPath, minioConfigFile)

	v := serverConfigVersion

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

		// Test 11 - Test AMQP
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "amqp": { "1": { "enable": true, "url": "", "exchange": "", "routingKey": "", "exchangeType": "", "mandatory": false, "immediate": false, "durable": false, "internal": false, "noWait": false, "autoDeleted": false }}}}`, false},

		// Test 12 - Test NATS
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "nats": { "1": { "enable": true, "address": "", "subject": "", "username": "", "password": "", "token": "", "secure": false, "pingInterval": 0, "streaming": { "enable": false, "clusterID": "", "clientID": "", "async": false, "maxPubAcksInflight": 0 } } }}}`, false},

		// Test 13 - Test ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "url": "", "index": "" } }}}`, false},

		// Test 14 - Test Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "address": "", "password": "", "key": "" } }}}`, false},

		// Test 15 - Test PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "table": "", "host": "", "port": "", "user": "", "password": "", "database": "" }}}}`, false},

		// Test 16 - Test Kafka
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "kafka": { "1": { "enable": true, "brokers": null, "topic": "" } }}}`, false},

		// Test 17 - Test Webhook
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "webhook": { "1": { "enable": true, "endpoint": "" } }}}`, false},

		// Test 18 - Test MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "table": "", "host": "", "port": "", "user": "", "password": "", "database": "" }}}}`, false},

		// Test 19 - Test Format for MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "format": "invalid", "table": "xxx", "host": "10.0.0.1", "port": "3306", "user": "abc", "password": "pqr", "database": "test1" }}}}`, false},

		// Test 20 - Test valid Format for MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "format": "namespace", "table": "xxx", "host": "10.0.0.1", "port": "3306", "user": "abc", "password": "pqr", "database": "test1" }}}}`, true},

		// Test 21 - Test Format for PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "format": "invalid", "table": "xxx", "host": "myhost", "port": "5432", "user": "abc", "password": "pqr", "database": "test1" }}}}`, false},

		// Test 22 - Test valid Format for PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "format": "namespace", "table": "xxx", "host": "myhost", "port": "5432", "user": "abc", "password": "pqr", "database": "test1" }}}}`, true},

		// Test 23 - Test Format for ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "format": "invalid", "url": "example.com", "index": "myindex" } }}}`, false},

		// Test 24 - Test valid Format for ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "format": "namespace", "url": "example.com", "index": "myindex" } }}}`, true},

		// Test 25 - Test Format for Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "format": "invalid", "address": "example.com:80", "password": "xxx", "key": "key1" } }}}`, false},

		// Test 26 - Test valid Format for Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "format": "namespace", "address": "example.com:80", "password": "xxx", "key": "key1" } }}}`, true},

		// Test 27 - Test MQTT
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mqtt": { "1": { "enable": true, "broker": "",  "topic": "", "qos": 0, "clientId": "", "username": "", "password": ""}}}}`, false},
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
			t.Errorf("Test %d, should fail but it succeeded.", i+1)
		}
	}

}

func TestConfigDiff(t *testing.T) {
	testCases := []struct {
		s, t *serverConfig
		diff string
	}{
		// 1
		{&serverConfig{}, nil, "Given configuration is empty"},
		// 2
		{
			&serverConfig{Credential: auth.Credentials{"u1", "p1"}},
			&serverConfig{Credential: auth.Credentials{"u1", "p2"}},
			"Credential configuration differs",
		},
		// 3
		{&serverConfig{Region: "us-east-1"}, &serverConfig{Region: "us-west-1"}, "Region configuration differs"},
		// 4
		{&serverConfig{Browser: false}, &serverConfig{Browser: true}, "Browser configuration differs"},
		// 5
		{&serverConfig{Domain: "domain1"}, &serverConfig{Domain: "domain2"}, "Domain configuration differs"},
		// 6
		{
			&serverConfig{StorageClass: storageClassConfig{storageClass{"1", 8}, storageClass{"2", 6}}},
			&serverConfig{StorageClass: storageClassConfig{storageClass{"1", 8}, storageClass{"2", 4}}},
			"StorageClass configuration differs",
		},
		// 7
		{
			&serverConfig{Notify: notifier{AMQP: map[string]amqpNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{AMQP: map[string]amqpNotify{"1": {Enable: false}}}},
			"AMQP Notification configuration differs",
		},
		// 8
		{
			&serverConfig{Notify: notifier{NATS: map[string]natsNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{NATS: map[string]natsNotify{"1": {Enable: false}}}},
			"NATS Notification configuration differs",
		},
		// 9
		{
			&serverConfig{Notify: notifier{ElasticSearch: map[string]elasticSearchNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{ElasticSearch: map[string]elasticSearchNotify{"1": {Enable: false}}}},
			"ElasticSearch Notification configuration differs",
		},
		// 10
		{
			&serverConfig{Notify: notifier{Redis: map[string]redisNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{Redis: map[string]redisNotify{"1": {Enable: false}}}},
			"Redis Notification configuration differs",
		},
		// 11
		{
			&serverConfig{Notify: notifier{PostgreSQL: map[string]postgreSQLNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{PostgreSQL: map[string]postgreSQLNotify{"1": {Enable: false}}}},
			"PostgreSQL Notification configuration differs",
		},
		// 12
		{
			&serverConfig{Notify: notifier{Kafka: map[string]kafkaNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{Kafka: map[string]kafkaNotify{"1": {Enable: false}}}},
			"Kafka Notification configuration differs",
		},
		// 13
		{
			&serverConfig{Notify: notifier{Webhook: map[string]webhookNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{Webhook: map[string]webhookNotify{"1": {Enable: false}}}},
			"Webhook Notification configuration differs",
		},
		// 14
		{
			&serverConfig{Notify: notifier{MySQL: map[string]mySQLNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{MySQL: map[string]mySQLNotify{"1": {Enable: false}}}},
			"MySQL Notification configuration differs",
		},
		// 15
		{
			&serverConfig{Notify: notifier{MQTT: map[string]mqttNotify{"1": {Enable: true}}}},
			&serverConfig{Notify: notifier{MQTT: map[string]mqttNotify{"1": {Enable: false}}}},
			"MQTT Notification configuration differs",
		},
	}

	for i, testCase := range testCases {
		got := testCase.s.ConfigDiff(testCase.t)
		if got != testCase.diff {
			t.Errorf("Test %d: got %s expected %s", i+1, got, testCase.diff)
		}
	}
}
