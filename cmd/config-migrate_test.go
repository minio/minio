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
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

const lastConfigVersion = 7

// TestServerConfigMigrateV1 - tests if a config v1 is purged
func TestServerConfigMigrateV1(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	setGlobalConfigPath(rootPath)

	// Create a V1 config json file and store it
	configJSON := "{ \"version\":\"1\", \"accessKeyId\":\"abcde\", \"secretAccessKey\":\"abcdefgh\"}"
	configPath := rootPath + "/fsUsers.json"
	if err := ioutil.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Fire a migrateConfig()
	if err := migrateConfig(); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	// Check if config v1 is removed from filesystem
	if _, err := os.Stat(configPath); err == nil || !os.IsNotExist(err) {
		t.Fatal("Config V1 file is not purged")
	}

	// Initialize server config and check again if everything is fine
	if err := initConfig(); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}

// TestServerConfigMigrateV1 - tests if all migrate code return nil when config file is not existent
func TestServerConfigMigrateInexistentConfig(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	setGlobalConfigPath(rootPath)
	configPath := rootPath + "/" + globalMinioConfigFile

	// Remove config file
	if err := os.Remove(configPath); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	if err := migrateV2ToV3(); err != nil {
		t.Fatal("migrate v2 to v3 should succeed when no config file is found")
	}
	if err := migrateV3ToV4(); err != nil {
		t.Fatal("migrate v3 to v4 should succeed when no config file is found")
	}
	if err := migrateV4ToV5(); err != nil {
		t.Fatal("migrate v4 to v5 should succeed when no config file is found")
	}
	if err := migrateV5ToV6(); err != nil {
		t.Fatal("migrate v5 to v6 should succeed when no config file is found")
	}
	if err := migrateV6ToV7(); err != nil {
		t.Fatal("migrate v6 to v7 should succeed when no config file is found")
	}
}

// TestServerConfigMigrateV2toV7 - tests if a config from v2 to v7 is successfully done
func TestServerConfigMigrateV2toV7(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	setGlobalConfigPath(rootPath)
	configPath := rootPath + "/" + globalMinioConfigFile

	// Create a corrupted config file
	if err := ioutil.WriteFile(configPath, []byte("{ \"version\":\"2\","), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	// Fire a migrateConfig()
	if err := migrateConfig(); err == nil {
		t.Fatal("migration should fail with corrupted config file")
	}

	accessKey := "accessfoo"
	secretKey := "secretfoo"

	// Create a V2 config json file and store it
	configJSON := "{ \"version\":\"2\", \"credentials\": {\"accessKeyId\":\"" + accessKey + "\", \"secretAccessKey\":\"" + secretKey + "\", \"region\":\"us-east-1\"}, \"mongoLogger\":{\"addr\":\"127.0.0.1:3543\", \"db\":\"foodb\", \"collection\":\"foo\"}, \"syslogLogger\":{\"network\":\"127.0.0.1:543\", \"addr\":\"addr\"}, \"fileLogger\":{\"filename\":\"log.out\"}}"
	if err := ioutil.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	// Fire a migrateConfig()
	if err := migrateConfig(); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Initialize server config and check again if everything is fine
	if err := initConfig(); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}

	// Check the version number in the upgraded config file
	expectedVersion := strconv.Itoa(lastConfigVersion)
	if serverConfig.Version != expectedVersion {
		t.Fatalf("Expect version "+expectedVersion+", found: %v", serverConfig.Version)
	}

	// Check if accessKey and secretKey are not altered during migration
	if serverConfig.Credential.AccessKeyID != accessKey {
		t.Fatalf("Access key lost during migration, expected: %v, found:%v", accessKey, serverConfig.Credential.AccessKeyID)
	}
	if serverConfig.Credential.SecretAccessKey != secretKey {
		t.Fatalf("Secret key lost during migration, expected: %v, found: %v", secretKey, serverConfig.Credential.SecretAccessKey)
	}

	// Initialize server config and check again if everything is fine
	if err := initConfig(); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}

// TestServerConfigMigrateFaultyConfig - checks if all migrate code return errors with corrupted config files
func TestServerConfigMigrateFaultyConfig(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	setGlobalConfigPath(rootPath)
	configPath := rootPath + "/" + globalMinioConfigFile

	// Create a corrupted config file
	if err := ioutil.WriteFile(configPath, []byte("{ \"version\":\""), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Test different migrate versions and be sure they are returning an error
	if err := migrateV2ToV3(); err == nil {
		t.Fatal("migrateConfigV2ToV3() should fail with a corrupted json")
	}
	if err := migrateV3ToV4(); err == nil {
		t.Fatal("migrateConfigV3ToV4() should fail with a corrupted json")
	}
	if err := migrateV4ToV5(); err == nil {
		t.Fatal("migrateConfigV4ToV5() should fail with a corrupted json")
	}
	if err := migrateV5ToV6(); err == nil {
		t.Fatal("migrateConfigV5ToV6() should fail with a corrupted json")
	}
	if err := migrateV6ToV7(); err == nil {
		t.Fatal("migrateConfigV6ToV7() should fail with a corrupted json")
	}

}
