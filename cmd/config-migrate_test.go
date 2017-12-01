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
	"io/ioutil"
	"os"
	"testing"
)

// Test if config v1 is purged
func TestServerConfigMigrateV1(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	setConfigDir(rootPath)

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
	if err := loadConfig(); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}

// Test if all migrate code returns nil when config file does not
// exist
func TestServerConfigMigrateInexistentConfig(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	setConfigDir(rootPath)
	configPath := rootPath + "/" + minioConfigFile

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
	if err := migrateV7ToV8(); err != nil {
		t.Fatal("migrate v7 to v8 should succeed when no config file is found")
	}
	if err := migrateV8ToV9(); err != nil {
		t.Fatal("migrate v8 to v9 should succeed when no config file is found")
	}
	if err := migrateV9ToV10(); err != nil {
		t.Fatal("migrate v9 to v10 should succeed when no config file is found")
	}
	if err := migrateV10ToV11(); err != nil {
		t.Fatal("migrate v10 to v11 should succeed when no config file is found")
	}
	if err := migrateV11ToV12(); err != nil {
		t.Fatal("migrate v11 to v12 should succeed when no config file is found")
	}
	if err := migrateV12ToV13(); err != nil {
		t.Fatal("migrate v12 to v13 should succeed when no config file is found")
	}
	if err := migrateV13ToV14(); err != nil {
		t.Fatal("migrate v13 to v14 should succeed when no config file is found")
	}
	if err := migrateV14ToV15(); err != nil {
		t.Fatal("migrate v14 to v15 should succeed when no config file is found")
	}
	if err := migrateV15ToV16(); err != nil {
		t.Fatal("migrate v15 to v16 should succeed when no config file is found")
	}
	if err := migrateV16ToV17(); err != nil {
		t.Fatal("migrate v16 to v17 should succeed when no config file is found")
	}
	if err := migrateV17ToV18(); err != nil {
		t.Fatal("migrate v17 to v18 should succeed when no config file is found")
	}
	if err := migrateV18ToV19(); err != nil {
		t.Fatal("migrate v18 to v19 should succeed when no config file is found")
	}
	if err := migrateV19ToV20(); err != nil {
		t.Fatal("migrate v19 to v20 should succeed when no config file is found")
	}
	if err := migrateV20ToV21(); err != nil {
		t.Fatal("migrate v20 to v21 should succeed when no config file is found")
	}
}

// Test if a config migration from v2 to v21 is successfully done
func TestServerConfigMigrateV2toV21(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	setConfigDir(rootPath)
	configPath := rootPath + "/" + minioConfigFile

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
	if err := loadConfig(); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}

	// Check the version number in the upgraded config file
	expectedVersion := serverConfigVersion
	if globalServerConfig.Version != expectedVersion {
		t.Fatalf("Expect version "+expectedVersion+", found: %v", globalServerConfig.Version)
	}

	// Check if accessKey and secretKey are not altered during migration
	if globalServerConfig.Credential.AccessKey != accessKey {
		t.Fatalf("Access key lost during migration, expected: %v, found:%v", accessKey, globalServerConfig.Credential.AccessKey)
	}
	if globalServerConfig.Credential.SecretKey != secretKey {
		t.Fatalf("Secret key lost during migration, expected: %v, found: %v", secretKey, globalServerConfig.Credential.SecretKey)
	}
}

// Test if all migrate code returns error with corrupted config files
func TestServerConfigMigrateFaultyConfig(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	setConfigDir(rootPath)
	configPath := rootPath + "/" + minioConfigFile

	// Create a corrupted config file
	if err := ioutil.WriteFile(configPath, []byte("{ \"version\":\"2\", \"test\":"), 0644); err != nil {
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
	if err := migrateV7ToV8(); err == nil {
		t.Fatal("migrateConfigV7ToV8() should fail with a corrupted json")
	}
	if err := migrateV8ToV9(); err == nil {
		t.Fatal("migrateConfigV8ToV9() should fail with a corrupted json")
	}
	if err := migrateV9ToV10(); err == nil {
		t.Fatal("migrateConfigV9ToV10() should fail with a corrupted json")
	}
	if err := migrateV10ToV11(); err == nil {
		t.Fatal("migrateConfigV10ToV11() should fail with a corrupted json")
	}
	if err := migrateV11ToV12(); err == nil {
		t.Fatal("migrateConfigV11ToV12() should fail with a corrupted json")
	}
	if err := migrateV12ToV13(); err == nil {
		t.Fatal("migrateConfigV12ToV13() should fail with a corrupted json")
	}
	if err := migrateV13ToV14(); err == nil {
		t.Fatal("migrateConfigV13ToV14() should fail with a corrupted json")
	}
	if err := migrateV14ToV15(); err == nil {
		t.Fatal("migrateConfigV14ToV15() should fail with a corrupted json")
	}
	if err := migrateV15ToV16(); err == nil {
		t.Fatal("migrateConfigV15ToV16() should fail with a corrupted json")
	}
	if err := migrateV16ToV17(); err == nil {
		t.Fatal("migrateConfigV16ToV17() should fail with a corrupted json")
	}
	if err := migrateV17ToV18(); err == nil {
		t.Fatal("migrateConfigV17ToV18() should fail with a corrupted json")
	}
	if err := migrateV18ToV19(); err == nil {
		t.Fatal("migrateConfigV18ToV19() should fail with a corrupted json")
	}
	if err := migrateV19ToV20(); err == nil {
		t.Fatal("migrateConfigV19ToV20() should fail with a corrupted json")
	}
	if err := migrateV20ToV21(); err == nil {
		t.Fatal("migrateConfigV20ToV21() should fail with a corrupted json")
	}
}

// Test if all migrate code returns error with corrupted config files
func TestServerConfigMigrateCorruptedConfig(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	setConfigDir(rootPath)
	configPath := rootPath + "/" + minioConfigFile

	for i := 3; i <= 17; i++ {
		// Create a corrupted config file
		if err = ioutil.WriteFile(configPath, []byte(fmt.Sprintf("{ \"version\":\"%d\", \"credential\": { \"accessKey\": 1 } }", i)),
			0644); err != nil {
			t.Fatal("Unexpected error: ", err)
		}

		// Test different migrate versions and be sure they are returning an error
		if err = migrateConfig(); err == nil {
			t.Fatal("migrateConfig() should fail with a corrupted json")
		}
	}

	// Create a corrupted config file for version '2'.
	if err = ioutil.WriteFile(configPath, []byte("{ \"version\":\"2\", \"credentials\": { \"accessKeyId\": 1 } }"), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Test different migrate versions and be sure they are returning an error
	if err = migrateConfig(); err == nil {
		t.Fatal("migrateConfig() should fail with a corrupted json")
	}
}
