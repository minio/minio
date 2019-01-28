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
	"testing"
)

// Test if config v1 is purged
func TestServerConfigMigrateV1(t *testing.T) {
	rootPath, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)
	globalConfigDir = &ConfigDir{path: rootPath}

	// Create a V1 config json file and store it
	configJSON := "{ \"version\":\"1\", \"accessKeyId\":\"abcde\", \"secretAccessKey\":\"abcdefgh\"}"
	configPath := rootPath + "/fsUsers.json"
	if err := ioutil.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	if err := purgeV1(); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Check if config v1 is removed from filesystem
	if _, err := os.Stat(configPath); err == nil || !os.IsNotExist(err) {
		t.Fatal("Config V1 file is not purged")
	}
}

// Test if a config migration from v2 to v33 is successfully done
func TestServerConfigMigrateV2toV33(t *testing.T) {
	configBytes := []byte("{ \"version\":\"2\",")

	// Fire a migrateConfig()
	if _, err := migrateConfig(configBytes); err == nil {
		t.Fatal("migration should fail with corrupted config file")
	}

	accessKey := "accessfoo"
	secretKey := "secretfoo"

	// Create a V2 config json file and store it
	configJSON := "{ \"version\":\"2\", \"credentials\": {\"accessKeyId\":\"" + accessKey + "\", \"secretAccessKey\":\"" + secretKey + "\", \"region\":\"us-east-1\"}, \"mongoLogger\":{\"addr\":\"127.0.0.1:3543\", \"db\":\"foodb\", \"collection\":\"foo\"}, \"syslogLogger\":{\"network\":\"127.0.0.1:543\", \"addr\":\"addr\"}, \"fileLogger\":{\"filename\":\"log.out\"}}"

	configBytes, err := migrateConfig([]byte(configJSON))
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	version := configGetVersion(configBytes)
	if version != serverConfigVersion {
		t.Fatalf("migrateConfig failed, expected version: %s, got: %s", serverConfigVersion, version)
	}
}

// Test if all migrate code returns error with corrupted config files
func TestServerConfigMigrateFaultyConfig(t *testing.T) {
	configBytes := []byte("{ \"version\":\"2\", \"test\":")

	// Test different migrate versions and be sure they are returning an error
	if _, err := migrateV2ToV3(configBytes); err == nil {
		t.Fatal("migrateConfigV2ToV3() should fail with a corrupted json")
	}
	if _, err := migrateV3ToV4(configBytes); err == nil {
		t.Fatal("migrateConfigV3ToV4() should fail with a corrupted json")
	}
	if _, err := migrateV4ToV5(configBytes); err == nil {
		t.Fatal("migrateConfigV4ToV5() should fail with a corrupted json")
	}
	if _, err := migrateV5ToV6(configBytes); err == nil {
		t.Fatal("migrateConfigV5ToV6() should fail with a corrupted json")
	}
	if _, err := migrateV6ToV7(configBytes); err == nil {
		t.Fatal("migrateConfigV6ToV7() should fail with a corrupted json")
	}
	if _, err := migrateV7ToV8(configBytes); err == nil {
		t.Fatal("migrateConfigV7ToV8() should fail with a corrupted json")
	}
	if _, err := migrateV8ToV9(configBytes); err == nil {
		t.Fatal("migrateConfigV8ToV9() should fail with a corrupted json")
	}
	if _, err := migrateV9ToV10(configBytes); err == nil {
		t.Fatal("migrateConfigV9ToV10() should fail with a corrupted json")
	}
	if _, err := migrateV10ToV11(configBytes); err == nil {
		t.Fatal("migrateConfigV10ToV11() should fail with a corrupted json")
	}
	if _, err := migrateV11ToV12(configBytes); err == nil {
		t.Fatal("migrateConfigV11ToV12() should fail with a corrupted json")
	}
	if _, err := migrateV12ToV13(configBytes); err == nil {
		t.Fatal("migrateConfigV12ToV13() should fail with a corrupted json")
	}
	if _, err := migrateV13ToV14(configBytes); err == nil {
		t.Fatal("migrateConfigV13ToV14() should fail with a corrupted json")
	}
	if _, err := migrateV14ToV15(configBytes); err == nil {
		t.Fatal("migrateConfigV14ToV15() should fail with a corrupted json")
	}
	if _, err := migrateV15ToV16(configBytes); err == nil {
		t.Fatal("migrateConfigV15ToV16() should fail with a corrupted json")
	}
	if _, err := migrateV16ToV17(configBytes); err == nil {
		t.Fatal("migrateConfigV16ToV17() should fail with a corrupted json")
	}
	if _, err := migrateV17ToV18(configBytes); err == nil {
		t.Fatal("migrateConfigV17ToV18() should fail with a corrupted json")
	}
	if _, err := migrateV18ToV19(configBytes); err == nil {
		t.Fatal("migrateConfigV18ToV19() should fail with a corrupted json")
	}
	if _, err := migrateV19ToV20(configBytes); err == nil {
		t.Fatal("migrateConfigV19ToV20() should fail with a corrupted json")
	}
	if _, err := migrateV20ToV21(configBytes); err == nil {
		t.Fatal("migrateConfigV20ToV21() should fail with a corrupted json")
	}
	if _, err := migrateV21ToV22(configBytes); err == nil {
		t.Fatal("migrateConfigV21ToV22() should fail with a corrupted json")
	}
	if _, err := migrateV22ToV23(configBytes); err == nil {
		t.Fatal("migrateConfigV22ToV23() should fail with a corrupted json")
	}
	if _, err := migrateV23ToV24(configBytes); err == nil {
		t.Fatal("migrateConfigV23ToV24() should fail with a corrupted json")
	}
	if _, err := migrateV24ToV25(configBytes); err == nil {
		t.Fatal("migrateConfigV24ToV25() should fail with a corrupted json")
	}
	if _, err := migrateV25ToV26(configBytes); err == nil {
		t.Fatal("migrateConfigV25ToV26() should fail with a corrupted json")
	}
	if _, err := migrateV26ToV27(configBytes); err == nil {
		t.Fatal("migrateConfigV26ToV27() should fail with a corrupted json")
	}
	if _, err := migrateV27ToV28(configBytes); err == nil {
		t.Fatal("migrateConfigV27ToV28() should fail with a corrupted json")
	}
	if _, err := migrateV28ToV29(configBytes); err == nil {
		t.Fatal("migrateConfigV27ToV28() should fail with a corrupted json")
	}
	if _, err := migrateV29ToV30(configBytes); err == nil {
		t.Fatal("migrateConfigV27ToV28() should fail with a corrupted json")
	}
	if _, err := migrateV30ToV31(configBytes); err == nil {
		t.Fatal("migrateConfigV27ToV28() should fail with a corrupted json")
	}
	if _, err := migrateV31ToV32(configBytes); err == nil {
		t.Fatal("migrateConfigV27ToV28() should fail with a corrupted json")
	}
	if _, err := migrateV32ToV33(configBytes); err == nil {
		t.Fatal("migrateConfigV27ToV28() should fail with a corrupted json")
	}
}

// Test if all migrate code returns error with corrupted config files
func TestServerConfigMigrateCorruptedConfig(t *testing.T) {
	configBytes := []byte("{ \"version\":\"2\", \"credentials\": { \"accessKeyId\": 1 } }")

	// Test different migrate versions and be sure they are returning an error
	if _, err := migrateConfig(configBytes); err == nil {
		t.Fatal("migrateConfig() should fail with a corrupted json")
	}
}
