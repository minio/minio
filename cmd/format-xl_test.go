/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

// Test get offline/online uuids.
func TestGetUUIDs(t *testing.T) {
	fmtV2 := newFormatXLV3(4, 16)
	formats := make([]*formatXLV3, 64)

	for i := 0; i < 4; i++ {
		for j := 0; j < 16; j++ {
			newFormat := *fmtV2
			newFormat.XL.This = fmtV2.XL.Sets[i][j]
			formats[i*16+j] = &newFormat
		}
	}

	gotCount := len(getOnlineUUIDs(fmtV2, formats))
	if gotCount != 64 {
		t.Errorf("Expected online count '64', got '%d'", gotCount)
	}

	for i := 0; i < 4; i++ {
		for j := 0; j < 16; j++ {
			if j < 4 {
				formats[i*16+j] = nil
			}
		}
	}

	gotCount = len(getOnlineUUIDs(fmtV2, formats))
	if gotCount != 48 {
		t.Errorf("Expected online count '48', got '%d'", gotCount)
	}

	gotCount = len(getOfflineUUIDs(fmtV2, formats))
	if gotCount != 16 {
		t.Errorf("Expected offline count '16', got '%d'", gotCount)
	}

	markUUIDsOffline(fmtV2, formats)
	gotCount = 0
	for i := range fmtV2.XL.Sets {
		for j := range fmtV2.XL.Sets[i] {
			if fmtV2.XL.Sets[i][j] == offlineDiskUUID {
				gotCount++
			}
		}
	}
	if gotCount != 16 {
		t.Errorf("Expected offline count '16', got '%d'", gotCount)
	}
}

// tests fixFormatXLV3 - fix format.json on all disks.
func TestFixFormatV3(t *testing.T) {
	xlDirs, err := getRandomDisks(8)
	if err != nil {
		t.Fatal(err)
	}
	for _, xlDir := range xlDirs {
		defer os.RemoveAll(xlDir)
	}
	endpoints := mustGetNewEndpointList(xlDirs...)

	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		t.Fatal(err)
	}

	format := newFormatXLV3(1, 8)
	formats := make([]*formatXLV3, 8)

	for j := 0; j < 8; j++ {
		newFormat := *format
		newFormat.XL.This = format.XL.Sets[0][j]
		formats[j] = &newFormat
	}

	if err = initFormatXLMetaVolume(storageDisks, formats); err != nil {
		t.Fatal(err)
	}

	formats[1] = nil
	expThis := formats[2].XL.This
	formats[2].XL.This = ""
	if err := fixFormatXLV3(storageDisks, endpoints, formats); err != nil {
		t.Fatal(err)
	}

	newFormats, errs := loadFormatXLAll(storageDisks)
	for _, err := range errs {
		if err != nil && err != errUnformattedDisk {
			t.Fatal(err)
		}
	}
	gotThis := newFormats[2].XL.This
	if expThis != gotThis {
		t.Fatalf("expected uuid %s, got %s", expThis, gotThis)
	}
}

// tests formatXLV3ThisEmpty conditions.
func TestFormatXLEmpty(t *testing.T) {
	format := newFormatXLV3(1, 16)
	formats := make([]*formatXLV3, 16)

	for j := 0; j < 16; j++ {
		newFormat := *format
		newFormat.XL.This = format.XL.Sets[0][j]
		formats[j] = &newFormat
	}

	// empty format to indicate disk not found, but this
	// empty should return false.
	formats[0] = nil

	if ok := formatXLV3ThisEmpty(formats); ok {
		t.Fatalf("expected value false, got %t", ok)
	}

	formats[2].XL.This = ""
	if ok := formatXLV3ThisEmpty(formats); !ok {
		t.Fatalf("expected value true, got %t", ok)
	}
}

// Tests format xl get version.
func TestFormatXLGetVersion(t *testing.T) {
	// Get test root.
	rootPath, err := getTestRoot()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	v := &formatXLVersionDetect{}
	v.XL.Version = "1"
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(pathJoin(rootPath, formatConfigFile), b, os.FileMode(0644)); err != nil {
		t.Fatal(err)
	}

	_, err = formatXLGetVersion("not-found")
	if err == nil {
		t.Fatal("Expected to fail but found success")
	}

	vstr, err := formatXLGetVersion(pathJoin(rootPath, formatConfigFile))
	if err != nil {
		t.Fatal(err)
	}
	if vstr != "1" {
		t.Fatalf("Expected version '1', got '%s'", vstr)
	}
}

// Tests format get backend format.
func TestFormatMetaGetFormatBackendXL(t *testing.T) {
	// Get test root.
	rootPath, err := getTestRoot()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	m := &formatMetaV1{
		Format:  "fs",
		Version: formatMetaVersionV1,
	}

	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	if err = ioutil.WriteFile(pathJoin(rootPath, formatConfigFile), b, os.FileMode(0644)); err != nil {
		t.Fatal(err)
	}

	_, err = formatMetaGetFormatBackendXL("not-found")
	if err == nil {
		t.Fatal("Expected to fail but found success")
	}

	format, err := formatMetaGetFormatBackendXL(pathJoin(rootPath, formatConfigFile))
	if err != nil {
		t.Fatal(err)
	}
	if format != m.Format {
		t.Fatalf("Expected format value %s, got %s", m.Format, format)
	}

	m = &formatMetaV1{
		Format:  "xl",
		Version: "2",
	}

	b, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	if err = ioutil.WriteFile(pathJoin(rootPath, formatConfigFile), b, os.FileMode(0644)); err != nil {
		t.Fatal(err)
	}

	_, err = formatMetaGetFormatBackendXL(pathJoin(rootPath, formatConfigFile))
	if err == nil {
		t.Fatal("Expected to fail with incompatible meta version")
	}
}

// Tests xl format migration.
func TestFormatXLMigrate(t *testing.T) {
	// Get test root.
	rootPath, err := getTestRoot()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	m := &formatXLV1{}
	m.Format = formatBackendXL
	m.Version = formatMetaVersionV1
	m.XL.Version = formatXLVersionV1
	m.XL.Disk = mustGetUUID()
	m.XL.JBOD = []string{m.XL.Disk, mustGetUUID(), mustGetUUID(), mustGetUUID()}

	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	if err = os.MkdirAll(pathJoin(rootPath, minioMetaBucket), os.FileMode(0755)); err != nil {
		t.Fatal(err)
	}

	if err = ioutil.WriteFile(pathJoin(rootPath, minioMetaBucket, formatConfigFile), b, os.FileMode(0644)); err != nil {
		t.Fatal(err)
	}

	if err = formatXLMigrate(rootPath); err != nil {
		t.Fatal(err)
	}

	migratedVersion, err := formatXLGetVersion(pathJoin(rootPath, minioMetaBucket, formatConfigFile))
	if err != nil {
		t.Fatal(err)
	}
	if migratedVersion != formatXLVersionV3 {
		t.Fatalf("expected version: %s, got: %s", formatXLVersionV3, migratedVersion)
	}

	b, err = ioutil.ReadFile(pathJoin(rootPath, minioMetaBucket, formatConfigFile))
	if err != nil {
		t.Fatal(err)
	}
	formatV3 := &formatXLV3{}
	if err = json.Unmarshal(b, formatV3); err != nil {
		t.Fatal(err)
	}
	if formatV3.XL.This != m.XL.Disk {
		t.Fatalf("expected disk uuid: %s, got: %s", m.XL.Disk, formatV3.XL.This)
	}
	if len(formatV3.XL.Sets) != 1 {
		t.Fatalf("expected single set after migrating from v1 to v3, but found %d", len(formatV3.XL.Sets))
	}
	if !reflect.DeepEqual(formatV3.XL.Sets[0], m.XL.JBOD) {
		t.Fatalf("expected disk uuid: %v, got: %v", m.XL.JBOD, formatV3.XL.Sets[0])
	}

	m = &formatXLV1{}
	m.Format = "unknown"
	m.Version = formatMetaVersionV1
	m.XL.Version = formatXLVersionV1
	m.XL.Disk = mustGetUUID()
	m.XL.JBOD = []string{m.XL.Disk, mustGetUUID(), mustGetUUID(), mustGetUUID()}

	b, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	if err = ioutil.WriteFile(pathJoin(rootPath, minioMetaBucket, formatConfigFile), b, os.FileMode(0644)); err != nil {
		t.Fatal(err)
	}

	if err = formatXLMigrate(rootPath); err == nil {
		t.Fatal("Expected to fail with unexpected backend format")
	}

	m = &formatXLV1{}
	m.Format = formatBackendXL
	m.Version = formatMetaVersionV1
	m.XL.Version = "30"
	m.XL.Disk = mustGetUUID()
	m.XL.JBOD = []string{m.XL.Disk, mustGetUUID(), mustGetUUID(), mustGetUUID()}

	b, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	if err = ioutil.WriteFile(pathJoin(rootPath, minioMetaBucket, formatConfigFile), b, os.FileMode(0644)); err != nil {
		t.Fatal(err)
	}

	if err = formatXLMigrate(rootPath); err == nil {
		t.Fatal("Expected to fail with unexpected backend format version number")
	}
}

// Tests check format xl value.
func TestCheckFormatXLValue(t *testing.T) {
	testCases := []struct {
		format  *formatXLV3
		success bool
	}{
		// Invalid XL format version "2".
		{
			&formatXLV3{
				Version: "2",
				Format:  "XL",
				XL: struct {
					Version          string     `json:"version"`
					This             string     `json:"this"`
					Sets             [][]string `json:"sets"`
					DistributionAlgo string     `json:"distributionAlgo"`
				}{
					Version: "2",
				},
			},
			false,
		},
		// Invalid XL format "Unknown".
		{
			&formatXLV3{
				Version: "1",
				Format:  "Unknown",
				XL: struct {
					Version          string     `json:"version"`
					This             string     `json:"this"`
					Sets             [][]string `json:"sets"`
					DistributionAlgo string     `json:"distributionAlgo"`
				}{
					Version: "2",
				},
			},
			false,
		},
		// Invalid XL format version "0".
		{
			&formatXLV3{
				Version: "1",
				Format:  "XL",
				XL: struct {
					Version          string     `json:"version"`
					This             string     `json:"this"`
					Sets             [][]string `json:"sets"`
					DistributionAlgo string     `json:"distributionAlgo"`
				}{
					Version: "0",
				},
			},
			false,
		},
	}

	// Valid all test cases.
	for i, testCase := range testCases {
		if err := checkFormatXLValue(testCase.format); err != nil && testCase.success {
			t.Errorf("Test %d: Expected failure %s", i+1, err)
		}
	}
}

// Tests getFormatXLInQuorum()
func TestGetFormatXLInQuorumCheck(t *testing.T) {
	setCount := 2
	disksPerSet := 16

	format := newFormatXLV3(setCount, disksPerSet)
	formats := make([]*formatXLV3, 32)

	for i := 0; i < setCount; i++ {
		for j := 0; j < disksPerSet; j++ {
			newFormat := *format
			newFormat.XL.This = format.XL.Sets[i][j]
			formats[i*disksPerSet+j] = &newFormat
		}
	}

	// Return a format from list of formats in quorum.
	quorumFormat, err := getFormatXLInQuorum(formats)
	if err != nil {
		t.Fatal(err)
	}

	// Check if the reference format and input formats are same.
	if err = formatXLV3Check(quorumFormat, formats[0]); err != nil {
		t.Fatal(err)
	}

	// QuorumFormat has .This field empty on purpose, expect a failure.
	if err = formatXLV3Check(formats[0], quorumFormat); err == nil {
		t.Fatal("Unexpected success")
	}

	formats[0] = nil
	quorumFormat, err = getFormatXLInQuorum(formats)
	if err != nil {
		t.Fatal(err)
	}

	badFormat := *quorumFormat
	badFormat.XL.Sets = nil
	if err = formatXLV3Check(quorumFormat, &badFormat); err == nil {
		t.Fatal("Unexpected success")
	}

	badFormatUUID := *quorumFormat
	badFormatUUID.XL.Sets[0][0] = "bad-uuid"
	if err = formatXLV3Check(quorumFormat, &badFormatUUID); err == nil {
		t.Fatal("Unexpected success")
	}

	badFormatSetSize := *quorumFormat
	badFormatSetSize.XL.Sets[0] = nil
	if err = formatXLV3Check(quorumFormat, &badFormatSetSize); err == nil {
		t.Fatal("Unexpected success")
	}

	for i := range formats {
		if i < 17 {
			formats[i] = nil
		}
	}
	if _, err = getFormatXLInQuorum(formats); err == nil {
		t.Fatal("Unexpected success")
	}
}

// Initialize new format sets.
func TestNewFormatSets(t *testing.T) {
	setCount := 2
	disksPerSet := 16

	format := newFormatXLV3(setCount, disksPerSet)
	formats := make([]*formatXLV3, 32)
	errs := make([]error, 32)

	for i := 0; i < setCount; i++ {
		for j := 0; j < disksPerSet; j++ {
			newFormat := *format
			newFormat.XL.This = format.XL.Sets[i][j]
			formats[i*disksPerSet+j] = &newFormat
		}
	}

	quorumFormat, err := getFormatXLInQuorum(formats)
	if err != nil {
		t.Fatal(err)
	}

	// 16th disk is unformatted.
	errs[15] = errUnformattedDisk

	newFormats := newHealFormatSets(quorumFormat, setCount, disksPerSet, formats, errs)
	if newFormats == nil {
		t.Fatal("Unexpected failure")
	}
}
