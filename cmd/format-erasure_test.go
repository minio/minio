// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

// tests fixFormatErasureV3 - fix format.json on all disks.
func TestFixFormatV3(t *testing.T) {
	erasureDirs, err := getRandomDisks(8)
	if err != nil {
		t.Fatal(err)
	}
	for _, erasureDir := range erasureDirs {
		defer os.RemoveAll(erasureDir)
	}
	endpoints := mustGetNewEndpoints(erasureDirs...)

	storageDisks, errs := initStorageDisksWithErrors(endpoints)
	for _, err := range errs {
		if err != nil && err != errDiskNotFound {
			t.Fatal(err)
		}
	}

	format := newFormatErasureV3(1, 8)
	format.Erasure.DistributionAlgo = formatErasureVersionV2DistributionAlgoV1
	formats := make([]*formatErasureV3, 8)

	for j := 0; j < 8; j++ {
		newFormat := format.Clone()
		newFormat.Erasure.This = format.Erasure.Sets[0][j]
		formats[j] = newFormat
	}

	if err = initErasureMetaVolumesInLocalDisks(storageDisks, formats); err != nil {
		t.Fatal(err)
	}

	formats[1] = nil
	expThis := formats[2].Erasure.This
	formats[2].Erasure.This = ""
	if err := fixFormatErasureV3(storageDisks, endpoints, formats); err != nil {
		t.Fatal(err)
	}

	newFormats, errs := loadFormatErasureAll(storageDisks, false)
	for _, err := range errs {
		if err != nil && err != errUnformattedDisk {
			t.Fatal(err)
		}
	}
	gotThis := newFormats[2].Erasure.This
	if expThis != gotThis {
		t.Fatalf("expected uuid %s, got %s", expThis, gotThis)
	}
}

// tests formatErasureV3ThisEmpty conditions.
func TestFormatErasureEmpty(t *testing.T) {
	format := newFormatErasureV3(1, 16)
	format.Erasure.DistributionAlgo = formatErasureVersionV2DistributionAlgoV1
	formats := make([]*formatErasureV3, 16)

	for j := 0; j < 16; j++ {
		newFormat := format.Clone()
		newFormat.Erasure.This = format.Erasure.Sets[0][j]
		formats[j] = newFormat
	}

	// empty format to indicate disk not found, but this
	// empty should return false.
	formats[0] = nil

	if ok := formatErasureV3ThisEmpty(formats); ok {
		t.Fatalf("expected value false, got %t", ok)
	}

	formats[2].Erasure.This = ""
	if ok := formatErasureV3ThisEmpty(formats); !ok {
		t.Fatalf("expected value true, got %t", ok)
	}
}

// Tests xl format migration.
func TestFormatErasureMigrate(t *testing.T) {
	// Get test root.
	rootPath, err := getTestRoot()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	m := &formatErasureV1{}
	m.Format = formatBackendErasure
	m.Version = formatMetaVersionV1
	m.Erasure.Version = formatErasureVersionV1
	m.Erasure.Disk = mustGetUUID()
	m.Erasure.JBOD = []string{m.Erasure.Disk, mustGetUUID(), mustGetUUID(), mustGetUUID()}

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

	if err = formatErasureMigrate(rootPath); err != nil {
		t.Fatal(err)
	}

	migratedVersion, err := formatGetBackendErasureVersion(pathJoin(rootPath, minioMetaBucket, formatConfigFile))
	if err != nil {
		t.Fatal(err)
	}

	if migratedVersion != formatErasureVersionV3 {
		t.Fatalf("expected version: %s, got: %s", formatErasureVersionV3, migratedVersion)
	}

	b, err = ioutil.ReadFile(pathJoin(rootPath, minioMetaBucket, formatConfigFile))
	if err != nil {
		t.Fatal(err)
	}
	formatV3 := &formatErasureV3{}
	if err = json.Unmarshal(b, formatV3); err != nil {
		t.Fatal(err)
	}
	if formatV3.Erasure.This != m.Erasure.Disk {
		t.Fatalf("expected disk uuid: %s, got: %s", m.Erasure.Disk, formatV3.Erasure.This)
	}
	if len(formatV3.Erasure.Sets) != 1 {
		t.Fatalf("expected single set after migrating from v1 to v3, but found %d", len(formatV3.Erasure.Sets))
	}
	if !reflect.DeepEqual(formatV3.Erasure.Sets[0], m.Erasure.JBOD) {
		t.Fatalf("expected disk uuid: %v, got: %v", m.Erasure.JBOD, formatV3.Erasure.Sets[0])
	}

	m = &formatErasureV1{}
	m.Format = "unknown"
	m.Version = formatMetaVersionV1
	m.Erasure.Version = formatErasureVersionV1
	m.Erasure.Disk = mustGetUUID()
	m.Erasure.JBOD = []string{m.Erasure.Disk, mustGetUUID(), mustGetUUID(), mustGetUUID()}

	b, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	if err = ioutil.WriteFile(pathJoin(rootPath, minioMetaBucket, formatConfigFile), b, os.FileMode(0644)); err != nil {
		t.Fatal(err)
	}

	if err = formatErasureMigrate(rootPath); err == nil {
		t.Fatal("Expected to fail with unexpected backend format")
	}

	m = &formatErasureV1{}
	m.Format = formatBackendErasure
	m.Version = formatMetaVersionV1
	m.Erasure.Version = "30"
	m.Erasure.Disk = mustGetUUID()
	m.Erasure.JBOD = []string{m.Erasure.Disk, mustGetUUID(), mustGetUUID(), mustGetUUID()}

	b, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	if err = ioutil.WriteFile(pathJoin(rootPath, minioMetaBucket, formatConfigFile), b, os.FileMode(0644)); err != nil {
		t.Fatal(err)
	}

	if err = formatErasureMigrate(rootPath); err == nil {
		t.Fatal("Expected to fail with unexpected backend format version number")
	}
}

// Tests check format xl value.
func TestCheckFormatErasureValue(t *testing.T) {
	testCases := []struct {
		format  *formatErasureV3
		success bool
	}{
		// Invalid Erasure format version "2".
		{
			&formatErasureV3{
				formatMetaV1: formatMetaV1{
					Version: "2",
					Format:  "Erasure",
				},
				Erasure: struct {
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
		// Invalid Erasure format "Unknown".
		{
			&formatErasureV3{
				formatMetaV1: formatMetaV1{
					Version: "1",
					Format:  "Unknown",
				},
				Erasure: struct {
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
		// Invalid Erasure format version "0".
		{
			&formatErasureV3{
				formatMetaV1: formatMetaV1{
					Version: "1",
					Format:  "Erasure",
				},
				Erasure: struct {
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
		if err := checkFormatErasureValue(testCase.format, nil); err != nil && testCase.success {
			t.Errorf("Test %d: Expected failure %s", i+1, err)
		}
	}
}

// Tests getFormatErasureInQuorum()
func TestGetFormatErasureInQuorumCheck(t *testing.T) {
	setCount := 2
	setDriveCount := 16

	format := newFormatErasureV3(setCount, setDriveCount)
	format.Erasure.DistributionAlgo = formatErasureVersionV2DistributionAlgoV1
	formats := make([]*formatErasureV3, 32)

	for i := 0; i < setCount; i++ {
		for j := 0; j < setDriveCount; j++ {
			newFormat := format.Clone()
			newFormat.Erasure.This = format.Erasure.Sets[i][j]
			formats[i*setDriveCount+j] = newFormat
		}
	}

	// Return a format from list of formats in quorum.
	quorumFormat, err := getFormatErasureInQuorum(formats)
	if err != nil {
		t.Fatal(err)
	}

	// Check if the reference format and input formats are same.
	if err = formatErasureV3Check(quorumFormat, formats[0]); err != nil {
		t.Fatal(err)
	}

	// QuorumFormat has .This field empty on purpose, expect a failure.
	if err = formatErasureV3Check(formats[0], quorumFormat); err == nil {
		t.Fatal("Unexpected success")
	}

	formats[0] = nil
	quorumFormat, err = getFormatErasureInQuorum(formats)
	if err != nil {
		t.Fatal(err)
	}

	badFormat := *quorumFormat
	badFormat.Erasure.Sets = nil
	if err = formatErasureV3Check(quorumFormat, &badFormat); err == nil {
		t.Fatal("Unexpected success")
	}

	badFormatUUID := *quorumFormat
	badFormatUUID.Erasure.Sets[0][0] = "bad-uuid"
	if err = formatErasureV3Check(quorumFormat, &badFormatUUID); err == nil {
		t.Fatal("Unexpected success")
	}

	badFormatSetSize := *quorumFormat
	badFormatSetSize.Erasure.Sets[0] = nil
	if err = formatErasureV3Check(quorumFormat, &badFormatSetSize); err == nil {
		t.Fatal("Unexpected success")
	}

	for i := range formats {
		if i < 17 {
			formats[i] = nil
		}
	}
	if _, err = getFormatErasureInQuorum(formats); err == nil {
		t.Fatal("Unexpected success")
	}
}

// Tests formatErasureGetDeploymentID()
func TestGetErasureID(t *testing.T) {
	setCount := 2
	setDriveCount := 8

	format := newFormatErasureV3(setCount, setDriveCount)
	format.Erasure.DistributionAlgo = formatErasureVersionV2DistributionAlgoV1
	formats := make([]*formatErasureV3, 16)

	for i := 0; i < setCount; i++ {
		for j := 0; j < setDriveCount; j++ {
			newFormat := format.Clone()
			newFormat.Erasure.This = format.Erasure.Sets[i][j]
			formats[i*setDriveCount+j] = newFormat
		}
	}

	// Return a format from list of formats in quorum.
	quorumFormat, err := getFormatErasureInQuorum(formats)
	if err != nil {
		t.Fatal(err)
	}

	// Check if the reference format and input formats are same.
	var id string
	if id, err = formatErasureGetDeploymentID(quorumFormat, formats); err != nil {
		t.Fatal(err)
	}

	if id == "" {
		t.Fatal("ID cannot be empty.")
	}

	formats[0] = nil
	if id, err = formatErasureGetDeploymentID(quorumFormat, formats); err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("ID cannot be empty.")
	}

	formats[1].Erasure.Sets[0][0] = "bad-uuid"
	if id, err = formatErasureGetDeploymentID(quorumFormat, formats); err != nil {
		t.Fatal(err)
	}

	if id == "" {
		t.Fatal("ID cannot be empty.")
	}

	formats[2].ID = "bad-id"
	if _, err = formatErasureGetDeploymentID(quorumFormat, formats); !errors.Is(err, errCorruptedFormat) {
		t.Fatalf("Unexpect error %s", err)
	}
}

// Initialize new format sets.
func TestNewFormatSets(t *testing.T) {
	setCount := 2
	setDriveCount := 16

	format := newFormatErasureV3(setCount, setDriveCount)
	format.Erasure.DistributionAlgo = formatErasureVersionV2DistributionAlgoV1
	formats := make([]*formatErasureV3, 32)
	errs := make([]error, 32)

	for i := 0; i < setCount; i++ {
		for j := 0; j < setDriveCount; j++ {
			newFormat := format.Clone()
			newFormat.Erasure.This = format.Erasure.Sets[i][j]
			formats[i*setDriveCount+j] = newFormat
		}
	}

	quorumFormat, err := getFormatErasureInQuorum(formats)
	if err != nil {
		t.Fatal(err)
	}

	// 16th disk is unformatted.
	errs[15] = errUnformattedDisk

	newFormats := newHealFormatSets(quorumFormat, setCount, setDriveCount, formats, errs)
	if newFormats == nil {
		t.Fatal("Unexpected failure")
	}

	// Check if deployment IDs are preserved.
	for i := range newFormats {
		for j := range newFormats[i] {
			if newFormats[i][j] == nil {
				continue
			}
			if newFormats[i][j].ID != quorumFormat.ID {
				t.Fatal("Deployment id in the new format is lost")
			}
		}
	}
}

func BenchmarkInitStorageDisks256(b *testing.B) {
	benchmarkInitStorageDisksN(b, 256)
}

func BenchmarkInitStorageDisks1024(b *testing.B) {
	benchmarkInitStorageDisksN(b, 1024)
}

func BenchmarkInitStorageDisks2048(b *testing.B) {
	benchmarkInitStorageDisksN(b, 2048)
}

func BenchmarkInitStorageDisksMax(b *testing.B) {
	benchmarkInitStorageDisksN(b, 32*204)
}

func benchmarkInitStorageDisksN(b *testing.B, nDisks int) {
	b.ResetTimer()
	b.ReportAllocs()

	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		b.Fatal(err)
	}
	endpoints := mustGetNewEndpoints(fsDirs...)
	b.RunParallel(func(pb *testing.PB) {
		endpoints := endpoints
		for pb.Next() {
			initStorageDisksWithErrors(endpoints)
		}
	})
}
