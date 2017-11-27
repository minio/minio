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
	"sync"
	"testing"

	"github.com/minio/minio/pkg/errors"
)

func TestHouseKeeping(t *testing.T) {
	fsDirs, err := getRandomDisks(8)
	if err != nil {
		t.Fatalf("Failed to create disks for storage layer <ERROR> %v", err)
	}
	defer removeRoots(fsDirs)

	noSpaceDirs, err := getRandomDisks(8)
	if err != nil {
		t.Fatalf("Failed to create disks for storage layer <ERROR> %v", err)
	}
	defer removeRoots(noSpaceDirs)

	properStorage := []StorageAPI{}
	for _, fsDir := range fsDirs {
		var sd StorageAPI
		sd, err = newPosix(fsDir)
		if err != nil {
			t.Fatalf("Failed to create a local disk-based storage layer <ERROR> %v", err)
		}
		properStorage = append(properStorage, sd)
	}

	noSpaceBackend := []StorageAPI{}
	for _, noSpaceDir := range noSpaceDirs {
		sd, err := newPosix(noSpaceDir)
		if err != nil {
			t.Fatalf("Failed to create a local disk-based storage layer <ERROR> %v", err)
		}
		noSpaceBackend = append(noSpaceBackend, sd)
	}
	noSpaceStorage := prepareNErroredDisks(noSpaceBackend, 5, errDiskFull, t)

	// Create .minio.sys/tmp directory on all disks.
	wg := sync.WaitGroup{}
	errs := make([]error, len(properStorage))
	for i, store := range properStorage {
		wg.Add(1)
		go func(index int, store StorageAPI) {
			defer wg.Done()
			errs[index] = store.MakeVol(minioMetaBucket)
			if errs[index] != nil {
				return
			}
			errs[index] = store.MakeVol(minioMetaTmpBucket)
			if errs[index] != nil {
				return
			}
			errs[index] = store.AppendFile(minioMetaTmpBucket, "hello.txt", []byte("hello"))
		}(i, store)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("Failed to create .minio.sys/tmp directory on disk %v <ERROR> %v",
				properStorage[i], errs[i])
		}
	}

	nilDiskStorage := []StorageAPI{nil, nil, nil, nil, nil, nil, nil, nil}
	testCases := []struct {
		store       []StorageAPI
		expectedErr error
	}{
		{properStorage, nil},
		{noSpaceStorage, StorageFull{}},
		{nilDiskStorage, nil},
	}
	for i, test := range testCases {
		actualErr := errors.Cause(houseKeeping(test.store))
		if actualErr != test.expectedErr {
			t.Errorf("Test %d - actual error is %#v, expected error was %#v",
				i+1, actualErr, test.expectedErr)
		}
	}
}
