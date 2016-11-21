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
	"bytes"
	"net/rpc"
	"reflect"
	"testing"
)

// Tests retry storage.
func TestRetryStorage(t *testing.T) {
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(root)

	originalStorageDisks, disks := prepareXLStorageDisks(t)
	defer removeRoots(disks)

	var storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	// Validate all the conditions for retrying calls.

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		err = disk.Init()
		if err != rpc.ErrShutdown {
			t.Fatal("Expected rpc.ErrShutdown, got", err)
		}
	}

	for _, disk := range storageDisks {
		_, err = disk.DiskInfo()
		if err != nil {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if err = disk.MakeVol("existent"); err != nil {
			t.Fatal(err)
		}
		if _, err = disk.StatVol("existent"); err == errVolumeNotFound {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if _, err = disk.StatVol("existent"); err == errVolumeNotFound {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if _, err = disk.ListVols(); err != nil {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if err = disk.DeleteVol("existent"); err != nil {
			t.Fatal(err)
		}
		if str := disk.String(); str == "" {
			t.Fatal("String method for disk cannot be empty.")
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if err = disk.MakeVol("existent"); err != nil {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if err = disk.PrepareFile("existent", "path", 10); err != nil {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if err = disk.AppendFile("existent", "path", []byte("Hello, World")); err != nil {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		var buf1 []byte
		if buf1, err = disk.ReadAll("existent", "path"); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf1, []byte("Hello, World")) {
			t.Fatalf("Expected `Hello, World`, got %s", string(buf1))
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		var buf2 = make([]byte, 5)
		var n int64
		if n, err = disk.ReadFile("existent", "path", 7, buf2); err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("Expected 5, got %d", n)
		}
		if !bytes.Equal(buf2, []byte("World")) {
			t.Fatalf("Expected `World`, got %s", string(buf2))
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if err = disk.RenameFile("existent", "path", "existent", "new-path"); err != nil {
			t.Fatal(err)
		}
		if _, err = disk.StatFile("existent", "new-path"); err != nil {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if _, err = disk.StatFile("existent", "new-path"); err != nil {
			t.Fatal(err)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		var entries []string
		if entries, err = disk.ListDir("existent", ""); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(entries, []string{"new-path"}) {
			t.Fatalf("Expected []string{\"new-path\"}, got %s", entries)
		}
	}

	storageDisks = make([]StorageAPI, len(originalStorageDisks))
	for i := range originalStorageDisks {
		retryDisk, ok := originalStorageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		storageDisks[i] = &retryStorage{newNaughtyDisk(retryDisk, map[int]error{
			1: rpc.ErrShutdown,
		}, nil)}
	}

	for _, disk := range storageDisks {
		if err = disk.DeleteFile("existent", "new-path"); err != nil {
			t.Fatal(err)
		}
		if err = disk.DeleteVol("existent"); err != nil {
			t.Fatal(err)
		}
	}
}
