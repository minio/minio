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

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"os"
	"testing"
)

func initializeObjLayer() (objLayer ObjectLayer, err error) {
	initNSLock()
	var disks []string
	var d string
	for i := 0; i < 8; i++ {
		d, err = ioutil.TempDir("", "disk")
		if err != nil {
			return nil, err
		}
		disks = append(disks, d)
	}
	objLayer, err = newXLObjects(disks)
	if err != nil {
		return nil, err
	}
	return objLayer, nil
}

func cleanupObjLayer(xl xlObjects) {
	disks := xl.physicalDisks
	for i := range disks {
		os.RemoveAll(disks[i])
	}
}

func failDisks(xl xlObjects, n int) (removedDisks []StorageAPI) {
	removedDisks = make([]StorageAPI, 4)
	copy(removedDisks, xl.storageDisks[4:])
	xl.storageDisks = xl.storageDisks[:4]
	for i := 0; i < n; i++ {
		xl.storageDisks = append(xl.storageDisks, nil)
	}
	return removedDisks
}

func TestRenameObjectWriteQuorum(t *testing.T) {
	var objLayer ObjectLayer
	var err error

	objLayer, err = initializeObjLayer()
	if err != nil {
		t.Fatal(err)
	}

	xl := objLayer.(xlObjects)

	// cleaning up of temporary test directories
	defer cleanupObjLayer(xl)

	err = objLayer.MakeBucket("bucket1")
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.NewReader([]byte("hello"))
	_, err = objLayer.PutObject("bucket1", "obj1", 5, data, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate failure of disks
	removedDisks := failDisks(xl, 4)
	if err = xl.renameObject("bucket1", "obj1", ".minio", "obj1"); err != errXLWriteQuorum {
		t.Fatal(err)
	}

	// Restoring the failed disks back
	xl.storageDisks = append(xl.storageDisks[:4], removedDisks...)

	// With all disks back online, renameObject should succeed.
	if err = xl.renameObject("bucket1", "obj1", ".minio", "obj1"); err != nil {
		t.Fatal(err)
	}

	// ... so should renaming back (succeed).
	if err = xl.renameObject(".minio", "obj1", "bucket1", "obj1"); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatPutObjectPart(t *testing.T) {
	var objLayer ObjectLayer
	var err error

	objLayer, err = initializeObjLayer()
	if err != nil {
		t.Fatal(err)
	}

	xl := objLayer.(xlObjects)

	defer cleanupObjLayer(xl)

	// cleaning up of temporary test directories
	defer cleanupObjLayer(xl)

	err = objLayer.MakeBucket("bucket1")
	if err != nil {
		t.Fatal(err)
	}

	uploadID, err := objLayer.NewMultipartUpload("bucket1", "mpartObj1", nil)
	if err != nil {
		t.Fatal(err)
	}
	fiveMBBytes := bytes.Repeat([]byte("a"), 5*1024*124)
	md5Writer := md5.New()
	md5Writer.Write(fiveMBBytes)
	md5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	_, err = objLayer.PutObjectPart("bucket1", "mpartObj1", uploadID, 1, 5*1024*1024, bytes.NewReader(fiveMBBytes), md5Hex)
	if err != nil {
		t.Fatal(err)
	}
	// PutObjectPart should succeed even if part already exists. ref: https://github.com/minio/minio/issues/1930
	_, err = objLayer.PutObjectPart("bucket1", "mpartObj1", uploadID, 1, 5*1024*1024, bytes.NewReader(fiveMBBytes), md5Hex)
	if err != nil {
		t.Fatal(err)
	}

}
