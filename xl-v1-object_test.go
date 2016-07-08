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
	"testing"
)

func TestRepeatPutObjectPart(t *testing.T) {
	var objLayer ObjectLayer
	var disks []string
	var err error

	objLayer, disks, err = getXLObjectLayer()
	if err != nil {
		t.Fatal(err)
	}

	// cleaning up of temporary test directories
	defer removeRoots(disks)

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
