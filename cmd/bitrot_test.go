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
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestBitrotReaderWriter(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	volume := "testvol"
	filePath := "testfile"

	disk, err := newPosix(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	disk.MakeVol(volume)

	writer := newBitrotWriter(disk, volume, filePath, HighwayHash256)

	err = writer.Append([]byte("aaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	err = writer.Append([]byte("a"))
	if err != nil {
		log.Fatal(err)
	}
	err = writer.Append([]byte("aaaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	err = writer.Append([]byte("aaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	err = writer.Append([]byte("aaaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}

	reader := newBitrotReader(disk, volume, filePath, HighwayHash256, 35, writer.Sum())

	if _, err = reader.ReadChunk(0, 35); err != nil {
		log.Fatal(err)
	}
}
