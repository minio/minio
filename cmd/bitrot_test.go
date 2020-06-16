/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func testBitrotReaderWriterAlgo(t *testing.T, bitrotAlgo BitrotAlgorithm) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	volume := "testvol"
	filePath := "testfile"

	disk, err := newXLStorage(tmpDir, "")
	if err != nil {
		t.Fatal(err)
	}

	disk.MakeVol(volume)

	writer := newBitrotWriter(disk, volume, filePath, 35, bitrotAlgo, 10)

	_, err = writer.Write([]byte("aaaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = writer.Write([]byte("aaaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = writer.Write([]byte("aaaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = writer.Write([]byte("aaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	writer.(io.Closer).Close()

	reader := newBitrotReader(disk, volume, filePath, 35, bitrotAlgo, bitrotWriterSum(writer), 10)
	b := make([]byte, 10)
	if _, err = reader.ReadAt(b, 0); err != nil {
		log.Fatal(err)
	}
	if _, err = reader.ReadAt(b, 10); err != nil {
		log.Fatal(err)
	}
	if _, err = reader.ReadAt(b, 20); err != nil {
		log.Fatal(err)
	}
	if _, err = reader.ReadAt(b[:5], 30); err != nil {
		log.Fatal(err)
	}
}

func TestAllBitrotAlgorithms(t *testing.T) {
	for bitrotAlgo := range bitrotAlgorithms {
		testBitrotReaderWriterAlgo(t, bitrotAlgo)
	}
}
