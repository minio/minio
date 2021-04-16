/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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

package zipindex_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/minio/minio/pkg/ioutil"
	"github.com/minio/minio/pkg/zipindex"
)

func ExampleReadDir() {
	b, err := ioutil.ReadFile("testdata/big.zip")
	if err != nil {
		panic(err)
	}
	// We only need the end of the file to parse the directory.
	// Usually this should be at least 64K on initial try.
	sz := 10 << 10
	var files zipindex.Files
	for {
		files, err = zipindex.ReadDir(b[len(b)-sz:], int64(len(b)))
		if err == nil {
			fmt.Printf("Got %d files\n", len(files))
			break
		}
		var terr zipindex.ErrNeedMoreData
		if errors.As(err, &terr) {
			if terr.FromEnd > 1<<20 {
				panic("we will only provide max 1MB data")
			}
			sz = int(terr.FromEnd)
			fmt.Printf("Retrying with %d bytes at the end of file\n", sz)
		} else {
			// Unable to parse...
			panic(err)
		}
	}

	fmt.Printf("First file: %+v", files[0])
	// Output:
	// Retrying with 57912 bytes at the end of file
	// Got 1000 files
	// First file: {Name:file-0.txt CompressedSize64:1 UncompressedSize64:1 Offset:0 CRC32:4108050209 Method:0}
}

func ExampleDeserializeFiles() {
	exitOnErr := func(err error) {
		if err != nil {
			log.Fatalln(err)
		}
	}

	b, err := ioutil.ReadFile("testdata/big.zip")
	exitOnErr(err)
	// We only need the end of the file to parse the directory.
	// Usually this should be at least 64K on initial try.
	sz := 64 << 10
	var files zipindex.Files
	files, err = zipindex.ReadDir(b[len(b)-sz:], int64(len(b)))
	exitOnErr(err)

	// Serialize files to binary.
	serialized, err := files.Serialize()
	exitOnErr(err)

	// Deserialize the content.
	files, err = zipindex.DeserializeFiles(serialized)
	exitOnErr(err)

	file := files.Find("file-10.txt")
	fmt.Printf("Reading file: %+v\n", *file)

	// Create a reader with entire zip file...
	rs := bytes.NewReader(b)
	// Seek to the file offset.
	_, err = rs.Seek(file.Offset, io.SeekStart)
	exitOnErr(err)

	// Provide the forwarded reader..
	rc, err := file.Open(rs)
	exitOnErr(err)
	defer rc.Close()

	// Read the zip file content.
	content, err := io.ReadAll(rc)
	exitOnErr(err)

	fmt.Printf("File content is '%s'\n", string(content))

	// Output:
	// Reading file: {Name:file-10.txt CompressedSize64:2 UncompressedSize64:2 Offset:410 CRC32:2707236321 Method:0}
	// File content is '10'
}
