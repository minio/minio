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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v6/pkg/set"
	parquet "github.com/minio/minio/pkg/s3select/internal/parquet-go"
)

func getReader(name string, offset int64, length int64) (io.ReadCloser, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if offset < 0 {
		offset = fi.Size() + offset
	}

	if _, err = file.Seek(offset, os.SEEK_SET); err != nil {
		return nil, err
	}

	return file, nil
}

func printUsage() {
	progName := path.Base(os.Args[0])
	fmt.Printf("Usage: %v PARQUET-FILE [COLUMN...]\n", progName)
	fmt.Println()
	fmt.Printf("Examples:\n")
	fmt.Printf("# Convert all columns to JSON\n")
	fmt.Printf("$ %v example.parquet\n", progName)
	fmt.Println()
	fmt.Printf("# Convert specific columns to JSON\n")
	fmt.Printf("$ %v example.par firstname dob\n", progName)
	fmt.Println()
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(-1)
	}

	name := os.Args[1]
	ext := path.Ext(name)
	jsonFilename := name + ".json"
	if ext == ".parquet" || ext == ".par" {
		jsonFilename = strings.TrimSuffix(name, ext) + ".json"
	}

	columns := set.CreateStringSet(os.Args[2:]...)
	if len(columns) == 0 {
		columns = nil
	}

	file, err := parquet.NewReader(
		func(offset, length int64) (io.ReadCloser, error) {
			return getReader(name, offset, length)
		},
		columns,
	)
	if err != nil {
		fmt.Printf("%v: %v\n", name, err)
		os.Exit(1)
	}

	defer file.Close()

	jsonFile, err := os.OpenFile(jsonFilename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Printf("%v: %v\n", jsonFilename, err)
		os.Exit(1)
	}

	defer jsonFile.Close()

	for {
		record, err := file.Read()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("%v: %v\n", name, err)
				os.Exit(1)
			}

			break
		}

		data, err := json.Marshal(record)
		if err != nil {
			fmt.Printf("%v: %v\n", name, err)
			os.Exit(1)
		}
		data = append(data, byte('\n'))

		if _, err = jsonFile.Write(data); err != nil {
			fmt.Printf("%v: %v\n", jsonFilename, err)
			os.Exit(1)
		}
	}
}
