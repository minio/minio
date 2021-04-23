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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v7/pkg/set"
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

	if _, err = file.Seek(offset, io.SeekStart); err != nil {
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
