// +build ignore

//
// Mini Object Storage, (C) 2015 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	sha256intel "github.com/minio-io/minio/pkg/utils/crypto/sha256"
)

func SumIntel(reader io.Reader) ([]byte, error) {
	h := sha256intel.New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		h.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return h.Sum(nil), nil
}

func Sum(reader io.Reader) ([]byte, error) {
	k := sha256.New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		k.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return k.Sum(nil), nil
}

func main() {
	fmt.Println("-- start")

	file1, _ := os.Open("filename1")
	defer file1.Close()
	stark := time.Now()
	sum, _ := Sum(file1)
	endk := time.Since(stark)

	file2, _ := os.Open("filename2")
	defer file2.Close()
	starth := time.Now()
	sumSSE, _ := SumIntel(file2)
	endh := time.Since(starth)

	fmt.Println("std(", endk, ")", "ssse3(", endh, ")")
	fmt.Println(hex.EncodeToString(sum), hex.EncodeToString(sumSSE))
}
