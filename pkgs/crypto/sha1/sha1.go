/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

// +build amd64

package sha1

// #include <stdio.h>
// #include <stdint.h>
// void sha1_transform(int32_t *hash, const char* input, size_t num_blocks);
import "C"
import (
	gosha1 "crypto/sha1"
	"errors"
	"io"
	"unsafe"

	"github.com/minio-io/minio/pkgs/cpu"
)

const (
	SHA1_BLOCKSIZE  = 64
	SHA1_DIGESTSIZE = 20
)

func Sha1(buffer []byte) ([]int32, error) {
	if !cpu.HasAVX2() {
		// Unsupported processor but do not error out tests
		return []int32{0}, nil
	}

	var shbuf []int32
	var cbuffer *C.char

	shbuf = make([]int32, SHA1_DIGESTSIZE)
	var length = len(buffer)
	if length == 0 {
		return []int32{0}, errors.New("Invalid input")
	}

	rem := length % SHA1_BLOCKSIZE
	padded_len := length

	if rem > 0 {
		padded_len = length + (SHA1_BLOCKSIZE - rem)
	}

	rounds := padded_len / SHA1_BLOCKSIZE
	pad := padded_len - length
	if pad > 0 {
		s := make([]byte, pad)
		// Expand with new padded blocks to the byte array
		buffer = append(buffer, s...)
	}

	cshbuf := (*C.int32_t)(unsafe.Pointer(&shbuf[0]))
	cbuffer = (*C.char)(unsafe.Pointer(&buffer[0]))
	C.sha1_transform(cshbuf, cbuffer, C.size_t(rounds))

	return shbuf, nil
}

func Sum(reader io.Reader) ([]byte, error) {
	hash := gosha1.New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		hash.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return hash.Sum(nil), nil
}
