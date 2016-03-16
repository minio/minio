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

package fs

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func BenchmarkGetObject(b *testing.B) {
	// Make a temporary directory to use as the filesystem.
	directory, e := ioutil.TempDir("", "minio-benchmark-getobject")
	if e != nil {
		b.Fatal(e)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		b.Fatal(err)
	}

	// Make a bucket and put in a few objects.
	err = filesystem.MakeBucket("bucket")
	if err != nil {
		b.Fatal(err)
	}

	text := "Jack and Jill went up the hill / To fetch a pail of water."
	hasher := md5.New()
	hasher.Write([]byte(text))
	sum := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	for i := 0; i < 10; i++ {
		_, err = filesystem.CreateObject("bucket", "object"+strconv.Itoa(i), sum, int64(len(text)), bytes.NewBufferString(text), nil)
		if err != nil {
			b.Fatal(err)
		}
	}

	var w bytes.Buffer

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n, err := filesystem.GetObject(&w, "bucket", "object"+strconv.Itoa(i%10), 0, 0)
		if err != nil {
			b.Error(err)
		}
		if n != int64(len(text)) {
			b.Errorf("GetObject returned incorrect length %d (should be %d)\n", n, int64(len(text)))
		}
	}
}
