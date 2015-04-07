/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package file

import (
	"bufio"
	"bytes"
	"os"
	"strings"
	"sync"

	"github.com/minio-io/objectdriver"
)

// fileDriver - file local variables
type fileDriver struct {
	root string
	lock *sync.Mutex
}

// fileMetadata - carries metadata about object
type fileMetadata struct {
	Md5sum      []byte
	ContentType string
}

func appendUniq(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

type bucketDir struct {
	files map[string]os.FileInfo
	root  string
}

func (p *bucketDir) getAllFiles(object string, fl os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if fl.Mode().IsRegular() {
		if strings.HasSuffix(object, "$metadata") {
			return nil
		}
		_p := strings.Split(object, p.root+"/")
		if len(_p) > 1 {
			p.files[_p[1]] = fl
		}
	}
	return nil
}

func delimiter(object, delimiter string) string {
	readBuffer := bytes.NewBufferString(object)
	reader := bufio.NewReader(readBuffer)
	stringReader := strings.NewReader(delimiter)
	delimited, _ := stringReader.ReadByte()
	delimitedStr, _ := reader.ReadString(delimited)
	return delimitedStr
}

type byObjectKey []drivers.ObjectMetadata

// Len
func (b byObjectKey) Len() int { return len(b) }

// Swap
func (b byObjectKey) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// Less
func (b byObjectKey) Less(i, j int) bool { return b[i].Key < b[j].Key }
