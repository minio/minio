/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package ioutil

import (
	goioutil "io/ioutil"
	"os"
	"testing"
)

func TestCloseOnWriter(t *testing.T) {
	writer := WriteOnClose(goioutil.Discard)
	if writer.HasWritten() {
		t.Error("WriteOnCloser must not be marked as HasWritten")
	}
	writer.Write(nil)
	if !writer.HasWritten() {
		t.Error("WriteOnCloser must be marked as HasWritten")
	}

	writer = WriteOnClose(goioutil.Discard)
	writer.Close()
	if !writer.HasWritten() {
		t.Error("WriteOnCloser must be marked as HasWritten")
	}
}

// Test for AppendFile.
func TestAppendFile(t *testing.T) {
	f, err := goioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	name1 := f.Name()
	defer os.Remove(name1)
	f.WriteString("aaaaaaaaaa")
	f.Close()

	f, err = goioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	name2 := f.Name()
	defer os.Remove(name2)
	f.WriteString("bbbbbbbbbb")
	f.Close()

	if err = AppendFile(name1, name2); err != nil {
		t.Error(err)
	}

	b, err := goioutil.ReadFile(name1)
	if err != nil {
		t.Error(err)
	}

	expected := "aaaaaaaaaabbbbbbbbbb"
	if string(b) != expected {
		t.Errorf("AppendFile() failed, expected: %s, got %s", expected, string(b))
	}
}
