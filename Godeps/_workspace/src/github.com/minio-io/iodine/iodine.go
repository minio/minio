/*
 * Iodine, (C) 2014 Minio, Inc.
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

package iodine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
)

type Error struct {
	EmbeddedError error `json:"-"`
	ErrorMessage  string

	Stack []StackEntry
}

type StackEntry struct {
	Host string
	File string
	Line int
	Data map[string]string
}

func Wrap(err error, data map[string]string) *Error {
	entry := createStackEntry()
	for k, v := range data {
		entry.Data[k] = v
	}
	return &Error{
		EmbeddedError: err,
		ErrorMessage:  err.Error(),
		Stack:         []StackEntry{entry},
	}
}

func createStackEntry() StackEntry {
	host, _ := os.Hostname()
	_, file, line, _ := runtime.Caller(2)
	entry := StackEntry{
		Host: host,
		File: file,
		Line: line,
		Data: make(map[string]string),
	}
	return entry
}

func (err *Error) Annotate(info map[string]string) *Error {
	data := make(map[string]string)
	if info != nil {
		for k, v := range info {
			data[k] = v
		}
	}
	entry := createStackEntry()
	err.Stack = append(err.Stack, entry)
	return err
}

func (err Error) EmitJson() ([]byte, error) {
	return json.Marshal(err)
}

func (err Error) EmitHumanReadable() string {
	var errorBuffer bytes.Buffer
	fmt.Fprintln(&errorBuffer, err.Error())
	for i, entry := range err.Stack {
		fmt.Fprintln(&errorBuffer, "-", i, entry.Host+":"+entry.File+":"+strconv.Itoa(entry.Line), entry.Data)
	}
	return string(errorBuffer.Bytes())
}

func (err Error) Error() string {
	return err.EmbeddedError.Error()
}
