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
	"errors"
	"log"
	"testing"
)

func TestIodine(t *testing.T) {
	iodineError := New(errors.New("Hello"), nil)
	iodineError.Annotate(nil)
	iodineError.Annotate(nil)
	iodineError.Annotate(nil)
	if len(iodineError.Stack) != 4 {
		t.Fail()
	}
	jsonResult, err := iodineError.EmitJSON()
	if err != nil {
		t.Fail()
	}
	var prettyBuffer bytes.Buffer
	json.Indent(&prettyBuffer, jsonResult, "", "  ")
	log.Println(string(prettyBuffer.Bytes()))
	log.Println(iodineError.EmitHumanReadable())
}
