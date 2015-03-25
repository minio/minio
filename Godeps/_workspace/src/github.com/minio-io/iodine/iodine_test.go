/*
 * Iodine, (C) 2015 Minio, Inc.
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
	if prettyBuffer.String() == "" {
		t.Fail()
	}
}

func TestState(t *testing.T) {
	SetGlobalState("hello", "world")
	result := GetGlobalStateKey("hello")
	if result != "world" {
		t.Error("global state not set: hello->world")
		t.Fail()
	}
	ClearGlobalState()
	if len(GetGlobalState()) != 0 {
		t.Fail()
	}
	SetGlobalState("foo", "bar")
	err := New(errors.New("a simple error"), nil)
	if res, ok := err.Stack[0].Data["foo"]; ok {
		if res != "bar" {
			t.Error("global state not set: foo->bar")
		}
	} else {
		t.Fail()
	}
	err.Annotate(map[string]string{"foo2": "bar2"})
	if res, ok := err.Stack[0].Data["foo"]; ok {
		if res != "bar" {
			t.Error("annotate should not modify previous data entries")
		}
	} else {
		t.Error("annotate should not remove previous data entries")
	}
	if res, ok := err.Stack[1].Data["foo"]; ok {
		if res != "bar" {
			t.Error("global state should set value properly in annotate")
		}
	} else {
		t.Error("global state should set key properly in annotate")
	}
	if res, ok := err.Stack[1].Data["foo2"]; ok {
		if res != "bar2" {
			err.Annotate(nil)
			t.Error("foo2 -> bar should be set")
		}
	} else {
		err.Annotate(nil)
		t.Error("foo2 should be set")
	}
}
