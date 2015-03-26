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
	iodineError = New(iodineError, nil)
	iodineError = New(iodineError, nil)
	iodineError = New(iodineError, nil)
	switch typedError := iodineError.(type) {
	case Error:
		{
			if len(typedError.Stack) != 4 {
				t.Fail()
			}
			jsonResult, err := typedError.EmitJSON()
			if err != nil {
				t.Fail()
			}
			var prettyBuffer bytes.Buffer
			json.Indent(&prettyBuffer, jsonResult, "", "  ")
			if prettyBuffer.String() == "" {
				t.Fail()
			}
		}
	default:
		{
			t.Fail()
		}
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
	switch typedError := err.(type) {
	case Error:
		{
			if res, ok := typedError.Stack[0].Data["foo"]; ok {
				if res != "bar" {
					t.Error("global state not set: foo->bar")
				}
			} else {
				t.Fail()
			}
			typedError = New(typedError, map[string]string{"foo2": "bar2"}).(Error)
			if res, ok := typedError.Stack[0].Data["foo"]; ok {
				if res != "bar" {
					t.Error("annotate should not modify previous data entries")
				}
			} else {
				t.Error("annotate should not remove previous data entries")
			}
			if res, ok := typedError.Stack[1].Data["foo"]; ok {
				if res != "bar" {
					t.Error("global state should set value properly in annotate")
				}
			} else {
				t.Error("global state should set key properly in annotate")
			}
			if res, ok := typedError.Stack[1].Data["foo2"]; ok {
				if res != "bar2" {
					//					typedError = Error(typedError, nil).(WrappedError)
					t.Error("foo2 -> bar should be set")
				}
			} else {
				//				typedError = Error(typedError, nil).(WrappedError)
				t.Error("foo2 should be set")
			}
		}
	}
}
