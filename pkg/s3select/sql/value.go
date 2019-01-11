/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
)

// Value - represents any primitive value of bool, int, float, string and time.
type Value struct {
	value     interface{}
	valueType Type
}

// String - represents value as string.
func (value *Value) String() string {
	if value.value == nil {
		if value.valueType == Null {
			return "NULL"
		}

		return "<nil>"
	}

	switch value.valueType {
	case String:
		return fmt.Sprintf("'%v'", value.value)
	case Array:
		var valueStrings []string
		for _, v := range value.value.([]*Value) {
			valueStrings = append(valueStrings, fmt.Sprintf("%v", v))
		}
		return fmt.Sprintf("(%v)", strings.Join(valueStrings, ","))
	}

	return fmt.Sprintf("%v", value.value)
}

// CSVString - encodes to CSV string.
func (value *Value) CSVString() string {
	if value.valueType == Null {
		return ""
	}

	return fmt.Sprintf("%v", value.value)
}

// MarshalJSON - encodes to JSON data.
func (value *Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(value.value)
}

// NullValue - returns underlying null value. It panics if value is not null type.
func (value *Value) NullValue() *struct{} {
	if value.valueType == Null {
		return nil
	}

	panic(fmt.Sprintf("requested bool value but found %T type", value.value))
}

// BoolValue - returns underlying bool value. It panics if value is not Bool type.
func (value *Value) BoolValue() bool {
	if value.valueType == Bool {
		return value.value.(bool)
	}

	panic(fmt.Sprintf("requested bool value but found %T type", value.value))
}

// IntValue - returns underlying int value. It panics if value is not Int type.
func (value *Value) IntValue() int64 {
	if value.valueType == Int {
		return value.value.(int64)
	}

	panic(fmt.Sprintf("requested int value but found %T type", value.value))
}

// FloatValue - returns underlying int/float value as float64. It panics if value is not Int/Float type.
func (value *Value) FloatValue() float64 {
	switch value.valueType {
	case Int:
		return float64(value.value.(int64))
	case Float:
		return value.value.(float64)
	}

	panic(fmt.Sprintf("requested float value but found %T type", value.value))
}

// StringValue - returns underlying string value. It panics if value is not String type.
func (value *Value) StringValue() string {
	if value.valueType == String {
		return value.value.(string)
	}

	panic(fmt.Sprintf("requested string value but found %T type", value.value))
}

// TimeValue - returns underlying time value. It panics if value is not Timestamp type.
func (value *Value) TimeValue() time.Time {
	if value.valueType == Timestamp {
		return value.value.(time.Time)
	}

	panic(fmt.Sprintf("requested time value but found %T type", value.value))
}

// ArrayValue - returns underlying value array. It panics if value is not Array type.
func (value *Value) ArrayValue() []*Value {
	if value.valueType == Array {
		return value.value.([]*Value)
	}

	panic(fmt.Sprintf("requested array value but found %T type", value.value))
}

func (value *Value) recordValue() Record {
	if value.valueType == record {
		return value.value.(Record)
	}

	panic(fmt.Sprintf("requested record value but found %T type", value.value))
}

// Type - returns value type.
func (value *Value) Type() Type {
	return value.valueType
}

// Value - returns underneath value interface.
func (value *Value) Value() interface{} {
	return value.value
}

// NewNull - creates new null value.
func NewNull() *Value {
	return &Value{nil, Null}
}

// NewBool - creates new Bool value of b.
func NewBool(b bool) *Value {
	return &Value{b, Bool}
}

// NewInt - creates new Int value of i.
func NewInt(i int64) *Value {
	return &Value{i, Int}
}

// NewFloat - creates new Float value of f.
func NewFloat(f float64) *Value {
	return &Value{f, Float}
}

// NewString - creates new Sring value of s.
func NewString(s string) *Value {
	return &Value{s, String}
}

// NewTime - creates new Time value of t.
func NewTime(t time.Time) *Value {
	return &Value{t, Timestamp}
}

// NewArray - creates new Array value of values.
func NewArray(values []*Value) *Value {
	return &Value{values, Array}
}

func newRecordValue(r Record) *Value {
	return &Value{r, record}
}

// NewValue - creates new Value from SQLVal v.
func NewValue(v *sqlparser.SQLVal) (*Value, error) {
	switch v.Type {
	case sqlparser.StrVal:
		return NewString(string(v.Val)), nil
	case sqlparser.IntVal:
		i64, err := strconv.ParseInt(string(v.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return NewInt(i64), nil
	case sqlparser.FloatVal:
		f64, err := strconv.ParseFloat(string(v.Val), 64)
		if err != nil {
			return nil, err
		}
		return NewFloat(f64), nil
	case sqlparser.HexNum: // represented as 0xDD
		i64, err := strconv.ParseInt(string(v.Val), 16, 64)
		if err != nil {
			return nil, err
		}
		return NewInt(i64), nil
	case sqlparser.HexVal: // represented as X'0DD'
		i64, err := strconv.ParseInt(string(v.Val), 16, 64)
		if err != nil {
			return nil, err
		}
		return NewInt(i64), nil
	case sqlparser.BitVal: // represented as B'00'
		i64, err := strconv.ParseInt(string(v.Val), 2, 64)
		if err != nil {
			return nil, err
		}
		return NewInt(i64), nil
	case sqlparser.ValArg:
		// FIXME: the format is unknown and not sure how to handle it.
	}

	return nil, fmt.Errorf("unknown SQL value %v; %v ", v, v.Type)
}
