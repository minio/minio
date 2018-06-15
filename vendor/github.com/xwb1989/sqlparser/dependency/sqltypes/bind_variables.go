/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/xwb1989/sqlparser/dependency/querypb"
)

// NullBindVariable is a bindvar with NULL value.
var NullBindVariable = &querypb.BindVariable{Type: querypb.Type_NULL_TYPE}

// ValueToProto converts Value to a *querypb.Value.
func ValueToProto(v Value) *querypb.Value {
	return &querypb.Value{Type: v.typ, Value: v.val}
}

// ProtoToValue converts a *querypb.Value to a Value.
func ProtoToValue(v *querypb.Value) Value {
	return MakeTrusted(v.Type, v.Value)
}

// BuildBindVariables builds a map[string]*querypb.BindVariable from a map[string]interface{}.
func BuildBindVariables(in map[string]interface{}) (map[string]*querypb.BindVariable, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make(map[string]*querypb.BindVariable, len(in))
	for k, v := range in {
		bv, err := BuildBindVariable(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", k, err)
		}
		out[k] = bv
	}
	return out, nil
}

// Int32BindVariable converts an int32 to a bind var.
func Int32BindVariable(v int32) *querypb.BindVariable {
	return ValueBindVariable(NewInt32(v))
}

// Int64BindVariable converts an int64 to a bind var.
func Int64BindVariable(v int64) *querypb.BindVariable {
	return ValueBindVariable(NewInt64(v))
}

// Uint64BindVariable converts a uint64 to a bind var.
func Uint64BindVariable(v uint64) *querypb.BindVariable {
	return ValueBindVariable(NewUint64(v))
}

// Float64BindVariable converts a float64 to a bind var.
func Float64BindVariable(v float64) *querypb.BindVariable {
	return ValueBindVariable(NewFloat64(v))
}

// StringBindVariable converts a string to a bind var.
func StringBindVariable(v string) *querypb.BindVariable {
	return ValueBindVariable(NewVarChar(v))
}

// BytesBindVariable converts a []byte to a bind var.
func BytesBindVariable(v []byte) *querypb.BindVariable {
	return &querypb.BindVariable{Type: VarBinary, Value: v}
}

// ValueBindVariable converts a Value to a bind var.
func ValueBindVariable(v Value) *querypb.BindVariable {
	return &querypb.BindVariable{Type: v.typ, Value: v.val}
}

// BuildBindVariable builds a *querypb.BindVariable from a valid input type.
func BuildBindVariable(v interface{}) (*querypb.BindVariable, error) {
	switch v := v.(type) {
	case string:
		return StringBindVariable(v), nil
	case []byte:
		return BytesBindVariable(v), nil
	case int:
		return &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int64:
		return Int64BindVariable(v), nil
	case uint64:
		return Uint64BindVariable(v), nil
	case float64:
		return Float64BindVariable(v), nil
	case nil:
		return NullBindVariable, nil
	case Value:
		return ValueBindVariable(v), nil
	case *querypb.BindVariable:
		return v, nil
	case []interface{}:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			lbv, err := BuildBindVariable(lv)
			if err != nil {
				return nil, err
			}
			values[i].Type = lbv.Type
			values[i].Value = lbv.Value
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []string:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_VARCHAR
			values[i].Value = []byte(lv)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case [][]byte:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_VARBINARY
			values[i].Value = lv
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []int:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_INT64
			values[i].Value = strconv.AppendInt(nil, int64(lv), 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []int64:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_INT64
			values[i].Value = strconv.AppendInt(nil, lv, 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []uint64:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_UINT64
			values[i].Value = strconv.AppendUint(nil, lv, 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []float64:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_FLOAT64
			values[i].Value = strconv.AppendFloat(nil, lv, 'g', -1, 64)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	}
	return nil, fmt.Errorf("type %T not supported as bind var: %v", v, v)
}

// ValidateBindVariables validates a map[string]*querypb.BindVariable.
func ValidateBindVariables(bv map[string]*querypb.BindVariable) error {
	for k, v := range bv {
		if err := ValidateBindVariable(v); err != nil {
			return fmt.Errorf("%s: %v", k, err)
		}
	}
	return nil
}

// ValidateBindVariable returns an error if the bind variable has inconsistent
// fields.
func ValidateBindVariable(bv *querypb.BindVariable) error {
	if bv == nil {
		return errors.New("bind variable is nil")
	}

	if bv.Type == querypb.Type_TUPLE {
		if len(bv.Values) == 0 {
			return errors.New("empty tuple is not allowed")
		}
		for _, val := range bv.Values {
			if val.Type == querypb.Type_TUPLE {
				return errors.New("tuple not allowed inside another tuple")
			}
			if err := ValidateBindVariable(&querypb.BindVariable{Type: val.Type, Value: val.Value}); err != nil {
				return err
			}
		}
		return nil
	}

	// If NewValue succeeds, the value is valid.
	_, err := NewValue(bv.Type, bv.Value)
	return err
}

// BindVariableToValue converts a bind var into a Value.
func BindVariableToValue(bv *querypb.BindVariable) (Value, error) {
	if bv.Type == querypb.Type_TUPLE {
		return NULL, errors.New("cannot convert a TUPLE bind var into a value")
	}
	return MakeTrusted(bv.Type, bv.Value), nil
}

// BindVariablesEqual compares two maps of bind variables.
func BindVariablesEqual(x, y map[string]*querypb.BindVariable) bool {
	return reflect.DeepEqual(&querypb.BoundQuery{BindVariables: x}, &querypb.BoundQuery{BindVariables: y})
}

// CopyBindVariables returns a shallow-copy of the given bindVariables map.
func CopyBindVariables(bindVariables map[string]*querypb.BindVariable) map[string]*querypb.BindVariable {
	result := make(map[string]*querypb.BindVariable, len(bindVariables))
	for key, value := range bindVariables {
		result[key] = value
	}
	return result
}
