// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package sql

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	errArithMismatchedTypes = errors.New("cannot perform arithmetic on mismatched types")
	errArithInvalidOperator = errors.New("invalid arithmetic operator")
	errArithDivideByZero    = errors.New("cannot divide by 0")

	errCmpMismatchedTypes     = errors.New("cannot compare values of different types")
	errCmpInvalidBoolOperator = errors.New("invalid comparison operator for boolean arguments")
)

// Value represents a value of restricted type reduced from an
// expression represented by an ASTNode. Only one of the fields is
// non-nil.
//
// In cases where we are fetching data from a data source (like csv),
// the type may not be determined yet. In these cases, a byte-slice is
// used.
type Value struct {
	value interface{}
}

// MarshalJSON provides json marshaling of values.
func (v Value) MarshalJSON() ([]byte, error) {
	if b, ok := v.ToBytes(); ok {
		return b, nil
	}
	return json.Marshal(v.value)
}

// GetTypeString returns a string representation for vType
func (v Value) GetTypeString() string {
	switch v.value.(type) {
	case nil:
		return "NULL"
	case bool:
		return "BOOL"
	case string:
		return "STRING"
	case int64:
		return "INT"
	case float64:
		return "FLOAT"
	case time.Time:
		return "TIMESTAMP"
	case []byte:
		return "BYTES"
	case []Value:
		return "ARRAY"
	}
	return "--"
}

// Repr returns a string representation of value.
func (v Value) Repr() string {
	switch x := v.value.(type) {
	case nil:
		return ":NULL"
	case bool, int64, float64:
		return fmt.Sprintf("%v:%s", v.value, v.GetTypeString())
	case time.Time:
		return fmt.Sprintf("%s:TIMESTAMP", x)
	case string:
		return fmt.Sprintf("\"%s\":%s", x, v.GetTypeString())
	case []byte:
		return fmt.Sprintf("\"%s\":BYTES", string(x))
	case []Value:
		var s strings.Builder
		s.WriteByte('[')
		for i, v := range x {
			s.WriteString(v.Repr())
			if i < len(x)-1 {
				s.WriteByte(',')
			}
		}
		s.WriteString("]:ARRAY")
		return s.String()
	default:
		return fmt.Sprintf("%v:INVALID", v.value)
	}
}

// FromFloat creates a Value from a number
func FromFloat(f float64) *Value {
	return &Value{value: f}
}

// FromInt creates a Value from an int
func FromInt(f int64) *Value {
	return &Value{value: f}
}

// FromString creates a Value from a string
func FromString(str string) *Value {
	return &Value{value: str}
}

// FromBool creates a Value from a bool
func FromBool(b bool) *Value {
	return &Value{value: b}
}

// FromTimestamp creates a Value from a timestamp
func FromTimestamp(t time.Time) *Value {
	return &Value{value: t}
}

// FromNull creates a Value with Null value
func FromNull() *Value {
	return &Value{value: nil}
}

// FromBytes creates a Value from a []byte
func FromBytes(b []byte) *Value {
	return &Value{value: b}
}

// FromArray creates a Value from an array of values.
func FromArray(a []Value) *Value {
	return &Value{value: a}
}

// ToFloat works for int and float values
func (v Value) ToFloat() (val float64, ok bool) {
	switch x := v.value.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	}
	return 0, false
}

// ToInt returns the value if int.
func (v Value) ToInt() (val int64, ok bool) {
	val, ok = v.value.(int64)
	return
}

// ToString returns the value if string.
func (v Value) ToString() (val string, ok bool) {
	val, ok = v.value.(string)
	return
}

// Equals returns whether the values strictly match.
// Both type and value must match.
func (v Value) Equals(b Value) (ok bool) {
	if !v.SameTypeAs(b) {
		return false
	}
	return reflect.DeepEqual(v.value, b.value)
}

// SameTypeAs return whether the two types are strictly the same.
func (v Value) SameTypeAs(b Value) (ok bool) {
	switch v.value.(type) {
	case bool:
		_, ok = b.value.(bool)
	case string:
		_, ok = b.value.(string)
	case int64:
		_, ok = b.value.(int64)
	case float64:
		_, ok = b.value.(float64)
	case time.Time:
		_, ok = b.value.(time.Time)
	case []byte:
		_, ok = b.value.([]byte)
	case []Value:
		_, ok = b.value.([]Value)
	default:
		ok = reflect.TypeOf(v.value) == reflect.TypeOf(b.value)
	}
	return ok
}

// ToBool returns the bool value; second return value refers to if the bool
// conversion succeeded.
func (v Value) ToBool() (val bool, ok bool) {
	val, ok = v.value.(bool)
	return
}

// ToTimestamp returns the timestamp value if present.
func (v Value) ToTimestamp() (t time.Time, ok bool) {
	t, ok = v.value.(time.Time)
	return
}

// ToBytes returns the value if byte-slice.
func (v Value) ToBytes() (val []byte, ok bool) {
	val, ok = v.value.([]byte)
	return
}

// ToArray returns the value if it is a slice of values.
func (v Value) ToArray() (val []Value, ok bool) {
	val, ok = v.value.([]Value)
	return
}

// IsNull - checks if value is missing.
func (v Value) IsNull() bool {
	switch v.value.(type) {
	case nil:
		return true
	}
	return false
}

// IsArray returns whether the value is an array.
func (v Value) IsArray() (ok bool) {
	_, ok = v.value.([]Value)
	return ok
}

func (v Value) isNumeric() bool {
	switch v.value.(type) {
	case int64, float64:
		return true
	}
	return false
}

// setters used internally to mutate values

func (v *Value) setInt(i int64) {
	v.value = i
}

func (v *Value) setFloat(f float64) {
	v.value = f
}

func (v *Value) setString(s string) {
	v.value = s
}

func (v *Value) setBool(b bool) {
	v.value = b
}

func (v *Value) setTimestamp(t time.Time) {
	v.value = t
}

func (v Value) String() string {
	return fmt.Sprintf("%#v", v.value)
}

// CSVString - convert to string for CSV serialization
func (v Value) CSVString() string {
	switch x := v.value.(type) {
	case nil:
		return ""
	case bool:
		if x {
			return "true"
		}
		return "false"
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		return FormatSQLTimestamp(x)
	case []byte:
		return string(x)
	case []Value:
		b, _ := json.Marshal(x)
		return string(b)

	default:
		return "CSV serialization not implemented for this type"
	}
}

// negate negates a numeric value
func (v *Value) negate() {
	switch x := v.value.(type) {
	case float64:
		v.value = -x
	case int64:
		v.value = -x
	}
}

// Value comparison functions: we do not expose them outside the
// module. Logical operators "<", ">", ">=", "<=" work on strings and
// numbers. Equality operators "=", "!=" work on strings,
// numbers and booleans.

// Supported comparison operators
const (
	opLt   = "<"
	opLte  = "<="
	opGt   = ">"
	opGte  = ">="
	opEq   = "="
	opIneq = "!="
)

// InferBytesType will attempt to infer the data type of bytes.
// Will fail if value type is not bytes or it would result in invalid utf8.
// ORDER: int, float, bool, JSON (object or array), timestamp, string
// If the content is valid JSON, the type will still be bytes.
func (v *Value) InferBytesType() (err error) {
	b, ok := v.ToBytes()
	if !ok {
		return fmt.Errorf("InferByteType: Input is not bytes, but %v", v.GetTypeString())
	}

	// Check for numeric inference
	if x, ok := v.bytesToInt(); ok {
		v.setInt(x)
		return nil
	}
	if x, ok := v.bytesToFloat(); ok {
		v.setFloat(x)
		return nil
	}
	if x, ok := v.bytesToBool(); ok {
		v.setBool(x)
		return nil
	}

	asString := strings.TrimSpace(v.bytesToString())
	if len(b) > 0 &&
		(strings.HasPrefix(asString, "{") || strings.HasPrefix(asString, "[")) {
		return nil
	}

	if t, err := parseSQLTimestamp(asString); err == nil {
		v.setTimestamp(t)
		return nil
	}
	if !utf8.Valid(b) {
		return errors.New("value is not valid utf-8")
	}
	// Fallback to string
	v.setString(asString)
	return
}

// When numeric types are compared, type promotions could happen. If
// values do not have types (e.g. when reading from CSV), for
// comparison operations, automatic type conversion happens by trying
// to check if the value is a number (first an integer, then a float),
// and falling back to string.
func (v *Value) compareOp(op string, a *Value) (res bool, err error) {
	if !isValidComparisonOperator(op) {
		return false, errArithInvalidOperator
	}

	// Check if type conversion/inference is needed - it is needed
	// if the Value is a byte-slice.
	err = inferTypesForCmp(v, a)
	if err != nil {
		return false, err
	}

	// Check if either is nil
	if v.IsNull() || a.IsNull() {
		// If one is, both must be.
		return boolCompare(op, v.IsNull(), a.IsNull())
	}

	// Check array values
	aArr, aOK := a.ToArray()
	vArr, vOK := v.ToArray()
	if aOK && vOK {
		return arrayCompare(op, aArr, vArr)
	}

	isNumeric := v.isNumeric() && a.isNumeric()
	if isNumeric {
		intV, ok1i := v.ToInt()
		intA, ok2i := a.ToInt()
		if ok1i && ok2i {
			return intCompare(op, intV, intA), nil
		}

		// If both values are numeric, then at least one is
		// float since we got here, so we convert.
		flV, _ := v.ToFloat()
		flA, _ := a.ToFloat()
		return floatCompare(op, flV, flA), nil
	}

	strV, ok1s := v.ToString()
	strA, ok2s := a.ToString()
	if ok1s && ok2s {
		return stringCompare(op, strV, strA), nil
	}

	boolV, ok1b := v.ToBool()
	boolA, ok2b := a.ToBool()
	if ok1b && ok2b {
		return boolCompare(op, boolV, boolA)
	}

	timestampV, ok1t := v.ToTimestamp()
	timestampA, ok2t := a.ToTimestamp()
	if ok1t && ok2t {
		return timestampCompare(op, timestampV, timestampA), nil
	}

	// Types cannot be compared, they do not match.
	switch op {
	case opEq:
		return false, nil
	case opIneq:
		return true, nil
	}
	return false, errCmpInvalidBoolOperator
}

func inferTypesForCmp(a *Value, b *Value) error {
	_, okA := a.ToBytes()
	_, okB := b.ToBytes()
	switch {
	case !okA && !okB:
		// Both Values already have types
		return nil

	case okA && okB:
		// Both Values are untyped so try the types in order:
		// int, float, bool, string

		// Check for numeric inference
		iA, okAi := a.bytesToInt()
		iB, okBi := b.bytesToInt()
		if okAi && okBi {
			a.setInt(iA)
			b.setInt(iB)
			return nil
		}

		fA, okAf := a.bytesToFloat()
		fB, okBf := b.bytesToFloat()
		if okAf && okBf {
			a.setFloat(fA)
			b.setFloat(fB)
			return nil
		}

		// Check if they int and float combination.
		if okAi && okBf {
			a.setInt(iA)
			b.setFloat(fA)
			return nil
		}
		if okBi && okAf {
			a.setFloat(fA)
			b.setInt(iB)
			return nil
		}

		// Not numeric types at this point.

		// Check for bool inference
		bA, okAb := a.bytesToBool()
		bB, okBb := b.bytesToBool()
		if okAb && okBb {
			a.setBool(bA)
			b.setBool(bB)
			return nil
		}

		// Fallback to string
		sA := a.bytesToString()
		sB := b.bytesToString()
		a.setString(sA)
		b.setString(sB)
		return nil

	case okA && !okB:
		// Here a has `a` is untyped, but `b` has a fixed
		// type.
		switch b.value.(type) {
		case string:
			s := a.bytesToString()
			a.setString(s)

		case int64, float64:
			if iA, ok := a.bytesToInt(); ok {
				a.setInt(iA)
			} else if fA, ok := a.bytesToFloat(); ok {
				a.setFloat(fA)
			} else {
				return fmt.Errorf("Could not convert %s to a number", a.String())
			}

		case bool:
			if bA, ok := a.bytesToBool(); ok {
				a.setBool(bA)
			} else {
				return fmt.Errorf("Could not convert %s to a boolean", a.String())
			}

		default:
			return errCmpMismatchedTypes
		}
		return nil

	case !okA && okB:
		// swap arguments to avoid repeating code
		return inferTypesForCmp(b, a)

	default:
		// Does not happen
		return nil
	}
}

// Value arithmetic functions: we do not expose them outside the
// module. All arithmetic works only on numeric values with automatic
// promotion to the "larger" type that can represent the value. TODO:
// Add support for large number arithmetic.

// Supported arithmetic operators
const (
	opPlus     = "+"
	opMinus    = "-"
	opDivide   = "/"
	opMultiply = "*"
	opModulo   = "%"
)

// For arithmetic operations, if both values are numeric then the
// operation shall succeed. If the types are unknown automatic type
// conversion to a number is attempted.
func (v *Value) arithOp(op string, a *Value) error {
	err := inferTypeForArithOp(v)
	if err != nil {
		return err
	}

	err = inferTypeForArithOp(a)
	if err != nil {
		return err
	}

	if !v.isNumeric() || !a.isNumeric() {
		return errInvalidDataType(errArithMismatchedTypes)
	}

	if !isValidArithOperator(op) {
		return errInvalidDataType(errArithMismatchedTypes)
	}

	intV, ok1i := v.ToInt()
	intA, ok2i := a.ToInt()
	switch {
	case ok1i && ok2i:
		res, err := intArithOp(op, intV, intA)
		v.setInt(res)
		return err

	default:
		// Convert arguments to float
		flV, _ := v.ToFloat()
		flA, _ := a.ToFloat()
		res, err := floatArithOp(op, flV, flA)
		v.setFloat(res)
		return err
	}
}

func inferTypeForArithOp(a *Value) error {
	if _, ok := a.ToBytes(); !ok {
		return nil
	}

	if i, ok := a.bytesToInt(); ok {
		a.setInt(i)
		return nil
	}

	if f, ok := a.bytesToFloat(); ok {
		a.setFloat(f)
		return nil
	}

	err := fmt.Errorf("Could not convert %q to a number", string(a.value.([]byte)))
	return errInvalidDataType(err)
}

// All the bytesTo* functions defined below assume the value is a byte-slice.

// Converts untyped value into int. The bool return implies success -
// it returns false only if there is a conversion failure.
func (v Value) bytesToInt() (int64, bool) {
	bytes, _ := v.ToBytes()
	i, err := strconv.ParseInt(strings.TrimSpace(string(bytes)), 10, 64)
	return i, err == nil
}

// Converts untyped value into float. The bool return implies success
// - it returns false only if there is a conversion failure.
func (v Value) bytesToFloat() (float64, bool) {
	bytes, _ := v.ToBytes()
	i, err := strconv.ParseFloat(strings.TrimSpace(string(bytes)), 64)
	return i, err == nil
}

// Converts untyped value into bool. The second bool return implies
// success - it returns false in case of a conversion failure.
func (v Value) bytesToBool() (val bool, ok bool) {
	bytes, _ := v.ToBytes()
	ok = true
	switch strings.ToLower(strings.TrimSpace(string(bytes))) {
	case "t", "true", "1":
		val = true
	case "f", "false", "0":
		val = false
	default:
		ok = false
	}
	return val, ok
}

// bytesToString - never fails, but returns empty string if value is not bytes.
func (v Value) bytesToString() string {
	bytes, _ := v.ToBytes()
	return string(bytes)
}

// Calculates minimum or maximum of v and a and assigns the result to
// v - it works only on numeric arguments, where `v` is already
// assumed to be numeric. Attempts conversion to numeric type for `a`
// (first int, then float) only if the underlying values do not have a
// type.
func (v *Value) minmax(a *Value, isMax, isFirstRow bool) error {
	err := inferTypeForArithOp(a)
	if err != nil {
		return err
	}

	if !a.isNumeric() {
		return errArithMismatchedTypes
	}

	// In case of first row, set v to a.
	if isFirstRow {
		intA, okI := a.ToInt()
		if okI {
			v.setInt(intA)
			return nil
		}
		floatA, _ := a.ToFloat()
		v.setFloat(floatA)
		return nil
	}

	intV, ok1i := v.ToInt()
	intA, ok2i := a.ToInt()
	if ok1i && ok2i {
		result := intV
		if !isMax {
			if intA < result {
				result = intA
			}
		} else {
			if intA > result {
				result = intA
			}
		}
		v.setInt(result)
		return nil
	}

	floatV, _ := v.ToFloat()
	floatA, _ := a.ToFloat()
	var result float64
	if !isMax {
		result = math.Min(floatV, floatA)
	} else {
		result = math.Max(floatV, floatA)
	}
	v.setFloat(result)
	return nil
}

func inferTypeAsTimestamp(v *Value) error {
	if s, ok := v.ToString(); ok {
		t, err := parseSQLTimestamp(s)
		if err != nil {
			return err
		}
		v.setTimestamp(t)
	} else if b, ok := v.ToBytes(); ok {
		s := string(b)
		t, err := parseSQLTimestamp(s)
		if err != nil {
			return err
		}
		v.setTimestamp(t)
	}
	return nil
}

// inferTypeAsString is used to convert untyped values to string - it
// is called when the caller requires a string context to proceed.
func inferTypeAsString(v *Value) {
	b, ok := v.ToBytes()
	if !ok {
		return
	}

	v.setString(string(b))
}

func isValidComparisonOperator(op string) bool {
	switch op {
	case opLt:
	case opLte:
	case opGt:
	case opGte:
	case opEq:
	case opIneq:
	default:
		return false
	}
	return true
}

func intCompare(op string, left, right int64) bool {
	switch op {
	case opLt:
		return left < right
	case opLte:
		return left <= right
	case opGt:
		return left > right
	case opGte:
		return left >= right
	case opEq:
		return left == right
	case opIneq:
		return left != right
	}
	// This case does not happen
	return false
}

func floatCompare(op string, left, right float64) bool {
	diff := math.Abs(left - right)
	switch op {
	case opLt:
		return left < right
	case opLte:
		return left <= right
	case opGt:
		return left > right
	case opGte:
		return left >= right
	case opEq:
		return diff < floatCmpTolerance
	case opIneq:
		return diff > floatCmpTolerance
	}
	// This case does not happen
	return false
}

func stringCompare(op string, left, right string) bool {
	switch op {
	case opLt:
		return left < right
	case opLte:
		return left <= right
	case opGt:
		return left > right
	case opGte:
		return left >= right
	case opEq:
		return left == right
	case opIneq:
		return left != right
	}
	// This case does not happen
	return false
}

func boolCompare(op string, left, right bool) (bool, error) {
	switch op {
	case opEq:
		return left == right, nil
	case opIneq:
		return left != right, nil
	default:
		return false, errCmpInvalidBoolOperator
	}
}

func arrayCompare(op string, left, right []Value) (bool, error) {
	switch op {
	case opEq:
		if len(left) != len(right) {
			return false, nil
		}
		for i, l := range left {
			eq, err := l.compareOp(op, &right[i])
			if !eq || err != nil {
				return eq, err
			}
		}
		return true, nil
	case opIneq:
		for i, l := range left {
			eq, err := l.compareOp(op, &right[i])
			if eq || err != nil {
				return eq, err
			}
		}
		return false, nil
	default:
		return false, errCmpInvalidBoolOperator
	}
}

func isValidArithOperator(op string) bool {
	switch op {
	case opPlus:
	case opMinus:
	case opDivide:
	case opMultiply:
	case opModulo:
	default:
		return false
	}
	return true
}

// Overflow errors are ignored.
func intArithOp(op string, left, right int64) (int64, error) {
	switch op {
	case opPlus:
		return left + right, nil
	case opMinus:
		return left - right, nil
	case opDivide:
		if right == 0 {
			return 0, errArithDivideByZero
		}
		return left / right, nil
	case opMultiply:
		return left * right, nil
	case opModulo:
		if right == 0 {
			return 0, errArithDivideByZero
		}
		return left % right, nil
	}
	// This does not happen
	return 0, nil
}

// Overflow errors are ignored.
func floatArithOp(op string, left, right float64) (float64, error) {
	switch op {
	case opPlus:
		return left + right, nil
	case opMinus:
		return left - right, nil
	case opDivide:
		if right == 0 {
			return 0, errArithDivideByZero
		}
		return left / right, nil
	case opMultiply:
		return left * right, nil
	case opModulo:
		if right == 0 {
			return 0, errArithDivideByZero
		}
		return math.Mod(left, right), nil
	}
	// This does not happen
	return 0, nil
}
