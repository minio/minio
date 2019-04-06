/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var (
	errArithMismatchedTypes = errors.New("cannot perform arithmetic on mismatched types")
	errArithInvalidOperator = errors.New("invalid arithmetic operator")
	errArithDivideByZero    = errors.New("cannot divide by 0")

	errCmpMismatchedTypes     = errors.New("cannot compare values of different types")
	errCmpInvalidBoolOperator = errors.New("invalid comparison operator for boolean arguments")
)

// vType represents the concrete type of a `Value`
type vType int

// Valid values for Type
const (
	typeNull vType = iota + 1
	typeBool
	typeString

	// 64-bit signed integer
	typeInt

	// 64-bit floating point
	typeFloat

	// timestamp type
	typeTimestamp

	// This type refers to untyped values, e.g. as read from CSV
	typeBytes
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
	vType vType
}

// GetTypeString returns a string representation for vType
func (v *Value) GetTypeString() string {
	switch v.vType {
	case typeNull:
		return "NULL"
	case typeBool:
		return "BOOL"
	case typeString:
		return "STRING"
	case typeInt:
		return "INT"
	case typeFloat:
		return "FLOAT"
	case typeTimestamp:
		return "TIMESTAMP"
	case typeBytes:
		return "BYTES"
	}
	return "--"
}

// Repr returns a string representation of value.
func (v *Value) Repr() string {
	switch v.vType {
	case typeNull:
		return ":NULL"
	case typeBool, typeInt, typeFloat:
		return fmt.Sprintf("%v:%s", v.value, v.GetTypeString())
	case typeTimestamp:
		return fmt.Sprintf("%s:TIMESTAMP", v.value.(*time.Time))
	case typeString:
		return fmt.Sprintf("\"%s\":%s", v.value.(string), v.GetTypeString())
	case typeBytes:
		return fmt.Sprintf("\"%s\":BYTES", string(v.value.([]byte)))
	default:
		return fmt.Sprintf("%v:INVALID", v.value)
	}
}

// FromFloat creates a Value from a number
func FromFloat(f float64) *Value {
	return &Value{value: f, vType: typeFloat}
}

// FromInt creates a Value from an int
func FromInt(f int64) *Value {
	return &Value{value: f, vType: typeInt}
}

// FromString creates a Value from a string
func FromString(str string) *Value {
	return &Value{value: str, vType: typeString}
}

// FromBool creates a Value from a bool
func FromBool(b bool) *Value {
	return &Value{value: b, vType: typeBool}
}

// FromTimestamp creates a Value from a timestamp
func FromTimestamp(t time.Time) *Value {
	return &Value{value: t, vType: typeTimestamp}
}

// FromNull creates a Value with Null value
func FromNull() *Value {
	return &Value{vType: typeNull}
}

// FromBytes creates a Value from a []byte
func FromBytes(b []byte) *Value {
	return &Value{value: b, vType: typeBytes}
}

// ToFloat works for int and float values
func (v *Value) ToFloat() (val float64, ok bool) {
	switch v.vType {
	case typeFloat:
		val, ok = v.value.(float64)
	case typeInt:
		var i int64
		i, ok = v.value.(int64)
		val = float64(i)
	default:
	}
	return
}

// ToInt converts value to int.
func (v *Value) ToInt() (val int64, ok bool) {
	switch v.vType {
	case typeInt:
		val, ok = v.value.(int64)
	default:
	}
	return
}

// ToString converts value to string.
func (v *Value) ToString() (val string, ok bool) {
	switch v.vType {
	case typeString:
		val, ok = v.value.(string)
	default:
	}
	return
}

// ToBool returns the bool value; second return value refers to if the bool
// conversion succeeded.
func (v *Value) ToBool() (val bool, ok bool) {
	switch v.vType {
	case typeBool:
		return v.value.(bool), true
	}
	return false, false
}

// ToTimestamp returns the timestamp value if present.
func (v *Value) ToTimestamp() (t time.Time, ok bool) {
	switch v.vType {
	case typeTimestamp:
		return v.value.(time.Time), true
	}
	return t, false
}

// ToBytes converts Value to byte-slice.
func (v *Value) ToBytes() ([]byte, bool) {
	switch v.vType {
	case typeBytes:
		return v.value.([]byte), true
	}
	return nil, false
}

// IsNull - checks if value is missing.
func (v *Value) IsNull() bool {
	return v.vType == typeNull
}

func (v *Value) isNumeric() bool {
	return v.vType == typeInt || v.vType == typeFloat
}

// setters used internally to mutate values

func (v *Value) setInt(i int64) {
	v.vType = typeInt
	v.value = i
}

func (v *Value) setFloat(f float64) {
	v.vType = typeFloat
	v.value = f
}

func (v *Value) setString(s string) {
	v.vType = typeString
	v.value = s
}

func (v *Value) setBool(b bool) {
	v.vType = typeBool
	v.value = b
}

func (v *Value) setTimestamp(t time.Time) {
	v.vType = typeTimestamp
	v.value = t
}

// CSVString - convert to string for CSV serialization
func (v *Value) CSVString() string {
	switch v.vType {
	case typeNull:
		return ""
	case typeBool:
		return fmt.Sprintf("%v", v.value.(bool))
	case typeString:
		return v.value.(string)
	case typeInt:
		return fmt.Sprintf("%v", v.value.(int64))
	case typeFloat:
		return fmt.Sprintf("%v", v.value.(float64))
	case typeTimestamp:
		return FormatSQLTimestamp(v.value.(time.Time))
	case typeBytes:
		return fmt.Sprintf("%v", string(v.value.([]byte)))
	default:
		return "CSV serialization not implemented for this type"
	}
}

// floatToValue converts a float into int representation if needed.
func floatToValue(f float64) *Value {
	intPart, fracPart := math.Modf(f)
	if fracPart == 0 {
		return FromInt(int64(intPart))
	}
	return FromFloat(f)
}

// negate negates a numeric value
func (v *Value) negate() {
	switch v.vType {
	case typeFloat:
		v.value = -(v.value.(float64))
	case typeInt:
		v.value = -(v.value.(int64))
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
	boolA, ok2b := v.ToBool()
	if ok1b && ok2b {
		return boolCompare(op, boolV, boolA)
	}

	return false, errCmpMismatchedTypes
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
		switch b.vType {
		case typeString:
			s := a.bytesToString()
			a.setString(s)

		case typeInt, typeFloat:
			if iA, ok := a.bytesToInt(); ok {
				a.setInt(iA)
			} else if fA, ok := a.bytesToFloat(); ok {
				a.setFloat(fA)
			} else {
				return fmt.Errorf("Could not convert %s to a number", string(a.value.([]byte)))
			}

		case typeBool:
			if bA, ok := a.bytesToBool(); ok {
				a.setBool(bA)
			} else {
				return fmt.Errorf("Could not convert %s to a boolean", string(a.value.([]byte)))
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

	err := fmt.Errorf("Could not convert %s to a number", string(a.value.([]byte)))
	return errInvalidDataType(err)
}

// All the bytesTo* functions defined below assume the value is a byte-slice.

// Converts untyped value into int. The bool return implies success -
// it returns false only if there is a conversion failure.
func (v *Value) bytesToInt() (int64, bool) {
	bytes, _ := v.ToBytes()
	i, err := strconv.ParseInt(string(bytes), 10, 64)
	return i, err == nil
}

// Converts untyped value into float. The bool return implies success
// - it returns false only if there is a conversion failure.
func (v *Value) bytesToFloat() (float64, bool) {
	bytes, _ := v.ToBytes()
	i, err := strconv.ParseFloat(string(bytes), 64)
	return i, err == nil
}

// Converts untyped value into bool. The second bool return implies
// success - it returns false in case of a conversion failure.
func (v *Value) bytesToBool() (val bool, ok bool) {
	bytes, _ := v.ToBytes()
	ok = true
	switch strings.ToLower(string(bytes)) {
	case "t", "true":
		val = true
	case "f", "false":
		val = false
	default:
		ok = false
	}
	return val, ok
}

// bytesToString - never fails
func (v *Value) bytesToString() string {
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
