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
	"fmt"
	"strings"
	"time"
)

// FuncName - SQL function name.
type FuncName string

const (
	// Avg - aggregate SQL function AVG().
	Avg FuncName = "AVG"

	// Count - aggregate SQL function COUNT().
	Count FuncName = "COUNT"

	// Max - aggregate SQL function MAX().
	Max FuncName = "MAX"

	// Min - aggregate SQL function MIN().
	Min FuncName = "MIN"

	// Sum - aggregate SQL function SUM().
	Sum FuncName = "SUM"

	// Coalesce - conditional SQL function COALESCE().
	Coalesce FuncName = "COALESCE"

	// NullIf - conditional SQL function NULLIF().
	NullIf FuncName = "NULLIF"

	// ToTimestamp - conversion SQL function TO_TIMESTAMP().
	ToTimestamp FuncName = "TO_TIMESTAMP"

	// UTCNow - date SQL function UTCNOW().
	UTCNow FuncName = "UTCNOW"

	// CharLength - string SQL function CHAR_LENGTH().
	CharLength FuncName = "CHAR_LENGTH"

	// CharacterLength - string SQL function CHARACTER_LENGTH() same as CHAR_LENGTH().
	CharacterLength FuncName = "CHARACTER_LENGTH"

	// Lower - string SQL function LOWER().
	Lower FuncName = "LOWER"

	// Substring - string SQL function SUBSTRING().
	Substring FuncName = "SUBSTRING"

	// Trim - string SQL function TRIM().
	Trim FuncName = "TRIM"

	// Upper - string SQL function UPPER().
	Upper FuncName = "UPPER"

	// DateAdd         FuncName = "DATE_ADD"
	// DateDiff        FuncName = "DATE_DIFF"
	// Extract         FuncName = "EXTRACT"
	// ToString        FuncName = "TO_STRING"
	// Cast            FuncName = "CAST" // CAST('2007-04-05T14:30Z' AS TIMESTAMP)
)

func isAggregateFuncName(s string) bool {
	switch FuncName(s) {
	case Avg, Count, Max, Min, Sum:
		return true
	}

	return false
}

func callForNumber(f Expr, record Record) (*Value, error) {
	value, err := f.Eval(record)
	if err != nil {
		return nil, err
	}

	if !value.Type().isNumber() {
		err := fmt.Errorf("%v evaluated to %v; not to number", f, value.Type())
		return nil, errExternalEvalException(err)
	}

	return value, nil
}

func callForInt(f Expr, record Record) (*Value, error) {
	value, err := f.Eval(record)
	if err != nil {
		return nil, err
	}

	if value.Type() != Int {
		err := fmt.Errorf("%v evaluated to %v; not to int", f, value.Type())
		return nil, errExternalEvalException(err)
	}

	return value, nil
}

func callForString(f Expr, record Record) (*Value, error) {
	value, err := f.Eval(record)
	if err != nil {
		return nil, err
	}

	if value.Type() != String {
		err := fmt.Errorf("%v evaluated to %v; not to string", f, value.Type())
		return nil, errExternalEvalException(err)
	}

	return value, nil
}

// funcExpr - SQL function.
type funcExpr struct {
	args []Expr
	name FuncName

	sumValue   float64
	countValue int64
	maxValue   float64
	minValue   float64
}

// String - returns string representation of this function.
func (f *funcExpr) String() string {
	var argStrings []string
	for _, arg := range f.args {
		argStrings = append(argStrings, fmt.Sprintf("%v", arg))
	}

	return fmt.Sprintf("%v(%v)", f.name, strings.Join(argStrings, ","))
}

func (f *funcExpr) sum(record Record) (*Value, error) {
	value, err := callForNumber(f.args[0], record)
	if err != nil {
		return nil, err
	}

	f.sumValue += value.FloatValue()
	f.countValue++
	return nil, nil
}

func (f *funcExpr) count(record Record) (*Value, error) {
	value, err := f.args[0].Eval(record)
	if err != nil {
		return nil, err
	}

	if value.valueType != Null {
		f.countValue++
	}

	return nil, nil
}

func (f *funcExpr) max(record Record) (*Value, error) {
	value, err := callForNumber(f.args[0], record)
	if err != nil {
		return nil, err
	}

	v := value.FloatValue()
	if v > f.maxValue {
		f.maxValue = v
	}

	return nil, nil
}

func (f *funcExpr) min(record Record) (*Value, error) {
	value, err := callForNumber(f.args[0], record)
	if err != nil {
		return nil, err
	}

	v := value.FloatValue()
	if v < f.minValue {
		f.minValue = v
	}
	return nil, nil
}

func (f *funcExpr) charLength(record Record) (*Value, error) {
	value, err := callForString(f.args[0], record)
	if err != nil {
		return nil, err
	}

	return NewInt(int64(len(value.StringValue()))), nil
}

func (f *funcExpr) trim(record Record) (*Value, error) {
	value, err := callForString(f.args[0], record)
	if err != nil {
		return nil, err
	}

	return NewString(strings.TrimSpace(value.StringValue())), nil
}

func (f *funcExpr) lower(record Record) (*Value, error) {
	value, err := callForString(f.args[0], record)
	if err != nil {
		return nil, err
	}

	return NewString(strings.ToLower(value.StringValue())), nil
}

func (f *funcExpr) upper(record Record) (*Value, error) {
	value, err := callForString(f.args[0], record)
	if err != nil {
		return nil, err
	}

	return NewString(strings.ToUpper(value.StringValue())), nil
}

func (f *funcExpr) substring(record Record) (*Value, error) {
	stringValue, err := callForString(f.args[0], record)
	if err != nil {
		return nil, err
	}

	offsetValue, err := callForInt(f.args[1], record)
	if err != nil {
		return nil, err
	}

	var lengthValue *Value
	if len(f.args) == 3 {
		lengthValue, err = callForInt(f.args[2], record)
		if err != nil {
			return nil, err
		}
	}

	value := stringValue.StringValue()
	offset := int(offsetValue.FloatValue())
	if offset < 0 || offset > len(value) {
		offset = 0
	}
	length := len(value)
	if lengthValue != nil {
		length = int(lengthValue.FloatValue())
		if length < 0 || length > len(value) {
			length = len(value)
		}
	}

	return NewString(value[offset:length]), nil
}

func (f *funcExpr) coalesce(record Record) (*Value, error) {
	values := make([]*Value, len(f.args))
	var err error
	for i := range f.args {
		values[i], err = f.args[i].Eval(record)
		if err != nil {
			return nil, err
		}
	}

	for i := range values {
		if values[i].Type() != Null {
			return values[i], nil
		}
	}

	return values[0], nil
}

func (f *funcExpr) nullIf(record Record) (*Value, error) {
	value1, err := f.args[0].Eval(record)
	if err != nil {
		return nil, err
	}

	value2, err := f.args[1].Eval(record)
	if err != nil {
		return nil, err
	}

	result, err := equal(value1, value2)
	if err != nil {
		return nil, err
	}

	if result {
		return NewNull(), nil
	}

	return value1, nil
}

func (f *funcExpr) toTimeStamp(record Record) (*Value, error) {
	value, err := callForString(f.args[0], record)
	if err != nil {
		return nil, err
	}

	t, err := time.Parse(time.RFC3339, value.StringValue())
	if err != nil {
		err := fmt.Errorf("%v: value '%v': %v", f, value, err)
		return nil, errValueParseFailure(err)
	}

	return NewTime(t), nil
}

func (f *funcExpr) utcNow(record Record) (*Value, error) {
	return NewTime(time.Now().UTC()), nil
}

// Call - evaluates this function for given arg values and returns result as Value.
func (f *funcExpr) Eval(record Record) (*Value, error) {
	switch f.name {
	case Avg, Sum:
		return f.sum(record)
	case Count:
		return f.count(record)
	case Max:
		return f.max(record)
	case Min:
		return f.min(record)
	case Coalesce:
		return f.coalesce(record)
	case NullIf:
		return f.nullIf(record)
	case ToTimestamp:
		return f.toTimeStamp(record)
	case UTCNow:
		return f.utcNow(record)
	case Substring:
		return f.substring(record)
	case CharLength, CharacterLength:
		return f.charLength(record)
	case Trim:
		return f.trim(record)
	case Lower:
		return f.lower(record)
	case Upper:
		return f.upper(record)
	}

	panic(fmt.Sprintf("unsupported aggregate function %v", f.name))
}

// AggregateValue - returns aggregated value.
func (f *funcExpr) AggregateValue() (*Value, error) {
	switch f.name {
	case Avg:
		return NewFloat(f.sumValue / float64(f.countValue)), nil
	case Count:
		return NewInt(f.countValue), nil
	case Max:
		return NewFloat(f.maxValue), nil
	case Min:
		return NewFloat(f.minValue), nil
	case Sum:
		return NewFloat(f.sumValue), nil
	}

	err := fmt.Errorf("%v is not aggreate function", f)
	return nil, errExternalEvalException(err)
}

// Type - returns Function or aggregateFunction type.
func (f *funcExpr) Type() Type {
	switch f.name {
	case Avg, Count, Max, Min, Sum:
		return aggregateFunction
	}

	return function
}

// ReturnType - returns respective primitive type depending on SQL function.
func (f *funcExpr) ReturnType() Type {
	switch f.name {
	case Avg, Max, Min, Sum:
		return Float
	case Count:
		return Int
	case CharLength, CharacterLength, Trim, Lower, Upper, Substring:
		return String
	case ToTimestamp, UTCNow:
		return Timestamp
	case Coalesce, NullIf:
		return column
	}

	return function
}

// newFuncExpr - creates new SQL function.
func newFuncExpr(funcName FuncName, funcs ...Expr) (*funcExpr, error) {
	switch funcName {
	case Avg, Max, Min, Sum:
		if len(funcs) != 1 {
			err := fmt.Errorf("%v(): exactly one argument expected; got %v", funcName, len(funcs))
			return nil, errParseNonUnaryAgregateFunctionCall(err)
		}

		if !funcs[0].ReturnType().isNumberKind() {
			err := fmt.Errorf("%v(): argument %v evaluate to %v, not number", funcName, funcs[0], funcs[0].ReturnType())
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}

		return &funcExpr{
			args: funcs,
			name: funcName,
		}, nil

	case Count:
		if len(funcs) != 1 {
			err := fmt.Errorf("%v(): exactly one argument expected; got %v", funcName, len(funcs))
			return nil, errParseNonUnaryAgregateFunctionCall(err)
		}

		switch funcs[0].ReturnType() {
		case Null, Bool, Int, Float, String, Timestamp, column, record:
		default:
			err := fmt.Errorf("%v(): argument %v evaluate to %v is incompatible", funcName, funcs[0], funcs[0].ReturnType())
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}

		return &funcExpr{
			args: funcs,
			name: funcName,
		}, nil

	case CharLength, CharacterLength, Trim, Lower, Upper, ToTimestamp:
		if len(funcs) != 1 {
			err := fmt.Errorf("%v(): exactly one argument expected; got %v", funcName, len(funcs))
			return nil, errEvaluatorInvalidArguments(err)
		}

		if !funcs[0].ReturnType().isStringKind() {
			err := fmt.Errorf("%v(): argument %v evaluate to %v, not string", funcName, funcs[0], funcs[0].ReturnType())
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}

		return &funcExpr{
			args: funcs,
			name: funcName,
		}, nil

	case Coalesce:
		if len(funcs) < 1 {
			err := fmt.Errorf("%v(): one or more argument expected; got %v", funcName, len(funcs))
			return nil, errEvaluatorInvalidArguments(err)
		}

		for i := range funcs {
			if !funcs[i].ReturnType().isBaseKind() {
				err := fmt.Errorf("%v(): argument-%v %v evaluate to %v is incompatible", funcName, i+1, funcs[i], funcs[i].ReturnType())
				return nil, errIncorrectSQLFunctionArgumentType(err)
			}
		}

		return &funcExpr{
			args: funcs,
			name: funcName,
		}, nil

	case NullIf:
		if len(funcs) != 2 {
			err := fmt.Errorf("%v(): exactly two arguments expected; got %v", funcName, len(funcs))
			return nil, errEvaluatorInvalidArguments(err)
		}

		if !funcs[0].ReturnType().isBaseKind() {
			err := fmt.Errorf("%v(): argument-1 %v evaluate to %v is incompatible", funcName, funcs[0], funcs[0].ReturnType())
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}

		if !funcs[1].ReturnType().isBaseKind() {
			err := fmt.Errorf("%v(): argument-2 %v evaluate to %v is incompatible", funcName, funcs[1], funcs[1].ReturnType())
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}

		return &funcExpr{
			args: funcs,
			name: funcName,
		}, nil

	case UTCNow:
		if len(funcs) != 0 {
			err := fmt.Errorf("%v(): no argument expected; got %v", funcName, len(funcs))
			return nil, errEvaluatorInvalidArguments(err)
		}

		return &funcExpr{
			args: funcs,
			name: funcName,
		}, nil

	case Substring:
		if len(funcs) < 2 || len(funcs) > 3 {
			err := fmt.Errorf("%v(): exactly two or three arguments expected; got %v", funcName, len(funcs))
			return nil, errEvaluatorInvalidArguments(err)
		}

		if !funcs[0].ReturnType().isStringKind() {
			err := fmt.Errorf("%v(): argument-1 %v evaluate to %v, not string", funcName, funcs[0], funcs[0].ReturnType())
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}

		if !funcs[1].ReturnType().isIntKind() {
			err := fmt.Errorf("%v(): argument-2 %v evaluate to %v, not int", funcName, funcs[1], funcs[1].ReturnType())
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}

		if len(funcs) > 2 {
			if !funcs[2].ReturnType().isIntKind() {
				err := fmt.Errorf("%v(): argument-3 %v evaluate to %v, not int", funcName, funcs[2], funcs[2].ReturnType())
				return nil, errIncorrectSQLFunctionArgumentType(err)
			}
		}

		return &funcExpr{
			args: funcs,
			name: funcName,
		}, nil
	}

	return nil, errUnsupportedFunction(fmt.Errorf("unknown function name %v", funcName))
}
