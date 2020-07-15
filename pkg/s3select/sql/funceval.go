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
	"strconv"
	"strings"
	"time"
)

// FuncName - SQL function name.
type FuncName string

// SQL Function name constants
const (
	// Conditionals
	sqlFnCoalesce FuncName = "COALESCE"
	sqlFnNullIf   FuncName = "NULLIF"

	// Conversion
	sqlFnCast FuncName = "CAST"

	// Date and time
	sqlFnDateAdd     FuncName = "DATE_ADD"
	sqlFnDateDiff    FuncName = "DATE_DIFF"
	sqlFnExtract     FuncName = "EXTRACT"
	sqlFnToString    FuncName = "TO_STRING"
	sqlFnToTimestamp FuncName = "TO_TIMESTAMP"
	sqlFnUTCNow      FuncName = "UTCNOW"

	// String
	sqlFnCharLength      FuncName = "CHAR_LENGTH"
	sqlFnCharacterLength FuncName = "CHARACTER_LENGTH"
	sqlFnLower           FuncName = "LOWER"
	sqlFnSubstring       FuncName = "SUBSTRING"
	sqlFnTrim            FuncName = "TRIM"
	sqlFnUpper           FuncName = "UPPER"
)

var (
	errUnimplementedCast = errors.New("This cast not yet implemented")
	errNonStringTrimArg  = errors.New("TRIM() received a non-string argument")
	errNonTimestampArg   = errors.New("Expected a timestamp argument")
)

func (e *FuncExpr) getFunctionName() FuncName {
	switch {
	case e.SFunc != nil:
		return FuncName(strings.ToUpper(e.SFunc.FunctionName))
	case e.Count != nil:
		return FuncName(aggFnCount)
	case e.Cast != nil:
		return sqlFnCast
	case e.Substring != nil:
		return sqlFnSubstring
	case e.Extract != nil:
		return sqlFnExtract
	case e.Trim != nil:
		return sqlFnTrim
	case e.DateAdd != nil:
		return sqlFnDateAdd
	case e.DateDiff != nil:
		return sqlFnDateDiff
	default:
		return ""
	}
}

// evalSQLFnNode assumes that the FuncExpr is not an aggregation
// function.
func (e *FuncExpr) evalSQLFnNode(r Record) (res *Value, err error) {
	// Handle functions that have phrase arguments
	switch e.getFunctionName() {
	case sqlFnCast:
		expr := e.Cast.Expr
		res, err = expr.castTo(r, strings.ToUpper(e.Cast.CastType))
		return

	case sqlFnSubstring:
		return handleSQLSubstring(r, e.Substring)

	case sqlFnExtract:
		return handleSQLExtract(r, e.Extract)

	case sqlFnTrim:
		return handleSQLTrim(r, e.Trim)

	case sqlFnDateAdd:
		return handleDateAdd(r, e.DateAdd)

	case sqlFnDateDiff:
		return handleDateDiff(r, e.DateDiff)

	}

	// For all simple argument functions, we evaluate the arguments here
	argVals := make([]*Value, len(e.SFunc.ArgsList))
	for i, arg := range e.SFunc.ArgsList {
		argVals[i], err = arg.evalNode(r)
		if err != nil {
			return nil, err
		}
	}

	switch e.getFunctionName() {
	case sqlFnCoalesce:
		return coalesce(argVals)

	case sqlFnNullIf:
		return nullif(argVals[0], argVals[1])

	case sqlFnCharLength, sqlFnCharacterLength:
		return charlen(argVals[0])

	case sqlFnLower:
		return lowerCase(argVals[0])

	case sqlFnUpper:
		return upperCase(argVals[0])

	case sqlFnUTCNow:
		return handleUTCNow()

	case sqlFnToString, sqlFnToTimestamp:
		// TODO: implement
		fallthrough

	default:
		return nil, errNotImplemented
	}
}

func coalesce(args []*Value) (res *Value, err error) {
	for _, arg := range args {
		if arg.IsNull() {
			continue
		}
		return arg, nil
	}
	return FromNull(), nil
}

func nullif(v1, v2 *Value) (res *Value, err error) {
	// Handle Null cases
	if v1.IsNull() || v2.IsNull() {
		return v1, nil
	}

	err = inferTypesForCmp(v1, v2)
	if err != nil {
		return nil, err
	}

	atleastOneNumeric := v1.isNumeric() || v2.isNumeric()
	bothNumeric := v1.isNumeric() && v2.isNumeric()
	if atleastOneNumeric || !bothNumeric {
		return v1, nil
	}

	if v1.SameTypeAs(*v2) {
		return v1, nil
	}

	cmpResult, cmpErr := v1.compareOp(opEq, v2)
	if cmpErr != nil {
		return nil, cmpErr
	}

	if cmpResult {
		return FromNull(), nil
	}

	return v1, nil
}

func charlen(v *Value) (*Value, error) {
	inferTypeAsString(v)
	s, ok := v.ToString()
	if !ok {
		err := fmt.Errorf("%s/%s expects a string argument", sqlFnCharLength, sqlFnCharacterLength)
		return nil, errIncorrectSQLFunctionArgumentType(err)
	}
	return FromInt(int64(len([]rune(s)))), nil
}

func lowerCase(v *Value) (*Value, error) {
	inferTypeAsString(v)
	s, ok := v.ToString()
	if !ok {
		err := fmt.Errorf("%s expects a string argument", sqlFnLower)
		return nil, errIncorrectSQLFunctionArgumentType(err)
	}
	return FromString(strings.ToLower(s)), nil
}

func upperCase(v *Value) (*Value, error) {
	inferTypeAsString(v)
	s, ok := v.ToString()
	if !ok {
		err := fmt.Errorf("%s expects a string argument", sqlFnUpper)
		return nil, errIncorrectSQLFunctionArgumentType(err)
	}
	return FromString(strings.ToUpper(s)), nil
}

func handleDateAdd(r Record, d *DateAddFunc) (*Value, error) {
	q, err := d.Quantity.evalNode(r)
	if err != nil {
		return nil, err
	}
	inferTypeForArithOp(q)
	qty, ok := q.ToFloat()
	if !ok {
		return nil, fmt.Errorf("QUANTITY must be a numeric argument to %s()", sqlFnDateAdd)
	}

	ts, err := d.Timestamp.evalNode(r)
	if err != nil {
		return nil, err
	}
	if err = inferTypeAsTimestamp(ts); err != nil {
		return nil, err
	}
	t, ok := ts.ToTimestamp()
	if !ok {
		return nil, fmt.Errorf("%s() expects a timestamp argument", sqlFnDateAdd)
	}

	return dateAdd(strings.ToUpper(d.DatePart), qty, t)
}

func handleDateDiff(r Record, d *DateDiffFunc) (*Value, error) {
	tval1, err := d.Timestamp1.evalNode(r)
	if err != nil {
		return nil, err
	}
	if err = inferTypeAsTimestamp(tval1); err != nil {
		return nil, err
	}
	ts1, ok := tval1.ToTimestamp()
	if !ok {
		return nil, fmt.Errorf("%s() expects two timestamp arguments", sqlFnDateDiff)
	}

	tval2, err := d.Timestamp2.evalNode(r)
	if err != nil {
		return nil, err
	}
	if err = inferTypeAsTimestamp(tval2); err != nil {
		return nil, err
	}
	ts2, ok := tval2.ToTimestamp()
	if !ok {
		return nil, fmt.Errorf("%s() expects two timestamp arguments", sqlFnDateDiff)
	}

	return dateDiff(strings.ToUpper(d.DatePart), ts1, ts2)
}

func handleUTCNow() (*Value, error) {
	return FromTimestamp(time.Now().UTC()), nil
}

func handleSQLSubstring(r Record, e *SubstringFunc) (val *Value, err error) {
	// Both forms `SUBSTRING('abc' FROM 2 FOR 1)` and
	// SUBSTRING('abc', 2, 1) are supported.

	// Evaluate the string argument
	v1, err := e.Expr.evalNode(r)
	if err != nil {
		return nil, err
	}
	inferTypeAsString(v1)
	s, ok := v1.ToString()
	if !ok {
		err := fmt.Errorf("Incorrect argument type passed to %s", sqlFnSubstring)
		return nil, errIncorrectSQLFunctionArgumentType(err)
	}

	// Assemble other arguments
	arg2, arg3 := e.From, e.For
	// Check if the second form of substring is being used
	if e.From == nil {
		arg2, arg3 = e.Arg2, e.Arg3
	}

	// Evaluate the FROM argument
	v2, err := arg2.evalNode(r)
	if err != nil {
		return nil, err
	}
	inferTypeForArithOp(v2)
	startIdx, ok := v2.ToInt()
	if !ok {
		err := fmt.Errorf("Incorrect type for start index argument in %s", sqlFnSubstring)
		return nil, errIncorrectSQLFunctionArgumentType(err)
	}

	length := -1
	// Evaluate the optional FOR argument
	if arg3 != nil {
		v3, err := arg3.evalNode(r)
		if err != nil {
			return nil, err
		}
		inferTypeForArithOp(v3)
		lenInt, ok := v3.ToInt()
		if !ok {
			err := fmt.Errorf("Incorrect type for length argument in %s", sqlFnSubstring)
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}
		length = int(lenInt)
		if length < 0 {
			err := fmt.Errorf("Negative length argument in %s", sqlFnSubstring)
			return nil, errIncorrectSQLFunctionArgumentType(err)
		}
	}

	res, err := evalSQLSubstring(s, int(startIdx), length)
	return FromString(res), err
}

func handleSQLTrim(r Record, e *TrimFunc) (res *Value, err error) {
	chars := ""
	ok := false
	if e.TrimChars != nil {
		charsV, cerr := e.TrimChars.evalNode(r)
		if cerr != nil {
			return nil, cerr
		}
		inferTypeAsString(charsV)
		chars, ok = charsV.ToString()
		if !ok {
			return nil, errNonStringTrimArg
		}
	}

	fromV, ferr := e.TrimFrom.evalNode(r)
	if ferr != nil {
		return nil, ferr
	}
	inferTypeAsString(fromV)
	from, ok := fromV.ToString()
	if !ok {
		return nil, errNonStringTrimArg
	}

	result, terr := evalSQLTrim(e.TrimWhere, chars, from)
	if terr != nil {
		return nil, terr
	}
	return FromString(result), nil
}

func handleSQLExtract(r Record, e *ExtractFunc) (res *Value, err error) {
	timeVal, verr := e.From.evalNode(r)
	if verr != nil {
		return nil, verr
	}

	if err = inferTypeAsTimestamp(timeVal); err != nil {
		return nil, err
	}

	t, ok := timeVal.ToTimestamp()
	if !ok {
		return nil, errNonTimestampArg
	}

	return extract(strings.ToUpper(e.Timeword), t)
}

func errUnsupportedCast(fromType, toType string) error {
	return fmt.Errorf("Cannot cast from %v to %v", fromType, toType)
}

func errCastFailure(msg string) error {
	return fmt.Errorf("Error casting: %s", msg)
}

// Allowed cast types
const (
	castBool      = "BOOL"
	castInt       = "INT"
	castInteger   = "INTEGER"
	castString    = "STRING"
	castFloat     = "FLOAT"
	castDecimal   = "DECIMAL"
	castNumeric   = "NUMERIC"
	castTimestamp = "TIMESTAMP"
)

func (e *Expression) castTo(r Record, castType string) (res *Value, err error) {
	v, err := e.evalNode(r)
	if err != nil {
		return nil, err
	}

	switch castType {
	case castInt, castInteger:
		i, err := intCast(v)
		return FromInt(i), err

	case castFloat:
		f, err := floatCast(v)
		return FromFloat(f), err

	case castString:
		s, err := stringCast(v)
		return FromString(s), err

	case castTimestamp:
		t, err := timestampCast(v)
		return FromTimestamp(t), err

	case castBool:
		b, err := boolCast(v)
		return FromBool(b), err

	case castDecimal, castNumeric:
		fallthrough

	default:
		return nil, errUnimplementedCast
	}
}

func intCast(v *Value) (int64, error) {
	// This conversion truncates floating point numbers to
	// integer.
	strToInt := func(s string) (int64, bool) {
		i, errI := strconv.ParseInt(s, 10, 64)
		if errI == nil {
			return i, true
		}
		f, errF := strconv.ParseFloat(s, 64)
		if errF == nil {
			return int64(f), true
		}
		return 0, false
	}

	switch x := v.value.(type) {
	case float64:
		// Truncate fractional part
		return int64(x), nil
	case int64:
		return x, nil
	case string:
		// Parse as number, truncate floating point if
		// needed.
		res, ok := strToInt(x)
		if !ok {
			return 0, errCastFailure("could not parse as int")
		}
		return res, nil
	case []byte:
		// Parse as number, truncate floating point if
		// needed.
		res, ok := strToInt(string(x))
		if !ok {
			return 0, errCastFailure("could not parse as int")
		}
		return res, nil

	default:
		return 0, errUnsupportedCast(v.GetTypeString(), castInt)
	}
}

func floatCast(v *Value) (float64, error) {
	switch x := v.value.(type) {
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, errCastFailure("could not parse as float")
		}
		return f, nil
	case []byte:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return 0, errCastFailure("could not parse as float")
		}
		return f, nil
	default:
		return 0, errUnsupportedCast(v.GetTypeString(), castFloat)
	}
}

func stringCast(v *Value) (string, error) {
	switch x := v.value.(type) {
	case float64:
		return fmt.Sprintf("%v", x), nil
	case int64:
		return fmt.Sprintf("%v", x), nil
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case bool:
		return fmt.Sprintf("%v", x), nil
	case nil:
		// FIXME: verify this case is correct
		return "NULL", nil
	}
	// This does not happen
	return "", errCastFailure(fmt.Sprintf("cannot cast %v to string type", v.GetTypeString()))
}

func timestampCast(v *Value) (t time.Time, _ error) {
	switch x := v.value.(type) {
	case string:
		return parseSQLTimestamp(x)
	case []byte:
		return parseSQLTimestamp(string(x))
	case time.Time:
		return x, nil
	default:
		return t, errCastFailure(fmt.Sprintf("cannot cast %v to Timestamp type", v.GetTypeString()))
	}
}

func boolCast(v *Value) (b bool, _ error) {
	sToB := func(s string) (bool, error) {
		switch s {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return false, errCastFailure("cannot cast to Bool")
		}
	}
	switch x := v.value.(type) {
	case bool:
		return x, nil
	case string:
		return sToB(strings.ToLower(x))
	case []byte:
		return sToB(strings.ToLower(string(x)))
	default:
		return false, errCastFailure("cannot cast %v to Bool")
	}
}
