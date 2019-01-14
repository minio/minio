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
	"regexp"
	"strings"
)

// ComparisonOperator - comparison operator.
type ComparisonOperator string

const (
	// Equal operator '='.
	Equal ComparisonOperator = "="

	// NotEqual operator '!=' or '<>'.
	NotEqual ComparisonOperator = "!="

	// LessThan operator '<'.
	LessThan ComparisonOperator = "<"

	// GreaterThan operator '>'.
	GreaterThan ComparisonOperator = ">"

	// LessThanEqual operator '<='.
	LessThanEqual ComparisonOperator = "<="

	// GreaterThanEqual operator '>='.
	GreaterThanEqual ComparisonOperator = ">="

	// Between operator 'BETWEEN'
	Between ComparisonOperator = "between"

	// In operator 'IN'
	In ComparisonOperator = "in"

	// Like operator 'LIKE'
	Like ComparisonOperator = "like"

	// NotBetween operator 'NOT BETWEEN'
	NotBetween ComparisonOperator = "not between"

	// NotIn operator 'NOT IN'
	NotIn ComparisonOperator = "not in"

	// NotLike operator 'NOT LIKE'
	NotLike ComparisonOperator = "not like"

	// IsNull operator 'IS NULL'
	IsNull ComparisonOperator = "is null"

	// IsNotNull operator 'IS NOT NULL'
	IsNotNull ComparisonOperator = "is not null"
)

// String - returns string representation of this operator.
func (operator ComparisonOperator) String() string {
	return strings.ToUpper((string(operator)))
}

func equal(leftValue, rightValue *Value) (bool, error) {
	switch {
	case leftValue.Type() == Null && rightValue.Type() == Null:
		return true, nil
	case leftValue.Type() == Bool && rightValue.Type() == Bool:
		return leftValue.BoolValue() == rightValue.BoolValue(), nil
	case (leftValue.Type() == Int || leftValue.Type() == Float) &&
		(rightValue.Type() == Int || rightValue.Type() == Float):
		return leftValue.FloatValue() == rightValue.FloatValue(), nil
	case leftValue.Type() == String && rightValue.Type() == String:
		return leftValue.StringValue() == rightValue.StringValue(), nil
	case leftValue.Type() == Timestamp && rightValue.Type() == Timestamp:
		return leftValue.TimeValue() == rightValue.TimeValue(), nil
	}

	return false, fmt.Errorf("left value type %v and right value type %v are incompatible for equality check", leftValue.Type(), rightValue.Type())
}

// comparisonExpr - comparison function.
type comparisonExpr struct {
	left     Expr
	right    Expr
	to       Expr
	operator ComparisonOperator
	funcType Type
}

// String - returns string representation of this function.
func (f *comparisonExpr) String() string {
	switch f.operator {
	case Equal, NotEqual, LessThan, GreaterThan, LessThanEqual, GreaterThanEqual, In, Like, NotIn, NotLike:
		return fmt.Sprintf("(%v %v %v)", f.left, f.operator, f.right)
	case Between, NotBetween:
		return fmt.Sprintf("(%v %v %v AND %v)", f.left, f.operator, f.right, f.to)
	}

	return fmt.Sprintf("(%v %v %v %v)", f.left, f.right, f.to, f.operator)
}

func (f *comparisonExpr) equal(leftValue, rightValue *Value) (*Value, error) {
	result, err := equal(leftValue, rightValue)
	if err != nil {
		err = fmt.Errorf("%v: %v", f, err)
		return nil, errExternalEvalException(err)
	}

	return NewBool(result), nil
}

func (f *comparisonExpr) notEqual(leftValue, rightValue *Value) (*Value, error) {
	result, err := equal(leftValue, rightValue)
	if err != nil {
		err = fmt.Errorf("%v: %v", f, err)
		return nil, errExternalEvalException(err)
	}

	return NewBool(!result), nil
}

func (f *comparisonExpr) lessThan(leftValue, rightValue *Value) (*Value, error) {
	if !leftValue.Type().isNumber() {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to number", f, leftValue.Type())
		return nil, errExternalEvalException(err)
	}
	if !rightValue.Type().isNumber() {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to number", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return NewBool(leftValue.FloatValue() < rightValue.FloatValue()), nil
}

func (f *comparisonExpr) greaterThan(leftValue, rightValue *Value) (*Value, error) {
	if !leftValue.Type().isNumber() {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to number", f, leftValue.Type())
		return nil, errExternalEvalException(err)
	}
	if !rightValue.Type().isNumber() {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to number", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return NewBool(leftValue.FloatValue() > rightValue.FloatValue()), nil
}

func (f *comparisonExpr) lessThanEqual(leftValue, rightValue *Value) (*Value, error) {
	if !leftValue.Type().isNumber() {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to number", f, leftValue.Type())
		return nil, errExternalEvalException(err)
	}
	if !rightValue.Type().isNumber() {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to number", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return NewBool(leftValue.FloatValue() <= rightValue.FloatValue()), nil
}

func (f *comparisonExpr) greaterThanEqual(leftValue, rightValue *Value) (*Value, error) {
	if !leftValue.Type().isNumber() {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to number", f, leftValue.Type())
		return nil, errExternalEvalException(err)
	}
	if !rightValue.Type().isNumber() {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to number", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return NewBool(leftValue.FloatValue() >= rightValue.FloatValue()), nil
}

func (f *comparisonExpr) computeBetween(leftValue, fromValue, toValue *Value) (bool, error) {
	if !leftValue.Type().isNumber() {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to number", f, leftValue.Type())
		return false, errExternalEvalException(err)
	}
	if !fromValue.Type().isNumber() {
		err := fmt.Errorf("%v: from side expression evaluated to %v; not to number", f, fromValue.Type())
		return false, errExternalEvalException(err)
	}
	if !toValue.Type().isNumber() {
		err := fmt.Errorf("%v: to side expression evaluated to %v; not to number", f, toValue.Type())
		return false, errExternalEvalException(err)
	}

	return leftValue.FloatValue() >= fromValue.FloatValue() &&
		leftValue.FloatValue() <= toValue.FloatValue(), nil
}

func (f *comparisonExpr) between(leftValue, fromValue, toValue *Value) (*Value, error) {
	result, err := f.computeBetween(leftValue, fromValue, toValue)
	if err != nil {
		return nil, err
	}

	return NewBool(result), nil
}

func (f *comparisonExpr) notBetween(leftValue, fromValue, toValue *Value) (*Value, error) {
	result, err := f.computeBetween(leftValue, fromValue, toValue)
	if err != nil {
		return nil, err
	}

	return NewBool(!result), nil
}

func (f *comparisonExpr) computeIn(leftValue, rightValue *Value) (found bool, err error) {
	if rightValue.Type() != Array {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to Array", f, rightValue.Type())
		return false, errExternalEvalException(err)
	}

	values := rightValue.ArrayValue()

	for i := range values {
		found, err = equal(leftValue, values[i])
		if err != nil {
			return false, err
		}

		if found {
			return true, nil
		}
	}

	return false, nil
}

func (f *comparisonExpr) in(leftValue, rightValue *Value) (*Value, error) {
	result, err := f.computeIn(leftValue, rightValue)
	if err != nil {
		err = fmt.Errorf("%v: %v", f, err)
		return nil, errExternalEvalException(err)
	}

	return NewBool(result), nil
}

func (f *comparisonExpr) notIn(leftValue, rightValue *Value) (*Value, error) {
	result, err := f.computeIn(leftValue, rightValue)
	if err != nil {
		err = fmt.Errorf("%v: %v", f, err)
		return nil, errExternalEvalException(err)
	}

	return NewBool(!result), nil
}

func (f *comparisonExpr) computeLike(leftValue, rightValue *Value) (matched bool, err error) {
	if leftValue.Type() != String {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to string", f, leftValue.Type())
		return false, errExternalEvalException(err)
	}
	if rightValue.Type() != String {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to string", f, rightValue.Type())
		return false, errExternalEvalException(err)
	}

	matched, err = regexp.MatchString(rightValue.StringValue(), leftValue.StringValue())
	if err != nil {
		err = fmt.Errorf("%v: %v", f, err)
		return false, errExternalEvalException(err)
	}

	return matched, nil
}

func (f *comparisonExpr) like(leftValue, rightValue *Value) (*Value, error) {
	result, err := f.computeLike(leftValue, rightValue)
	if err != nil {
		return nil, err
	}

	return NewBool(result), nil
}

func (f *comparisonExpr) notLike(leftValue, rightValue *Value) (*Value, error) {
	result, err := f.computeLike(leftValue, rightValue)
	if err != nil {
		return nil, err
	}

	return NewBool(!result), nil
}

func (f *comparisonExpr) compute(leftValue, rightValue, toValue *Value) (*Value, error) {
	switch f.operator {
	case Equal:
		return f.equal(leftValue, rightValue)
	case NotEqual:
		return f.notEqual(leftValue, rightValue)
	case LessThan:
		return f.lessThan(leftValue, rightValue)
	case GreaterThan:
		return f.greaterThan(leftValue, rightValue)
	case LessThanEqual:
		return f.lessThanEqual(leftValue, rightValue)
	case GreaterThanEqual:
		return f.greaterThanEqual(leftValue, rightValue)
	case Between:
		return f.between(leftValue, rightValue, toValue)
	case In:
		return f.in(leftValue, rightValue)
	case Like:
		return f.like(leftValue, rightValue)
	case NotBetween:
		return f.notBetween(leftValue, rightValue, toValue)
	case NotIn:
		return f.notIn(leftValue, rightValue)
	case NotLike:
		return f.notLike(leftValue, rightValue)
	}

	panic(fmt.Errorf("unexpected expression %v", f))
}

// Call - evaluates this function for given arg values and returns result as Value.
func (f *comparisonExpr) Eval(record Record) (*Value, error) {
	leftValue, err := f.left.Eval(record)
	if err != nil {
		return nil, err
	}

	rightValue, err := f.right.Eval(record)
	if err != nil {
		return nil, err
	}

	var toValue *Value
	if f.to != nil {
		toValue, err = f.to.Eval(record)
		if err != nil {
			return nil, err
		}
	}

	if f.funcType == aggregateFunction {
		return nil, nil
	}

	return f.compute(leftValue, rightValue, toValue)
}

// AggregateValue - returns aggregated value.
func (f *comparisonExpr) AggregateValue() (*Value, error) {
	if f.funcType != aggregateFunction {
		err := fmt.Errorf("%v is not aggreate expression", f)
		return nil, errExternalEvalException(err)
	}

	leftValue, err := f.left.AggregateValue()
	if err != nil {
		return nil, err
	}

	rightValue, err := f.right.AggregateValue()
	if err != nil {
		return nil, err
	}

	var toValue *Value
	if f.to != nil {
		toValue, err = f.to.AggregateValue()
		if err != nil {
			return nil, err
		}
	}

	return f.compute(leftValue, rightValue, toValue)
}

// Type - returns comparisonFunction or aggregateFunction type.
func (f *comparisonExpr) Type() Type {
	return f.funcType
}

// ReturnType - returns Bool as return type.
func (f *comparisonExpr) ReturnType() Type {
	return Bool
}

// newComparisonExpr - creates new comparison function.
func newComparisonExpr(operator ComparisonOperator, funcs ...Expr) (*comparisonExpr, error) {
	funcType := comparisonFunction
	switch operator {
	case Equal, NotEqual:
		if len(funcs) != 2 {
			panic(fmt.Sprintf("exactly two arguments are expected, but found %v", len(funcs)))
		}

		left := funcs[0]
		if !left.ReturnType().isBaseKind() {
			err := fmt.Errorf("operator %v: left side expression %v evaluate to %v is incompatible for equality check", operator, left, left.ReturnType())
			return nil, errInvalidDataType(err)
		}

		right := funcs[1]
		if !right.ReturnType().isBaseKind() {
			err := fmt.Errorf("operator %v: right side expression %v evaluate to %v is incompatible for equality check", operator, right, right.ReturnType())
			return nil, errInvalidDataType(err)
		}

		if left.Type() == aggregateFunction || right.Type() == aggregateFunction {
			funcType = aggregateFunction
			switch left.Type() {
			case column, Array, function, arithmeticFunction, comparisonFunction, logicalFunction, record:
				err := fmt.Errorf("operator %v: left side expression %v return type %v is incompatible for equality check", operator, left, left.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
			switch right.Type() {
			case column, Array, function, arithmeticFunction, comparisonFunction, logicalFunction, record:
				err := fmt.Errorf("operator %v: right side expression %v return type %v is incompatible for equality check", operator, right, right.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
		}

		return &comparisonExpr{
			left:     left,
			right:    right,
			operator: operator,
			funcType: funcType,
		}, nil

	case LessThan, GreaterThan, LessThanEqual, GreaterThanEqual:
		if len(funcs) != 2 {
			panic(fmt.Sprintf("exactly two arguments are expected, but found %v", len(funcs)))
		}

		left := funcs[0]
		if !left.ReturnType().isNumberKind() {
			err := fmt.Errorf("operator %v: left side expression %v evaluate to %v, not number", operator, left, left.ReturnType())
			return nil, errInvalidDataType(err)
		}

		right := funcs[1]
		if !right.ReturnType().isNumberKind() {
			err := fmt.Errorf("operator %v: right side expression %v evaluate to %v; not number", operator, right, right.ReturnType())
			return nil, errInvalidDataType(err)
		}

		if left.Type() == aggregateFunction || right.Type() == aggregateFunction {
			funcType = aggregateFunction
			switch left.Type() {
			case Int, Float, aggregateFunction:
			default:
				err := fmt.Errorf("operator %v: left side expression %v return type %v is incompatible for aggregate evaluation", operator, left, left.Type())
				return nil, errUnsupportedSQLOperation(err)
			}

			switch right.Type() {
			case Int, Float, aggregateFunction:
			default:
				err := fmt.Errorf("operator %v: right side expression %v return type %v is incompatible for aggregate evaluation", operator, right, right.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
		}

		return &comparisonExpr{
			left:     left,
			right:    right,
			operator: operator,
			funcType: funcType,
		}, nil

	case In, NotIn:
		if len(funcs) != 2 {
			panic(fmt.Sprintf("exactly two arguments are expected, but found %v", len(funcs)))
		}

		left := funcs[0]
		if !left.ReturnType().isBaseKind() {
			err := fmt.Errorf("operator %v: left side expression %v evaluate to %v is incompatible for equality check", operator, left, left.ReturnType())
			return nil, errInvalidDataType(err)
		}

		right := funcs[1]
		if right.ReturnType() != Array {
			err := fmt.Errorf("operator %v: right side expression %v evaluate to %v is incompatible for equality check", operator, right, right.ReturnType())
			return nil, errInvalidDataType(err)
		}

		if left.Type() == aggregateFunction || right.Type() == aggregateFunction {
			funcType = aggregateFunction
			switch left.Type() {
			case column, Array, function, arithmeticFunction, comparisonFunction, logicalFunction, record:
				err := fmt.Errorf("operator %v: left side expression %v return type %v is incompatible for aggregate evaluation", operator, left, left.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
			switch right.Type() {
			case Array, aggregateFunction:
			default:
				err := fmt.Errorf("operator %v: right side expression %v return type %v is incompatible for aggregate evaluation", operator, right, right.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
		}

		return &comparisonExpr{
			left:     left,
			right:    right,
			operator: operator,
			funcType: funcType,
		}, nil

	case Like, NotLike:
		if len(funcs) != 2 {
			panic(fmt.Sprintf("exactly two arguments are expected, but found %v", len(funcs)))
		}

		left := funcs[0]
		if !left.ReturnType().isStringKind() {
			err := fmt.Errorf("operator %v: left side expression %v evaluate to %v, not string", operator, left, left.ReturnType())
			return nil, errLikeInvalidInputs(err)
		}

		right := funcs[1]
		if !right.ReturnType().isStringKind() {
			err := fmt.Errorf("operator %v: right side expression %v evaluate to %v, not string", operator, right, right.ReturnType())
			return nil, errLikeInvalidInputs(err)
		}

		if left.Type() == aggregateFunction || right.Type() == aggregateFunction {
			funcType = aggregateFunction
			switch left.Type() {
			case String, aggregateFunction:
			default:
				err := fmt.Errorf("operator %v: left side expression %v return type %v is incompatible for aggregate evaluation", operator, left, left.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
			switch right.Type() {
			case String, aggregateFunction:
			default:
				err := fmt.Errorf("operator %v: right side expression %v return type %v is incompatible for aggregate evaluation", operator, right, right.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
		}

		return &comparisonExpr{
			left:     left,
			right:    right,
			operator: operator,
			funcType: funcType,
		}, nil
	case Between, NotBetween:
		if len(funcs) != 3 {
			panic(fmt.Sprintf("too many values in funcs %v", funcs))
		}

		left := funcs[0]
		if !left.ReturnType().isNumberKind() {
			err := fmt.Errorf("operator %v: left side expression %v evaluate to %v, not number", operator, left, left.ReturnType())
			return nil, errInvalidDataType(err)
		}

		from := funcs[1]
		if !from.ReturnType().isNumberKind() {
			err := fmt.Errorf("operator %v: from expression %v evaluate to %v, not number", operator, from, from.ReturnType())
			return nil, errInvalidDataType(err)
		}

		to := funcs[2]
		if !to.ReturnType().isNumberKind() {
			err := fmt.Errorf("operator %v: to expression %v evaluate to %v, not number", operator, to, to.ReturnType())
			return nil, errInvalidDataType(err)
		}

		if left.Type() == aggregateFunction || from.Type() == aggregateFunction || to.Type() == aggregateFunction {
			funcType = aggregateFunction
			switch left.Type() {
			case Int, Float, aggregateFunction:
			default:
				err := fmt.Errorf("operator %v: left side expression %v return type %v is incompatible for aggregate evaluation", operator, left, left.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
			switch from.Type() {
			case Int, Float, aggregateFunction:
			default:
				err := fmt.Errorf("operator %v: from expression %v return type %v is incompatible for aggregate evaluation", operator, from, from.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
			switch to.Type() {
			case Int, Float, aggregateFunction:
			default:
				err := fmt.Errorf("operator %v: to expression %v return type %v is incompatible for aggregate evaluation", operator, to, to.Type())
				return nil, errUnsupportedSQLOperation(err)
			}
		}

		return &comparisonExpr{
			left:     left,
			right:    from,
			to:       to,
			operator: operator,
			funcType: funcType,
		}, nil
	case IsNull, IsNotNull:
		if len(funcs) != 1 {
			panic(fmt.Sprintf("too many values in funcs %v", funcs))
		}

		if funcs[0].Type() == aggregateFunction {
			funcType = aggregateFunction
		}

		if operator == IsNull {
			operator = Equal
		} else {
			operator = NotEqual
		}

		return &comparisonExpr{
			left:     funcs[0],
			right:    newValueExpr(NewNull()),
			operator: operator,
			funcType: funcType,
		}, nil
	}

	return nil, errParseUnknownOperator(fmt.Errorf("unknown operator %v", operator))
}
