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

import "fmt"

// ArithOperator - arithmetic operator.
type ArithOperator string

const (
	// Add operator '+'.
	Add ArithOperator = "+"

	// Subtract operator '-'.
	Subtract ArithOperator = "-"

	// Multiply operator '*'.
	Multiply ArithOperator = "*"

	// Divide operator '/'.
	Divide ArithOperator = "/"

	// Modulo operator '%'.
	Modulo ArithOperator = "%"
)

// arithExpr - arithmetic function.
type arithExpr struct {
	left     Expr
	right    Expr
	operator ArithOperator
	funcType Type
}

// String - returns string representation of this function.
func (f *arithExpr) String() string {
	return fmt.Sprintf("(%v %v %v)", f.left, f.operator, f.right)
}

func (f *arithExpr) compute(lv, rv *Value) (*Value, error) {
	leftValueType := lv.Type()
	rightValueType := rv.Type()
	if !leftValueType.isNumber() {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to number", f, leftValueType)
		return nil, errExternalEvalException(err)
	}
	if !rightValueType.isNumber() {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to number", f, rightValueType)
		return nil, errExternalEvalException(err)
	}

	leftValue := lv.FloatValue()
	rightValue := rv.FloatValue()

	var result float64
	switch f.operator {
	case Add:
		result = leftValue + rightValue
	case Subtract:
		result = leftValue - rightValue
	case Multiply:
		result = leftValue * rightValue
	case Divide:
		result = leftValue / rightValue
	case Modulo:
		result = float64(int64(leftValue) % int64(rightValue))
	}

	if leftValueType == Float || rightValueType == Float {
		return NewFloat(result), nil
	}

	return NewInt(int64(result)), nil
}

// Call - evaluates this function for given arg values and returns result as Value.
func (f *arithExpr) Eval(record Record) (*Value, error) {
	leftValue, err := f.left.Eval(record)
	if err != nil {
		return nil, err
	}

	rightValue, err := f.right.Eval(record)
	if err != nil {
		return nil, err
	}

	if f.funcType == aggregateFunction {
		return nil, nil
	}

	return f.compute(leftValue, rightValue)
}

// AggregateValue - returns aggregated value.
func (f *arithExpr) AggregateValue() (*Value, error) {
	if f.funcType != aggregateFunction {
		err := fmt.Errorf("%v is not aggreate expression", f)
		return nil, errExternalEvalException(err)
	}

	lv, err := f.left.AggregateValue()
	if err != nil {
		return nil, err
	}

	rv, err := f.right.AggregateValue()
	if err != nil {
		return nil, err
	}

	return f.compute(lv, rv)
}

// Type - returns arithmeticFunction or aggregateFunction type.
func (f *arithExpr) Type() Type {
	return f.funcType
}

// ReturnType - returns Float as return type.
func (f *arithExpr) ReturnType() Type {
	return Float
}

// newArithExpr - creates new arithmetic function.
func newArithExpr(operator ArithOperator, left, right Expr) (*arithExpr, error) {
	if !left.ReturnType().isNumberKind() {
		err := fmt.Errorf("operator %v: left side expression %v evaluate to %v, not number", operator, left, left.ReturnType())
		return nil, errInvalidDataType(err)
	}

	if !right.ReturnType().isNumberKind() {
		err := fmt.Errorf("operator %v: right side expression %v evaluate to %v; not number", operator, right, right.ReturnType())
		return nil, errInvalidDataType(err)
	}

	funcType := arithmeticFunction
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

	return &arithExpr{
		left:     left,
		right:    right,
		operator: operator,
		funcType: funcType,
	}, nil
}
