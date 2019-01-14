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

// andExpr - logical AND function.
type andExpr struct {
	left     Expr
	right    Expr
	funcType Type
}

// String - returns string representation of this function.
func (f *andExpr) String() string {
	return fmt.Sprintf("(%v AND %v)", f.left, f.right)
}

// Call - evaluates this function for given arg values and returns result as Value.
func (f *andExpr) Eval(record Record) (*Value, error) {
	leftValue, err := f.left.Eval(record)
	if err != nil {
		return nil, err
	}

	if f.funcType == aggregateFunction {
		_, err = f.right.Eval(record)
		return nil, err
	}

	if leftValue.Type() != Bool {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to bool", f, leftValue.Type())
		return nil, errExternalEvalException(err)
	}

	if !leftValue.BoolValue() {
		return leftValue, nil
	}

	rightValue, err := f.right.Eval(record)
	if err != nil {
		return nil, err
	}
	if rightValue.Type() != Bool {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to bool", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return rightValue, nil
}

// AggregateValue - returns aggregated value.
func (f *andExpr) AggregateValue() (*Value, error) {
	if f.funcType != aggregateFunction {
		err := fmt.Errorf("%v is not aggreate expression", f)
		return nil, errExternalEvalException(err)
	}

	leftValue, err := f.left.AggregateValue()
	if err != nil {
		return nil, err
	}
	if leftValue.Type() != Bool {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to bool", f, leftValue.Type())
		return nil, errExternalEvalException(err)
	}

	if !leftValue.BoolValue() {
		return leftValue, nil
	}

	rightValue, err := f.right.AggregateValue()
	if err != nil {
		return nil, err
	}
	if rightValue.Type() != Bool {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to bool", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return rightValue, nil
}

// Type - returns logicalFunction or aggregateFunction type.
func (f *andExpr) Type() Type {
	return f.funcType
}

// ReturnType - returns Bool as return type.
func (f *andExpr) ReturnType() Type {
	return Bool
}

// newAndExpr - creates new AND logical function.
func newAndExpr(left, right Expr) (*andExpr, error) {
	if !left.ReturnType().isBoolKind() {
		err := fmt.Errorf("operator AND: left side expression %v evaluate to %v, not bool", left, left.ReturnType())
		return nil, errInvalidDataType(err)
	}

	if !right.ReturnType().isBoolKind() {
		err := fmt.Errorf("operator AND: right side expression %v evaluate to %v; not bool", right, right.ReturnType())
		return nil, errInvalidDataType(err)
	}

	funcType := logicalFunction
	if left.Type() == aggregateFunction || right.Type() == aggregateFunction {
		funcType = aggregateFunction
		if left.Type() == column {
			err := fmt.Errorf("operator AND: left side expression %v return type %v is incompatible for aggregate evaluation", left, left.Type())
			return nil, errUnsupportedSQLOperation(err)
		}

		if right.Type() == column {
			err := fmt.Errorf("operator AND: right side expression %v return type %v is incompatible for aggregate evaluation", right, right.Type())
			return nil, errUnsupportedSQLOperation(err)
		}
	}

	return &andExpr{
		left:     left,
		right:    right,
		funcType: funcType,
	}, nil
}

// orExpr - logical OR function.
type orExpr struct {
	left     Expr
	right    Expr
	funcType Type
}

// String - returns string representation of this function.
func (f *orExpr) String() string {
	return fmt.Sprintf("(%v OR %v)", f.left, f.right)
}

// Call - evaluates this function for given arg values and returns result as Value.
func (f *orExpr) Eval(record Record) (*Value, error) {
	leftValue, err := f.left.Eval(record)
	if err != nil {
		return nil, err
	}

	if f.funcType == aggregateFunction {
		_, err = f.right.Eval(record)
		return nil, err
	}

	if leftValue.Type() != Bool {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to bool", f, leftValue.Type())
		return nil, errExternalEvalException(err)
	}

	if leftValue.BoolValue() {
		return leftValue, nil
	}

	rightValue, err := f.right.Eval(record)
	if err != nil {
		return nil, err
	}
	if rightValue.Type() != Bool {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to bool", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return rightValue, nil
}

// AggregateValue - returns aggregated value.
func (f *orExpr) AggregateValue() (*Value, error) {
	if f.funcType != aggregateFunction {
		err := fmt.Errorf("%v is not aggreate expression", f)
		return nil, errExternalEvalException(err)
	}

	leftValue, err := f.left.AggregateValue()
	if err != nil {
		return nil, err
	}
	if leftValue.Type() != Bool {
		err := fmt.Errorf("%v: left side expression evaluated to %v; not to bool", f, leftValue.Type())
		return nil, errExternalEvalException(err)
	}

	if leftValue.BoolValue() {
		return leftValue, nil
	}

	rightValue, err := f.right.AggregateValue()
	if err != nil {
		return nil, err
	}
	if rightValue.Type() != Bool {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to bool", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return rightValue, nil
}

// Type - returns logicalFunction or aggregateFunction type.
func (f *orExpr) Type() Type {
	return f.funcType
}

// ReturnType - returns Bool as return type.
func (f *orExpr) ReturnType() Type {
	return Bool
}

// newOrExpr - creates new OR logical function.
func newOrExpr(left, right Expr) (*orExpr, error) {
	if !left.ReturnType().isBoolKind() {
		err := fmt.Errorf("operator OR: left side expression %v evaluate to %v, not bool", left, left.ReturnType())
		return nil, errInvalidDataType(err)
	}

	if !right.ReturnType().isBoolKind() {
		err := fmt.Errorf("operator OR: right side expression %v evaluate to %v; not bool", right, right.ReturnType())
		return nil, errInvalidDataType(err)
	}

	funcType := logicalFunction
	if left.Type() == aggregateFunction || right.Type() == aggregateFunction {
		funcType = aggregateFunction
		if left.Type() == column {
			err := fmt.Errorf("operator OR: left side expression %v return type %v is incompatible for aggregate evaluation", left, left.Type())
			return nil, errUnsupportedSQLOperation(err)
		}

		if right.Type() == column {
			err := fmt.Errorf("operator OR: right side expression %v return type %v is incompatible for aggregate evaluation", right, right.Type())
			return nil, errUnsupportedSQLOperation(err)
		}
	}

	return &orExpr{
		left:     left,
		right:    right,
		funcType: funcType,
	}, nil
}

// notExpr - logical NOT function.
type notExpr struct {
	right    Expr
	funcType Type
}

// String - returns string representation of this function.
func (f *notExpr) String() string {
	return fmt.Sprintf("(%v)", f.right)
}

// Call - evaluates this function for given arg values and returns result as Value.
func (f *notExpr) Eval(record Record) (*Value, error) {
	rightValue, err := f.right.Eval(record)
	if err != nil {
		return nil, err
	}

	if f.funcType == aggregateFunction {
		return nil, nil
	}

	if rightValue.Type() != Bool {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to bool", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return NewBool(!rightValue.BoolValue()), nil
}

// AggregateValue - returns aggregated value.
func (f *notExpr) AggregateValue() (*Value, error) {
	if f.funcType != aggregateFunction {
		err := fmt.Errorf("%v is not aggreate expression", f)
		return nil, errExternalEvalException(err)
	}

	rightValue, err := f.right.AggregateValue()
	if err != nil {
		return nil, err
	}
	if rightValue.Type() != Bool {
		err := fmt.Errorf("%v: right side expression evaluated to %v; not to bool", f, rightValue.Type())
		return nil, errExternalEvalException(err)
	}

	return NewBool(!rightValue.BoolValue()), nil
}

// Type - returns logicalFunction or aggregateFunction type.
func (f *notExpr) Type() Type {
	return f.funcType
}

// ReturnType - returns Bool as return type.
func (f *notExpr) ReturnType() Type {
	return Bool
}

// newNotExpr - creates new NOT logical function.
func newNotExpr(right Expr) (*notExpr, error) {
	if !right.ReturnType().isBoolKind() {
		err := fmt.Errorf("operator NOT: right side expression %v evaluate to %v; not bool", right, right.ReturnType())
		return nil, errInvalidDataType(err)
	}

	funcType := logicalFunction
	if right.Type() == aggregateFunction {
		funcType = aggregateFunction
	}

	return &notExpr{
		right:    right,
		funcType: funcType,
	}, nil
}
