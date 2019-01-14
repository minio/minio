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
)

// Expr - a SQL expression type.
type Expr interface {
	AggregateValue() (*Value, error)
	Eval(record Record) (*Value, error)
	ReturnType() Type
	Type() Type
}

// aliasExpr - aliases expression by alias.
type aliasExpr struct {
	alias string
	expr  Expr
}

// String - returns string representation of this expression.
func (expr *aliasExpr) String() string {
	return fmt.Sprintf("(%v AS %v)", expr.expr, expr.alias)
}

// Eval - evaluates underlaying expression for given record and returns evaluated result.
func (expr *aliasExpr) Eval(record Record) (*Value, error) {
	return expr.expr.Eval(record)
}

// AggregateValue - returns aggregated value from underlaying expression.
func (expr *aliasExpr) AggregateValue() (*Value, error) {
	return expr.expr.AggregateValue()
}

// Type - returns underlaying expression type.
func (expr *aliasExpr) Type() Type {
	return expr.expr.Type()
}

// ReturnType - returns underlaying expression's return type.
func (expr *aliasExpr) ReturnType() Type {
	return expr.expr.ReturnType()
}

// newAliasExpr - creates new alias expression.
func newAliasExpr(alias string, expr Expr) *aliasExpr {
	return &aliasExpr{alias, expr}
}

// starExpr - asterisk (*) expression.
type starExpr struct {
}

// String - returns string representation of this expression.
func (expr *starExpr) String() string {
	return "*"
}

// Eval - returns given args as map value.
func (expr *starExpr) Eval(record Record) (*Value, error) {
	return newRecordValue(record), nil
}

// AggregateValue - returns nil value.
func (expr *starExpr) AggregateValue() (*Value, error) {
	return nil, nil
}

// Type - returns record type.
func (expr *starExpr) Type() Type {
	return record
}

// ReturnType - returns record as return type.
func (expr *starExpr) ReturnType() Type {
	return record
}

// newStarExpr - returns new asterisk (*) expression.
func newStarExpr() *starExpr {
	return &starExpr{}
}

type valueExpr struct {
	value *Value
}

func (expr *valueExpr) String() string {
	return expr.value.String()
}

func (expr *valueExpr) Eval(record Record) (*Value, error) {
	return expr.value, nil
}

func (expr *valueExpr) AggregateValue() (*Value, error) {
	return expr.value, nil
}

func (expr *valueExpr) Type() Type {
	return expr.value.Type()
}

func (expr *valueExpr) ReturnType() Type {
	return expr.value.Type()
}

func newValueExpr(value *Value) *valueExpr {
	return &valueExpr{value: value}
}

type columnExpr struct {
	name string
}

func (expr *columnExpr) String() string {
	return expr.name
}

func (expr *columnExpr) Eval(record Record) (*Value, error) {
	value, err := record.Get(expr.name)
	if err != nil {
		return nil, errEvaluatorBindingDoesNotExist(err)
	}

	return value, nil
}

func (expr *columnExpr) AggregateValue() (*Value, error) {
	return nil, nil
}

func (expr *columnExpr) Type() Type {
	return column
}

func (expr *columnExpr) ReturnType() Type {
	return column
}

func newColumnExpr(columnName string) *columnExpr {
	return &columnExpr{name: columnName}
}
