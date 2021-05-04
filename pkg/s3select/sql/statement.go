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
	"errors"
	"fmt"
	"strings"

	"github.com/bcicen/jstream"
	"github.com/minio/simdjson-go"
)

var (
	errBadLimitSpecified = errors.New("Limit value must be a positive integer")
)

const (
	baseTableName = "s3object"
)

// SelectStatement is the top level parsed and analyzed structure
type SelectStatement struct {
	selectAST *Select

	// Analysis result of the statement
	selectQProp qProp

	// Result of parsing the limit clause if one is present
	// (otherwise -1)
	limitValue int64

	// Count of rows that have been output.
	outputCount int64

	// Table alias
	tableAlias string
}

// ParseSelectStatement - parses a select query from the given string
// and analyzes it.
func ParseSelectStatement(s string) (stmt SelectStatement, err error) {
	var selectAST Select
	err = SQLParser.ParseString(s, &selectAST)
	if err != nil {
		err = errQueryParseFailure(err)
		return
	}

	// Check if select is "SELECT s.* from S3Object s"
	if !selectAST.Expression.All &&
		len(selectAST.Expression.Expressions) == 1 &&
		len(selectAST.Expression.Expressions[0].Expression.And) == 1 &&
		len(selectAST.Expression.Expressions[0].Expression.And[0].Condition) == 1 &&
		selectAST.Expression.Expressions[0].Expression.And[0].Condition[0].Operand != nil &&
		selectAST.Expression.Expressions[0].Expression.And[0].Condition[0].Operand.Operand.Left != nil &&
		selectAST.Expression.Expressions[0].Expression.And[0].Condition[0].Operand.Operand.Left.Left != nil &&
		selectAST.Expression.Expressions[0].Expression.And[0].Condition[0].Operand.Operand.Left.Left.Primary != nil &&
		selectAST.Expression.Expressions[0].Expression.And[0].Condition[0].Operand.Operand.Left.Left.Primary.JPathExpr != nil {
		if selectAST.Expression.Expressions[0].Expression.And[0].Condition[0].Operand.Operand.Left.Left.Primary.JPathExpr.String() == selectAST.From.As+".*" {
			selectAST.Expression.All = true
		}
	}
	stmt.selectAST = &selectAST

	// Check the parsed limit value
	stmt.limitValue, err = parseLimit(selectAST.Limit)
	if err != nil {
		err = errQueryAnalysisFailure(err)
		return
	}

	// Analyze where clause
	if selectAST.Where != nil {
		whereQProp := selectAST.Where.analyze(&selectAST)
		if whereQProp.err != nil {
			err = errQueryAnalysisFailure(fmt.Errorf("Where clause error: %w", whereQProp.err))
			return
		}

		if whereQProp.isAggregation {
			err = errQueryAnalysisFailure(errors.New("WHERE clause cannot have an aggregation"))
			return
		}
	}

	// Validate table name
	err = validateTableName(selectAST.From)
	if err != nil {
		return
	}

	// Analyze main select expression
	stmt.selectQProp = selectAST.Expression.analyze(&selectAST)
	err = stmt.selectQProp.err
	if err != nil {
		err = errQueryAnalysisFailure(err)
	}

	// Set table alias
	stmt.tableAlias = selectAST.From.As
	return
}

func validateTableName(from *TableExpression) error {
	if strings.ToLower(from.Table.BaseKey.String()) != baseTableName {
		return errBadTableName(errors.New("table name must be `s3object`"))
	}

	if len(from.Table.PathExpr) > 0 {
		if !from.Table.PathExpr[0].ArrayWildcard {
			return errBadTableName(errors.New("keypath table name is invalid - please check the service documentation"))
		}
	}
	return nil
}

func parseLimit(v *LitValue) (int64, error) {
	switch {
	case v == nil:
		return -1, nil
	case v.Int == nil:
		return -1, errBadLimitSpecified
	default:
		r := int64(*v.Int)
		if r < 0 {
			return -1, errBadLimitSpecified
		}
		return r, nil
	}
}

// EvalFrom evaluates the From clause on the input record. It only
// applies to JSON input data format (currently).
func (e *SelectStatement) EvalFrom(format string, input Record) ([]*Record, error) {
	if !e.selectAST.From.HasKeypath() {
		return []*Record{&input}, nil
	}
	_, rawVal := input.Raw()

	if format != "json" {
		return nil, errDataSource(errors.New("path not supported"))
	}
	switch rec := rawVal.(type) {
	case jstream.KVS:
		txedRec, _, err := jsonpathEval(e.selectAST.From.Table.PathExpr[1:], rec)
		if err != nil {
			return nil, err
		}

		var kvs jstream.KVS
		switch v := txedRec.(type) {
		case jstream.KVS:
			kvs = v

		case []interface{}:
			recs := make([]*Record, len(v))
			for i, val := range v {
				tmpRec := input.Clone(nil)
				if err = tmpRec.Replace(val); err != nil {
					return nil, err
				}
				recs[i] = &tmpRec
			}
			return recs, nil

		default:
			kvs = jstream.KVS{jstream.KV{Key: "_1", Value: v}}
		}

		if err = input.Replace(kvs); err != nil {
			return nil, err
		}

		return []*Record{&input}, nil
	case simdjson.Object:
		txedRec, _, err := jsonpathEval(e.selectAST.From.Table.PathExpr[1:], rec)
		if err != nil {
			return nil, err
		}

		switch v := txedRec.(type) {
		case simdjson.Object:
			err := input.Replace(v)
			if err != nil {
				return nil, err
			}

		case []interface{}:
			recs := make([]*Record, len(v))
			for i, val := range v {
				tmpRec := input.Clone(nil)
				if err = tmpRec.Replace(val); err != nil {
					return nil, err
				}
				recs[i] = &tmpRec
			}
			return recs, nil

		default:
			input.Reset()
			input, err = input.Set("_1", &Value{value: v})
			if err != nil {
				return nil, err
			}
		}
		return []*Record{&input}, nil
	}
	return nil, errDataSource(errors.New("unexpected non JSON input"))
}

// IsAggregated returns if the statement involves SQL aggregation
func (e *SelectStatement) IsAggregated() bool {
	return e.selectQProp.isAggregation
}

// AggregateResult - returns the aggregated result after all input
// records have been processed. Applies only to aggregation queries.
func (e *SelectStatement) AggregateResult(output Record) error {
	for i, expr := range e.selectAST.Expression.Expressions {
		v, err := expr.evalNode(nil, e.tableAlias)
		if err != nil {
			return err
		}
		if expr.As != "" {
			output, err = output.Set(expr.As, v)
		} else {
			output, err = output.Set(fmt.Sprintf("_%d", i+1), v)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *SelectStatement) isPassingWhereClause(input Record) (bool, error) {
	if e.selectAST.Where == nil {
		return true, nil
	}
	value, err := e.selectAST.Where.evalNode(input, e.tableAlias)
	if err != nil {
		return false, err
	}

	b, ok := value.ToBool()
	if !ok {
		err = fmt.Errorf("WHERE expression did not return bool")
		return false, err
	}

	return b, nil
}

// AggregateRow - aggregates the input record. Applies only to
// aggregation queries.
func (e *SelectStatement) AggregateRow(input Record) error {
	ok, err := e.isPassingWhereClause(input)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	for _, expr := range e.selectAST.Expression.Expressions {
		err := expr.aggregateRow(input, e.tableAlias)
		if err != nil {
			return err
		}
	}
	return nil
}

// Eval - evaluates the Select statement for the given record. It
// applies only to non-aggregation queries.
// The function returns whether the statement passed the WHERE clause and should be outputted.
func (e *SelectStatement) Eval(input, output Record) (Record, error) {
	ok, err := e.isPassingWhereClause(input)
	if err != nil || !ok {
		// Either error or row did not pass where clause
		return nil, err
	}

	if e.selectAST.Expression.All {
		// Return the input record for `SELECT * FROM
		// .. WHERE ..`

		// Update count of records output.
		if e.limitValue > -1 {
			e.outputCount++
		}
		return input.Clone(output), nil
	}

	for i, expr := range e.selectAST.Expression.Expressions {
		v, err := expr.evalNode(input, e.tableAlias)
		if err != nil {
			return nil, err
		}

		// Pick output column names
		if expr.As != "" {
			output, err = output.Set(expr.As, v)
		} else if comp, ok := getLastKeypathComponent(expr.Expression); ok {
			output, err = output.Set(comp, v)
		} else {
			output, err = output.Set(fmt.Sprintf("_%d", i+1), v)
		}
		if err != nil {
			return nil, err
		}
	}

	// Update count of records output.
	if e.limitValue > -1 {
		e.outputCount++
	}

	return output, nil
}

// LimitReached - returns true if the number of records output has
// reached the value of the `LIMIT` clause.
func (e *SelectStatement) LimitReached() bool {
	if e.limitValue == -1 {
		return false
	}
	return e.outputCount >= e.limitValue
}
