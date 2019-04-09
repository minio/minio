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
	"strings"

	"github.com/bcicen/jstream"
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
			err = errQueryAnalysisFailure(fmt.Errorf("Where clause error: %v", whereQProp.err))
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
	case v.Number == nil:
		return -1, errBadLimitSpecified
	default:
		r := int64(*v.Number)
		if r < 0 {
			return -1, errBadLimitSpecified
		}
		return r, nil
	}
}

// EvalFrom evaluates the From clause on the input record. It only
// applies to JSON input data format (currently).
func (e *SelectStatement) EvalFrom(format string, input Record) (Record, error) {
	if e.selectAST.From.HasKeypath() {
		if format == "json" {
			objFmt, rawVal := input.Raw()
			if objFmt != SelectFmtJSON {
				return nil, errDataSource(errors.New("unexpected non JSON input"))
			}

			jsonRec := rawVal.(jstream.KVS)
			txedRec, err := jsonpathEval(e.selectAST.From.Table.PathExpr[1:], jsonRec)
			if err != nil {
				return nil, err
			}

			var kvs jstream.KVS
			switch v := txedRec.(type) {
			case jstream.KVS:
				kvs = v
			default:
				kvs = jstream.KVS{jstream.KV{Key: "_1", Value: v}}
			}

			if err = input.Replace(kvs); err != nil {
				return nil, err
			}

			return input, nil
		}
		return nil, errDataSource(errors.New("path not supported"))
	}
	return input, nil
}

// IsAggregated returns if the statement involves SQL aggregation
func (e *SelectStatement) IsAggregated() bool {
	return e.selectQProp.isAggregation
}

// AggregateResult - returns the aggregated result after all input
// records have been processed. Applies only to aggregation queries.
func (e *SelectStatement) AggregateResult(output Record) error {
	for i, expr := range e.selectAST.Expression.Expressions {
		v, err := expr.evalNode(nil)
		if err != nil {
			return err
		}
		output.Set(fmt.Sprintf("_%d", i+1), v)
	}
	return nil
}

func (e *SelectStatement) isPassingWhereClause(input Record) (bool, error) {
	if e.selectAST.Where == nil {
		return true, nil
	}
	value, err := e.selectAST.Where.evalNode(input)
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
		err := expr.aggregateRow(input)
		if err != nil {
			return err
		}
	}
	return nil
}

// Eval - evaluates the Select statement for the given record. It
// applies only to non-aggregation queries.
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

		return input, nil
	}

	for i, expr := range e.selectAST.Expression.Expressions {
		v, err := expr.evalNode(input)
		if err != nil {
			return nil, err
		}

		// Pick output column names
		if expr.As != "" {
			output.Set(expr.As, v)
		} else if comp, ok := getLastKeypathComponent(expr.Expression); ok {
			output.Set(comp, v)
		} else {
			output.Set(fmt.Sprintf("_%d", i+1), v)
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
