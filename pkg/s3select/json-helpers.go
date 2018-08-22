/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package s3select

import (
	"fmt"
	"io"
	"log"

	"github.com/tidwall/gjson"
	"github.com/xwb1989/sqlparser"
)

func (reader *JSONInput) jsonRead() map[string]interface{} {
	dec := reader.reader
	var m interface{}
	for {
		err := dec.Decode(&m)
		if err == io.EOF || err == io.ErrClosedPipe {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		return m.(map[string]interface{})
	}
	return nil
}

func jsonValue(input string, row string) string {
	value := gjson.Get(row, input)
	return value.String()

}

// It evaluates the where clause for JSON and return true if condition suffices
func jsonWhereClause(row string, alias string, whereClause interface{}) (bool, error) {

	var operator string
	var operand interface{}
	if fmt.Sprintf("%v", whereClause) == "false" {
		return false, nil
	}

	switch expr := whereClause.(type) {
	case *sqlparser.IsExpr:
		return jsonEvaluateIsExpr(expr, row, alias)
	case *sqlparser.RangeCond:
		// To be Implemented
	case *sqlparser.ComparisonExpr:
		operator = expr.Operator
		switch right := expr.Right.(type) {
		case *sqlparser.FuncExpr:
		// To be Implemented
		case *sqlparser.SQLVal:
			var err error
			operand, err = evaluateParserType(right)
			if err != nil {
				return false, err
			}
		}
		switch left := expr.Left.(type) {
		// case *sqlparser.FuncExpr:
		// To be Implemented
		case *sqlparser.ColName:
			return evaluateOperator(jsonValue((left.Name.CompliantName()), row), operator, operand)

		}

	case *sqlparser.AndExpr:
		var leftVal bool
		var rightVal bool
		switch left := expr.Left.(type) {
		case *sqlparser.ComparisonExpr:
			temp, err := jsonWhereClause(row, alias, left)
			if err != nil {
				return false, err
			}
			leftVal = temp
		}
		switch right := expr.Right.(type) {
		case *sqlparser.ComparisonExpr:
			temp, err := jsonWhereClause(row, alias, right)
			if err != nil {
				return false, err
			}
			rightVal = temp
		}
		return (rightVal && leftVal), nil

	case *sqlparser.OrExpr:
		var leftVal bool
		var rightVal bool
		switch left := expr.Left.(type) {
		case *sqlparser.ComparisonExpr:
			leftVal, _ = jsonWhereClause(row, alias, left)
		}
		switch right := expr.Right.(type) {
		case *sqlparser.ComparisonExpr:
			rightVal, _ = jsonWhereClause(row, alias, right)
		}
		return (rightVal || leftVal), nil

	}

	return true, nil
}
