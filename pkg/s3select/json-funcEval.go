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
	"strings"

	"github.com/xwb1989/sqlparser"
)

// evaluateIsExpr is a function for evaluating expressions of the form "column
// is ...."
func jsonEvaluateIsExpr(myFunc *sqlparser.IsExpr, row string, alias string) (bool, error) {
	operator := myFunc.Operator
	var myVal string
	switch myIs := myFunc.Expr.(type) {
	// case for literal val
	case *sqlparser.SQLVal:
		myVal = string(myIs.Val)
	// case for nested func val
	case *sqlparser.FuncExpr:
		//myVal = evaluateFuncExpr(myIs, "", row, columnNames)
		//To be implemented
	// case for col val
	case *sqlparser.ColName:
		myVal = jsonValue(myIs.Name.CompliantName(), row)
	}
	// case to evaluate is null
	if strings.ToLower(operator) == "is null" {
		return myVal == "", nil
	}
	// case to evaluate is not null
	if strings.ToLower(operator) == "is not null" {
		return myVal != "", nil
	}
	return false, ErrUnsupportedSQLOperation
}
