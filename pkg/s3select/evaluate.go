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

	"github.com/tidwall/gjson"
	"github.com/xwb1989/sqlparser"

	"github.com/minio/minio/pkg/s3select/format"
)

// stringOps is a function which handles the case in a clause
// if there is a need to perform a string function
func stringOps(myFunc *sqlparser.FuncExpr, record []byte, myReturnVal string) string {
	var value string
	funcName := myFunc.Name.CompliantName()
	switch tempArg := myFunc.Exprs[0].(type) {
	case *sqlparser.AliasedExpr:
		switch col := tempArg.Expr.(type) {
		case *sqlparser.FuncExpr:
			// myReturnVal is actually the tail recursive value being used in the eval func.
			return applyStrFunc(gjson.Parse(myReturnVal), funcName)
		case *sqlparser.ColName:
			value = applyStrFunc(gjson.GetBytes(record, col.Name.CompliantName()), funcName)
		case *sqlparser.SQLVal:
			value = applyStrFunc(gjson.ParseBytes(col.Val), funcName)
		}
	}
	return value
}

// coalOps is a function which decomposes a COALESCE func expr into its struct.
func coalOps(myFunc *sqlparser.FuncExpr, record []byte, myReturnVal string) string {
	myArgs := make([]string, len(myFunc.Exprs))

	for i, expr := range myFunc.Exprs {
		switch tempArg := expr.(type) {
		case *sqlparser.AliasedExpr:
			switch col := tempArg.Expr.(type) {
			case *sqlparser.FuncExpr:
				// myReturnVal is actually the tail recursive value being used in the eval func.
				return myReturnVal
			case *sqlparser.ColName:
				myArgs[i] = gjson.GetBytes(record, col.Name.CompliantName()).String()
			case *sqlparser.SQLVal:
				myArgs[i] = string(col.Val)
			}
		}
	}
	return processCoalNoIndex(myArgs)
}

// nullOps is a function which decomposes a NullIf func expr into its struct.
func nullOps(myFunc *sqlparser.FuncExpr, record []byte, myReturnVal string) string {
	myArgs := make([]string, 2)

	for i, expr := range myFunc.Exprs {
		switch tempArg := expr.(type) {
		case *sqlparser.AliasedExpr:
			switch col := tempArg.Expr.(type) {
			case *sqlparser.FuncExpr:
				return myReturnVal
			case *sqlparser.ColName:
				myArgs[i] = gjson.GetBytes(record, col.Name.CompliantName()).String()
			case *sqlparser.SQLVal:
				myArgs[i] = string(col.Val)
			}
		}
	}
	if myArgs[0] == myArgs[1] {
		return ""
	}
	return myArgs[0]
}

// isValidString is a function that ensures the
// current index is one with a StrFunc
func isValidFunc(myList []int, index int) bool {
	if myList == nil {
		return false
	}
	for _, i := range myList {
		if i == index {
			return true
		}
	}
	return false
}

// processCoalNoIndex is a function which evaluates a given COALESCE clause.
func processCoalNoIndex(coalStore []string) string {
	for _, coal := range coalStore {
		if coal != "null" && coal != "missing" && coal != "" {
			return coal
		}
	}
	return "null"
}

// evaluateFuncExpr is a function that allows for tail recursive evaluation of
// nested function expressions
func evaluateFuncExpr(myVal *sqlparser.FuncExpr, myReturnVal string, record []byte) string {
	if myVal == nil {
		return myReturnVal
	}
	// retrieve all the relevant arguments of the function
	var mySubFunc []*sqlparser.FuncExpr
	mySubFunc = make([]*sqlparser.FuncExpr, len(myVal.Exprs))
	for i, expr := range myVal.Exprs {
		switch col := expr.(type) {
		case *sqlparser.AliasedExpr:
			switch temp := col.Expr.(type) {
			case *sqlparser.FuncExpr:
				mySubFunc[i] = temp
			}
		}
	}
	// Need to do tree recursion so as to explore all possible directions of the
	// nested function recursion
	for i := 0; i < len(mySubFunc); i++ {
		if supportedString(myVal.Name.CompliantName()) {
			if mySubFunc != nil {
				return stringOps(myVal, record, evaluateFuncExpr(mySubFunc[i], myReturnVal, record))
			}
			return stringOps(myVal, record, myReturnVal)
		} else if strings.ToUpper(myVal.Name.CompliantName()) == "NULLIF" {
			if mySubFunc != nil {
				return nullOps(myVal, record, evaluateFuncExpr(mySubFunc[i], myReturnVal, record))
			}
			return nullOps(myVal, record, myReturnVal)
		} else if strings.ToUpper(myVal.Name.CompliantName()) == "COALESCE" {
			if mySubFunc != nil {
				return coalOps(myVal, record, evaluateFuncExpr(mySubFunc[i], myReturnVal, record))
			}
			return coalOps(myVal, record, myReturnVal)
		}
	}
	return ""
}

// evaluateFuncErr is a function that flags errors in nested functions.
func evaluateFuncErr(myVal *sqlparser.FuncExpr, reader format.Select) error {
	if myVal == nil {
		return nil
	}
	if !supportedFunc(myVal.Name.CompliantName()) {
		return ErrUnsupportedSQLOperation
	}
	for _, expr := range myVal.Exprs {
		switch tempArg := expr.(type) {
		case *sqlparser.StarExpr:
			return ErrParseUnsupportedCallWithStar
		case *sqlparser.AliasedExpr:
			switch col := tempArg.Expr.(type) {
			case *sqlparser.FuncExpr:
				if err := evaluateFuncErr(col, reader); err != nil {
					return err
				}
			case *sqlparser.ColName:
				if err := reader.ColNameErrs([]string{col.Name.CompliantName()}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// evaluateIsExpr is a function for evaluating expressions of the form "column is ...."
func evaluateIsExpr(myFunc *sqlparser.IsExpr, row []byte, alias string) (bool, error) {
	getMyVal := func() (myVal string) {
		switch myIs := myFunc.Expr.(type) {
		// case for literal val
		case *sqlparser.SQLVal:
			myVal = string(myIs.Val)
			// case for nested func val
		case *sqlparser.FuncExpr:
			myVal = evaluateFuncExpr(myIs, "", row)
			// case for col val
		case *sqlparser.ColName:
			myVal = gjson.GetBytes(row, myIs.Name.CompliantName()).String()
		}
		return myVal
	}

	operator := strings.ToLower(myFunc.Operator)
	switch operator {
	case "is null":
		return getMyVal() == "", nil
	case "is not null":
		return getMyVal() != "", nil
	default:
		return false, ErrUnsupportedSQLOperation
	}
}

// supportedString is a function that checks whether the function is a supported
// string one
func supportedString(strFunc string) bool {
	return format.StringInSlice(strings.ToUpper(strFunc), []string{"TRIM", "SUBSTRING", "CHAR_LENGTH", "CHARACTER_LENGTH", "LOWER", "UPPER"})
}

// supportedFunc is a function that checks whether the function is a supported
// S3 one.
func supportedFunc(strFunc string) bool {
	return format.StringInSlice(strings.ToUpper(strFunc), []string{"TRIM", "SUBSTRING", "CHAR_LENGTH", "CHARACTER_LENGTH", "LOWER", "UPPER", "COALESCE", "NULLIF"})
}
