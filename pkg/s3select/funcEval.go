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

// stringOps is a function which handles the case in a clause if there is a need
// to perform a string function
func stringOps(myFunc *sqlparser.FuncExpr, record []string, myReturnVal string, columnsMap map[string]int) string {
	var value string
	funcName := myFunc.Name.CompliantName()
	switch tempArg := myFunc.Exprs[0].(type) {
	case *sqlparser.AliasedExpr:
		switch col := tempArg.Expr.(type) {
		case *sqlparser.FuncExpr:
			// myReturnVal is actually the tail recursive value being used in the eval func.
			return applyStrFunc(myReturnVal, funcName)
		case *sqlparser.ColName:
			value = applyStrFunc(record[columnsMap[col.Name.CompliantName()]], funcName)
		case *sqlparser.SQLVal:
			value = applyStrFunc(string(col.Val), funcName)
		}
	}
	return value
}

// coalOps is a function which decomposes a COALESCE func expr into its struct.
func coalOps(myFunc *sqlparser.FuncExpr, record []string, myReturnVal string, columnsMap map[string]int) string {
	myArgs := make([]string, len(myFunc.Exprs))

	for i := 0; i < len(myFunc.Exprs); i++ {
		switch tempArg := myFunc.Exprs[i].(type) {
		case *sqlparser.AliasedExpr:
			switch col := tempArg.Expr.(type) {
			case *sqlparser.FuncExpr:
				// myReturnVal is actually the tail recursive value being used in the eval func.
				return myReturnVal
			case *sqlparser.ColName:
				myArgs[i] = record[columnsMap[col.Name.CompliantName()]]
			case *sqlparser.SQLVal:
				myArgs[i] = string(col.Val)
			}
		}
	}
	return processCoalNoIndex(myArgs)
}

// nullOps is a function which decomposes a NullIf func expr into its struct.
func nullOps(myFunc *sqlparser.FuncExpr, record []string, myReturnVal string, columnsMap map[string]int) string {
	myArgs := make([]string, 2)

	for i := 0; i < len(myFunc.Exprs); i++ {
		switch tempArg := myFunc.Exprs[i].(type) {
		case *sqlparser.AliasedExpr:
			switch col := tempArg.Expr.(type) {
			case *sqlparser.FuncExpr:
				return myReturnVal
			case *sqlparser.ColName:
				myArgs[i] = record[columnsMap[col.Name.CompliantName()]]
			case *sqlparser.SQLVal:
				myArgs[i] = string(col.Val)
			}
		}
	}
	return processNullIf(myArgs)
}

// isValidString is a function that ensures the current index is one with a
// StrFunc
func isValidFunc(myList []int, index int) bool {
	if myList == nil {
		return false
	}
	for i := 0; i < len(myList); i++ {
		if myList[i] == index {
			return true
		}
	}
	return false
}

// processNullIf is a function that evaluates a given NULLIF clause.
func processNullIf(nullStore []string) string {
	nullValOne := nullStore[0]
	nullValTwo := nullStore[1]
	if nullValOne == nullValTwo {
		return ""
	}
	return nullValOne
}

// processCoalNoIndex is a function which evaluates a given COALESCE clause.
func processCoalNoIndex(coalStore []string) string {
	for i := 0; i < len(coalStore); i++ {
		if coalStore[i] != "null" && coalStore[i] != "missing" && coalStore[i] != "" {
			return coalStore[i]
		}
	}
	return "null"
}

// evaluateFuncExpr is a function that allows for tail recursive evaluation of
// nested function expressions.
func evaluateFuncExpr(myVal *sqlparser.FuncExpr, myReturnVal string, myRecord []string, columnsMap map[string]int) string {
	if myVal == nil {
		return myReturnVal
	}
	// retrieve all the relevant arguments of the function
	var mySubFunc []*sqlparser.FuncExpr
	mySubFunc = make([]*sqlparser.FuncExpr, len(myVal.Exprs))
	for i := 0; i < len(myVal.Exprs); i++ {
		switch col := myVal.Exprs[i].(type) {
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
				return stringOps(myVal, myRecord, evaluateFuncExpr(mySubFunc[i], myReturnVal, myRecord, columnsMap), columnsMap)
			}
			return stringOps(myVal, myRecord, myReturnVal, columnsMap)
		} else if strings.ToUpper(myVal.Name.CompliantName()) == "NULLIF" {
			if mySubFunc != nil {
				return nullOps(myVal, myRecord, evaluateFuncExpr(mySubFunc[i], myReturnVal, myRecord, columnsMap), columnsMap)
			}
			return nullOps(myVal, myRecord, myReturnVal, columnsMap)
		} else if strings.ToUpper(myVal.Name.CompliantName()) == "COALESCE" {
			if mySubFunc != nil {
				return coalOps(myVal, myRecord, evaluateFuncExpr(mySubFunc[i], myReturnVal, myRecord, columnsMap), columnsMap)
			}
			return coalOps(myVal, myRecord, myReturnVal, columnsMap)
		}
	}
	return ""
}

// evaluateFuncErr is a function that flags errors in nested functions.
func (reader *Input) evaluateFuncErr(myVal *sqlparser.FuncExpr) error {
	if myVal == nil {
		return nil
	}
	if !supportedFunc(myVal.Name.CompliantName()) {
		return ErrUnsupportedSQLOperation
	}
	for i := 0; i < len(myVal.Exprs); i++ {
		switch tempArg := myVal.Exprs[i].(type) {
		case *sqlparser.StarExpr:
			return ErrParseUnsupportedCallWithStar
		case *sqlparser.AliasedExpr:
			switch col := tempArg.Expr.(type) {
			case *sqlparser.FuncExpr:
				if err := reader.evaluateFuncErr(col); err != nil {
					return err
				}
			case *sqlparser.ColName:
				if err := reader.colNameErrs([]string{col.Name.CompliantName()}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// evaluateIsExpr is a function for evaluating expressions of the form "column
// is ...."
func evaluateIsExpr(myFunc *sqlparser.IsExpr, row []string, columnNames map[string]int, alias string) (bool, error) {
	operator := myFunc.Operator
	var colName string
	var myVal string
	switch myIs := myFunc.Expr.(type) {
	// case for literal val
	case *sqlparser.SQLVal:
		myVal = string(myIs.Val)
	// case for nested func val
	case *sqlparser.FuncExpr:
		myVal = evaluateFuncExpr(myIs, "", row, columnNames)
	// case for col val
	case *sqlparser.ColName:
		colName = cleanCol(myIs.Name.CompliantName(), alias)
	}
	// case if it is a col val
	if colName != "" {
		myVal = row[columnNames[colName]]
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

// supportedString is a function that checks whether the function is a supported
// string one
func supportedString(strFunc string) bool {
	return stringInSlice(strings.ToUpper(strFunc), []string{"TRIM", "SUBSTRING", "CHAR_LENGTH", "CHARACTER_LENGTH", "LOWER", "UPPER"})
}

// supportedFunc is a function that checks whether the function is a supported
// S3 one.
func supportedFunc(strFunc string) bool {
	return stringInSlice(strings.ToUpper(strFunc), []string{"TRIM", "SUBSTRING", "CHAR_LENGTH", "CHARACTER_LENGTH", "LOWER", "UPPER", "COALESCE", "NULLIF"})
}
