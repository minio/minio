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
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio/pkg/s3select/format"
	"github.com/xwb1989/sqlparser"
)

// SelectFuncs contains the relevant values from the parser for S3 Select
// Functions
type SelectFuncs struct {
	funcExpr []*sqlparser.FuncExpr
	index    []int
}

// RunSqlParser allows us to easily bundle all the functions from above and run
// them in the appropriate order.
func runSelectParser(f format.Select, myRow chan Row) {
	reqCols, alias, myLimit, whereClause, aggFunctionNames, myFuncs, myErr := ParseSelect(f)
	if myErr != nil {
		myRow <- Row{
			err: myErr,
		}
		return
	}
	processSelectReq(reqCols, alias, whereClause, myLimit, aggFunctionNames, myRow, myFuncs, f)

}

// ParseSelect parses the SELECT expression, and effectively tokenizes it into
// its separate parts. It returns the requested column names,alias,limit of
// records, and the where clause.
func ParseSelect(f format.Select) ([]string, string, int64, interface{}, []string, SelectFuncs, error) {
	var sFuncs = SelectFuncs{}
	var whereClause interface{}
	var alias string
	var limit int64

	stmt, err := sqlparser.Parse(f.Expression())
	// TODO Maybe can parse their errors a bit to return some more of the s3 errors
	if err != nil {
		return nil, "", 0, nil, nil, sFuncs, ErrLexerInvalidChar
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// evaluates the where clause
		functionNames := make([]string, len(stmt.SelectExprs))
		columnNames := make([]string, len(stmt.SelectExprs))

		if stmt.Where != nil {
			switch expr := stmt.Where.Expr.(type) {
			default:
				whereClause = expr
			case *sqlparser.ComparisonExpr:
				whereClause = expr
			}
		}
		if stmt.SelectExprs != nil {
			for i := 0; i < len(stmt.SelectExprs); i++ {
				switch expr := stmt.SelectExprs[i].(type) {
				case *sqlparser.StarExpr:
					columnNames[0] = "*"
				case *sqlparser.AliasedExpr:
					switch smallerexpr := expr.Expr.(type) {
					case *sqlparser.FuncExpr:
						if smallerexpr.IsAggregate() {
							functionNames[i] = smallerexpr.Name.CompliantName()
							// Will return function name
							// Case to deal with if we have functions and not an asterix
							switch tempagg := smallerexpr.Exprs[0].(type) {
							case *sqlparser.StarExpr:
								columnNames[0] = "*"
								if smallerexpr.Name.CompliantName() != "count" {
									return nil, "", 0, nil, nil, sFuncs, ErrParseUnsupportedCallWithStar
								}
							case *sqlparser.AliasedExpr:
								switch col := tempagg.Expr.(type) {
								case *sqlparser.BinaryExpr:
									return nil, "", 0, nil, nil, sFuncs, ErrParseNonUnaryAgregateFunctionCall
								case *sqlparser.ColName:
									columnNames[i] = col.Name.CompliantName()
								}
							}
							// Case to deal with if COALESCE was used..
						} else if supportedFunc(smallerexpr.Name.CompliantName()) {
							if sFuncs.funcExpr == nil {
								sFuncs.funcExpr = make([]*sqlparser.FuncExpr, len(stmt.SelectExprs))
								sFuncs.index = make([]int, len(stmt.SelectExprs))
							}
							sFuncs.funcExpr[i] = smallerexpr
							sFuncs.index[i] = i
						} else {
							return nil, "", 0, nil, nil, sFuncs, ErrUnsupportedSQLOperation
						}
					case *sqlparser.ColName:
						columnNames[i] = smallerexpr.Name.CompliantName()
					}
				}
			}
		}

		// This code retrieves the alias and makes sure it is set to the correct
		// value, if not it sets it to the tablename
		if (stmt.From) != nil {
			for i := 0; i < len(stmt.From); i++ {
				switch smallerexpr := stmt.From[i].(type) {
				case *sqlparser.JoinTableExpr:
					return nil, "", 0, nil, nil, sFuncs, ErrParseMalformedJoin
				case *sqlparser.AliasedTableExpr:
					alias = smallerexpr.As.CompliantName()
					if alias == "" {
						alias = sqlparser.GetTableName(smallerexpr.Expr).CompliantName()
					}
				}
			}
		}
		if stmt.Limit != nil {
			switch expr := stmt.Limit.Rowcount.(type) {
			case *sqlparser.SQLVal:
				// The Value of how many rows we're going to limit by
				parsedLimit, _ := strconv.Atoi(string(expr.Val[:]))
				limit = int64(parsedLimit)
			}
		}
		if stmt.GroupBy != nil {
			return nil, "", 0, nil, nil, sFuncs, ErrParseUnsupportedLiteralsGroupBy
		}
		if stmt.OrderBy != nil {
			return nil, "", 0, nil, nil, sFuncs, ErrParseUnsupportedToken
		}
		if err := parseErrs(columnNames, whereClause, alias, sFuncs, f); err != nil {
			return nil, "", 0, nil, nil, sFuncs, err
		}
		return columnNames, alias, limit, whereClause, functionNames, sFuncs, nil
	}
	return nil, "", 0, nil, nil, sFuncs, nil
}

// This is the main function, It goes row by row and for records which validate
// the where clause it currently prints the appropriate row given the requested
// columns.
func processSelectReq(reqColNames []string, alias string, whereClause interface{}, limitOfRecords int64, functionNames []string, myRow chan Row, myFunc SelectFuncs, f format.Select) {
	counter := -1
	var columns []string
	filtrCount := 0
	functionFlag := false
	// My values is used to store our aggregation values if we need to store them.
	myAggVals := make([]float64, len(reqColNames))
	// LowercasecolumnsMap is used in accordance with hasDuplicates so that we can
	// raise the error "Ambigious" if a case insensitive column is provided and we
	// have multiple matches.
	lowercaseColumnsMap := make(map[string]int)
	hasDuplicates := make(map[string]bool)
	// ColumnsMap stores our columns and their index.
	columnsMap := make(map[string]int)
	if limitOfRecords == 0 {
		limitOfRecords = math.MaxInt64
	}
	for {
		record, err := f.Read()
		if err != nil {
			myRow <- Row{
				err: err,
			}
			return
		}
		if record == nil {
			if functionFlag {
				myRow <- Row{
					record: aggFuncToStr(myAggVals, f) + "\n",
				}
			}
			close(myRow)
			return
		}

		out, _ := json.Marshal(record)
		f.UpdateBytesProcessed(record)

		if counter == -1 && f.HasHeader() && len(f.Header()) > 0 {
			columns = f.Header()
			myErr := checkForDuplicates(columns, columnsMap, hasDuplicates, lowercaseColumnsMap)
			if format.IsInt(reqColNames[0]) {
				myErr = ErrMissingHeaders
			}
			if myErr != nil {
				myRow <- Row{
					err: myErr,
				}
				return
			}
		} else if counter == -1 && len(f.Header()) > 0 {
			columns = f.Header()
			for i := 0; i < len(columns); i++ {
				columnsMap["_"+strconv.Itoa(i)] = i
			}

		}
		// Return in case the number of record reaches the LIMIT defined in select query
		if int64(filtrCount) == limitOfRecords && limitOfRecords != 0 {
			close(myRow)
			return
		}

		// The call to the where function clause,ensures that the rows we print match our where clause.
		condition, myErr := matchesMyWhereClause(record, alias, whereClause)
		if myErr != nil {
			myRow <- Row{
				err: myErr,
			}
			return
		}
		if condition {
			// if its an asterix we just print everything in the row
			if reqColNames[0] == "*" && functionNames[0] == "" {
				var row Row
				switch f.Type() {
				case format.CSV:
					row = Row{
						record: strings.Join(convertToSlice(columnsMap, record, string(out)), f.OutputFieldDelimiter()) + "\n",
					}
				case format.JSON:
					row = Row{
						record: string(out) + "\n",
					}
				}
				myRow <- row
			} else if alias != "" {
				// This is for dealing with the case of if we have to deal with a
				// request for a column with an index e.g A_1.
				if format.IsInt(reqColNames[0]) {
					// This checks whether any aggregation function was called as now we
					// no longer will go through printing each row, and only print at the end
					if len(functionNames) > 0 && functionNames[0] != "" {
						functionFlag = true
						aggregationFunctions(counter, filtrCount, myAggVals, reqColNames, functionNames, string(out))
					} else {
						// The code below finds the appropriate columns of the row given the
						// indicies provided in the SQL request and utilizes the map to
						// retrieve the correct part of the row.
						myQueryRow, myErr := processColNameIndex(string(out), reqColNames, columns, f)
						if myErr != nil {
							myRow <- Row{
								err: myErr,
							}
							return
						}
						myRow <- Row{
							record: myQueryRow + "\n",
						}
					}
				} else {
					// This code does aggregation if we were provided column names in the
					// form of acutal names rather an indices.
					if len(functionNames) > 0 && functionNames[0] != "" {
						functionFlag = true
						aggregationFunctions(counter, filtrCount, myAggVals, reqColNames, functionNames, string(out))
					} else {
						// This code prints the appropriate part of the row given the filter
						// and select request, if the select request was based on column
						// names rather than indices.
						myQueryRow, myErr := processColNameLiteral(string(out), reqColNames, myFunc, f)
						if myErr != nil {
							myRow <- Row{
								err: myErr,
							}
							return
						}
						myRow <- Row{
							record: myQueryRow + "\n",
						}
					}
				}
			}
			filtrCount++
		}
		counter++
	}
}

// processColumnNames is a function which allows for cleaning of column names.
func processColumnNames(reqColNames []string, alias string, f format.Select) error {
	switch f.Type() {
	case format.CSV:
		for i := 0; i < len(reqColNames); i++ {
			// The code below basically cleans the column name of its alias and other
			// syntax, so that we can extract its pure name.
			reqColNames[i] = cleanCol(reqColNames[i], alias)
		}
	case format.JSON:
		// JSON doesnt have columns so no cleaning required
	}

	return nil
}

// processColNameIndex is the function which creates the row for an index based
// query.
func processColNameIndex(record string, reqColNames []string, columns []string, f format.Select) (string, error) {
	row := make([]string, len(reqColNames))
	for i := 0; i < len(reqColNames); i++ {
		// COALESCE AND NULLIF do not support index based access.
		if reqColNames[0] == "0" {
			return "", format.ErrInvalidColumnIndex
		}
		mytempindex, err := strconv.Atoi(reqColNames[i])
		if mytempindex > len(columns) {
			return "", format.ErrInvalidColumnIndex
		}

		if err != nil {
			return "", ErrMissingHeaders
		}
		// Subtract 1 because AWS Indexing is not 0 based, it starts at 1 generating the key like "_1".
		row[i] = jsonValue(string("_"+strconv.Itoa(mytempindex-1)), record)
	}
	rowStr := strings.Join(row, f.OutputFieldDelimiter())
	if len(rowStr) > MaxCharsPerRecord {
		return "", ErrOverMaxRecordSize
	}

	return rowStr, nil
}

// processColNameLiteral is the function which creates the row for an name based
// query.
func processColNameLiteral(record string, reqColNames []string, myFunc SelectFuncs, f format.Select) (string, error) {
	row := make([]string, len(reqColNames))
	for i := 0; i < len(reqColNames); i++ {
		// this is the case to deal with COALESCE.
		if reqColNames[i] == "" && isValidFunc(myFunc.index, i) {
			row[i] = evaluateFuncExpr(myFunc.funcExpr[i], "", record)
			continue
		}
		row[i] = jsonValue(reqColNames[i], record)
	}
	rowStr := strings.Join(row, f.OutputFieldDelimiter())
	if len(rowStr) > MaxCharsPerRecord {
		return "", ErrOverMaxRecordSize
	}
	return rowStr, nil
}

// aggregationFunctions is a function which performs the actual aggregation
// methods on the given row, it uses an array defined in the main parsing
// function to keep track of values.
func aggregationFunctions(counter int, filtrCount int, myAggVals []float64, storeReqCols []string, storeFunctions []string, record string) error {
	for i := 0; i < len(storeFunctions); i++ {
		if storeFunctions[i] == "" {
			i++
		} else if storeFunctions[i] == "count" {
			myAggVals[i]++
		} else {
			// If column names are provided as an index it'll use this if statement instead of the else/
			var convAggFloat float64
			if format.IsInt(storeReqCols[i]) {
				myIndex, _ := strconv.Atoi(storeReqCols[i])
				convAggFloat, _ = strconv.ParseFloat(jsonValue(string("_"+strconv.Itoa(myIndex)), record), 64)

			} else {
				// case that the columns are in the form of named columns rather than indices.
				convAggFloat, _ = strconv.ParseFloat(jsonValue(storeReqCols[i], record), 64)
			}
			// This if statement is for calculating the min.
			if storeFunctions[i] == "min" {
				if counter == -1 {
					myAggVals[i] = math.MaxFloat64
				}
				if convAggFloat < myAggVals[i] {
					myAggVals[i] = convAggFloat
				}

			} else if storeFunctions[i] == "max" {
				// This if statement is for calculating the max.
				if counter == -1 {
					myAggVals[i] = math.SmallestNonzeroFloat64
				}
				if convAggFloat > myAggVals[i] {
					myAggVals[i] = convAggFloat
				}

			} else if storeFunctions[i] == "sum" {
				// This if statement is for calculating the sum.
				myAggVals[i] += convAggFloat

			} else if storeFunctions[i] == "avg" {
				// This if statement is for calculating the average.
				if filtrCount == 0 {
					myAggVals[i] = convAggFloat
				} else {
					myAggVals[i] = (convAggFloat + (myAggVals[i] * float64(filtrCount))) / float64((filtrCount + 1))
				}
			} else {
				return ErrParseNonUnaryAgregateFunctionCall
			}
		}
	}
	return nil
}

// convertToSlice takes the map[string]interface{} and convert it to []string
func convertToSlice(columnsMap map[string]int, record map[string]interface{}, marshalledRecord string) []string {
	var result []string
	type kv struct {
		Key   string
		Value int
	}
	var ss []kv
	for k, v := range columnsMap {
		ss = append(ss, kv{k, v})
	}
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value < ss[j].Value
	})
	for _, kv := range ss {
		if _, ok := record[kv.Key]; ok {
			result = append(result, jsonValue(kv.Key, marshalledRecord))
		}
	}
	return result
}
