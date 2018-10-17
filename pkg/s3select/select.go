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
	"math"
	"strconv"
	"strings"

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
func (reader *Input) runSelectParser(selectExpression string, myRow chan *Row) {
	reqCols, alias, myLimit, whereClause, aggFunctionNames, myFuncs, myErr := reader.ParseSelect(selectExpression)
	if myErr != nil {
		rowStruct := &Row{
			err: myErr,
		}
		myRow <- rowStruct
		return
	}
	reader.processSelectReq(reqCols, alias, whereClause, myLimit, aggFunctionNames, myRow, myFuncs)
}

// ParseSelect parses the SELECT expression, and effectively tokenizes it into
// its separate parts. It returns the requested column names,alias,limit of
// records, and the where clause.
func (reader *Input) ParseSelect(sqlInput string) ([]string, string, int64, interface{}, []string, *SelectFuncs, error) {
	// return columnNames, alias, limitOfRecords, whereclause,coalStore, nil

	stmt, err := sqlparser.Parse(sqlInput)
	var whereClause interface{}
	var alias string
	var limit int64
	myFuncs := &SelectFuncs{}
	// TODO Maybe can parse their errors a bit to return some more of the s3 errors
	if err != nil {
		return nil, "", 0, nil, nil, nil, ErrLexerInvalidChar
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
									return nil, "", 0, nil, nil, nil, ErrParseUnsupportedCallWithStar
								}
							case *sqlparser.AliasedExpr:
								switch col := tempagg.Expr.(type) {
								case *sqlparser.BinaryExpr:
									return nil, "", 0, nil, nil, nil, ErrParseNonUnaryAgregateFunctionCall
								case *sqlparser.ColName:
									columnNames[i] = col.Name.CompliantName()
								}
							}
							// Case to deal with if COALESCE was used..
						} else if supportedFunc(smallerexpr.Name.CompliantName()) {
							if myFuncs.funcExpr == nil {
								myFuncs.funcExpr = make([]*sqlparser.FuncExpr, len(stmt.SelectExprs))
								myFuncs.index = make([]int, len(stmt.SelectExprs))
							}
							myFuncs.funcExpr[i] = smallerexpr
							myFuncs.index[i] = i
						} else {
							return nil, "", 0, nil, nil, nil, ErrUnsupportedSQLOperation
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
					return nil, "", 0, nil, nil, nil, ErrParseMalformedJoin
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
			return nil, "", 0, nil, nil, nil, ErrParseUnsupportedLiteralsGroupBy
		}
		if stmt.OrderBy != nil {
			return nil, "", 0, nil, nil, nil, ErrParseUnsupportedToken
		}
		if err := reader.parseErrs(columnNames, whereClause, alias, myFuncs); err != nil {
			return nil, "", 0, nil, nil, nil, err
		}
		return columnNames, alias, limit, whereClause, functionNames, myFuncs, nil
	}
	return nil, "", 0, nil, nil, nil, nil
}

// This is the main function, It goes row by row and for records which validate
// the where clause it currently prints the appropriate row given the requested
// columns.
func (reader *Input) processSelectReq(reqColNames []string, alias string, whereClause interface{}, limitOfRecords int64, functionNames []string, myRow chan *Row, myFunc *SelectFuncs) {
	counter := -1
	filtrCount := 0
	functionFlag := false
	// My values is used to store our aggregation values if we need to store them.
	myAggVals := make([]float64, len(reqColNames))
	var columns []string
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
		record := reader.ReadRecord()
		reader.stats.BytesProcessed += processSize(record)
		if record == nil {
			if functionFlag {
				rowStruct := &Row{
					record: reader.aggFuncToStr(myAggVals) + "\n",
				}
				myRow <- rowStruct
			}
			close(myRow)
			return
		}
		if counter == -1 && reader.options.HeaderOpt && len(reader.header) > 0 {
			columns = reader.Header()
			myErr := checkForDuplicates(columns, columnsMap, hasDuplicates, lowercaseColumnsMap)
			if myErr != nil {
				rowStruct := &Row{
					err: myErr,
				}
				myRow <- rowStruct
				return
			}
		} else if counter == -1 && len(reader.header) > 0 {
			columns = reader.Header()
		}
		// When we have reached our limit, on what the user specified as the number
		// of rows they wanted, we terminate our interpreter.
		if int64(filtrCount) == limitOfRecords && limitOfRecords != 0 {
			close(myRow)
			return
		}
		// The call to the where function clause,ensures that the rows we print match our where clause.
		condition, myErr := matchesMyWhereClause(record, columnsMap, alias, whereClause)
		if myErr != nil {
			rowStruct := &Row{
				err: myErr,
			}
			myRow <- rowStruct
			return
		}
		if condition {
			// if its an asterix we just print everything in the row
			if reqColNames[0] == "*" && functionNames[0] == "" {
				rowStruct := &Row{
					record: reader.printAsterix(record) + "\n",
				}
				myRow <- rowStruct
			} else if alias != "" {
				// This is for dealing with the case of if we have to deal with a
				// request for a column with an index e.g A_1.
				if representsInt(reqColNames[0]) {
					// This checks whether any aggregation function was called as now we
					// no longer will go through printing each row, and only print at the
					// end
					if len(functionNames) > 0 && functionNames[0] != "" {
						functionFlag = true
						aggregationFunctions(counter, filtrCount, myAggVals, columnsMap, reqColNames, functionNames, record)
					} else {
						// The code below finds the appropriate columns of the row given the
						// indicies provided in the SQL request and utilizes the map to
						// retrieve the correct part of the row.
						myQueryRow, myErr := reader.processColNameIndex(record, reqColNames, columns)
						if myErr != nil {
							rowStruct := &Row{
								err: myErr,
							}
							myRow <- rowStruct
							return
						}
						rowStruct := &Row{
							record: myQueryRow + "\n",
						}
						myRow <- rowStruct
					}
				} else {
					// This code does aggregation if we were provided column names in the
					// form of acutal names rather an indices.
					if len(functionNames) > 0 && functionNames[0] != "" {
						functionFlag = true
						aggregationFunctions(counter, filtrCount, myAggVals, columnsMap, reqColNames, functionNames, record)
					} else {
						// This code prints the appropriate part of the row given the filter
						// and select request, if the select request was based on column
						// names rather than indices.
						myQueryRow, myErr := reader.processColNameLiteral(record, reqColNames, columns, columnsMap, myFunc)
						if myErr != nil {
							rowStruct := &Row{
								err: myErr,
							}
							myRow <- rowStruct
							return
						}
						rowStruct := &Row{
							record: myQueryRow + "\n",
						}
						myRow <- rowStruct
					}
				}
			}
			filtrCount++
		}
		counter++
	}
}

// printAsterix helps to print out the entire row if an asterix is used.
func (reader *Input) printAsterix(record []string) string {
	return strings.Join(record, reader.options.OutputFieldDelimiter)
}

// processColumnNames is a function which allows for cleaning of column names.
func (reader *Input) processColumnNames(reqColNames []string, alias string) error {
	for i := 0; i < len(reqColNames); i++ {
		// The code below basically cleans the column name of its alias and other
		// syntax, so that we can extract its pure name.
		reqColNames[i] = cleanCol(reqColNames[i], alias)
	}
	return nil
}

// processColNameIndex is the function which creates the row for an index based
// query.
func (reader *Input) processColNameIndex(record []string, reqColNames []string, columns []string) (string, error) {
	row := make([]string, len(reqColNames))
	for i := 0; i < len(reqColNames); i++ {
		// COALESCE AND NULLIF do not support index based access.
		if reqColNames[0] == "0" {
			return "", ErrInvalidColumnIndex
		}
		// Subtract 1 because AWS Indexing is not 0 based, it starts at 1.
		mytempindex, err := strconv.Atoi(reqColNames[i])
		if err != nil {
			return "", ErrMissingHeaders
		}
		mytempindex = mytempindex - 1
		if mytempindex > len(columns) {
			return "", ErrInvalidColumnIndex
		}
		row[i] = record[mytempindex]
	}
	rowStr := strings.Join(row, reader.options.OutputFieldDelimiter)
	if len(rowStr) > 1000000 {
		return "", ErrOverMaxRecordSize
	}
	return rowStr, nil
}

// processColNameLiteral is the function which creates the row for an name based
// query.
func (reader *Input) processColNameLiteral(record []string, reqColNames []string, columns []string, columnsMap map[string]int, myFunc *SelectFuncs) (string, error) {
	row := make([]string, len(reqColNames))
	for i := 0; i < len(reqColNames); i++ {
		// this is the case to deal with COALESCE.
		if reqColNames[i] == "" && isValidFunc(myFunc.index, i) {
			row[i] = evaluateFuncExpr(myFunc.funcExpr[i], "", record, columnsMap)
			continue
		}
		myTempIndex, notFound := columnsMap[trimQuotes(reqColNames[i])]
		if !notFound {
			return "", ErrMissingHeaders
		}
		row[i] = record[myTempIndex]
	}
	rowStr := strings.Join(row, reader.options.OutputFieldDelimiter)
	if len(rowStr) > 1000000 {
		return "", ErrOverMaxRecordSize
	}
	return rowStr, nil
}

// aggregationFunctions performs the actual aggregation methods on the
// given row, it uses an array defined for the main parsing function
// to keep track of values.
func aggregationFunctions(counter int, filtrCount int, myAggVals []float64, columnsMap map[string]int, storeReqCols []string, storeFunctions []string, record []string) error {
	for i := 0; i < len(storeFunctions); i++ {
		if storeFunctions[i] == "" {
			i++
		} else if storeFunctions[i] == "count" {
			myAggVals[i]++
		} else {
			// If column names are provided as an index it'll use this if statement instead of the else/
			var convAggFloat float64
			if representsInt(storeReqCols[i]) {
				colIndex, _ := strconv.Atoi(storeReqCols[i])
				// colIndex is 1-based
				convAggFloat, _ = strconv.ParseFloat(record[colIndex-1], 64)

			} else {
				// case that the columns are in the form of named columns rather than indices.
				convAggFloat, _ = strconv.ParseFloat(record[columnsMap[trimQuotes(storeReqCols[i])]], 64)

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
