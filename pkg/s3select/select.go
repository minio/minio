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
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio/pkg/s3select/format"
	"github.com/tidwall/gjson"
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
func runSelectParser(f format.Select, rowCh chan Row) {
	reqCols, alias, limit, wc, aggFunctionNames, fns, err := ParseSelect(f)
	if err != nil {
		rowCh <- Row{
			err: err,
		}
		return
	}
	processSelectReq(reqCols, alias, wc, limit, aggFunctionNames, rowCh, fns, f)
}

// ParseSelect parses the SELECT expression, and effectively tokenizes it into
// its separate parts. It returns the requested column names,alias,limit of
// records, and the where clause.
func ParseSelect(f format.Select) ([]string, string, int64, sqlparser.Expr, []string, SelectFuncs, error) {
	var sFuncs = SelectFuncs{}
	var whereClause sqlparser.Expr
	var alias string
	var limit int64

	stmt, err := sqlparser.Parse(f.Expression())
	// TODO: Maybe can parse their errors a bit to return some more of the s3 errors
	if err != nil {
		return nil, "", 0, nil, nil, sFuncs, ErrLexerInvalidChar
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// evaluates the where clause
		fnNames := make([]string, len(stmt.SelectExprs))
		columnNames := make([]string, len(stmt.SelectExprs))

		if stmt.Where != nil {
			whereClause = stmt.Where.Expr
		}
		for i, sexpr := range stmt.SelectExprs {
			switch expr := sexpr.(type) {
			case *sqlparser.StarExpr:
				columnNames[0] = "*"
			case *sqlparser.AliasedExpr:
				switch smallerexpr := expr.Expr.(type) {
				case *sqlparser.FuncExpr:
					if smallerexpr.IsAggregate() {
						fnNames[i] = smallerexpr.Name.CompliantName()
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

		// This code retrieves the alias and makes sure it is set to the correct
		// value, if not it sets it to the tablename
		for _, fexpr := range stmt.From {
			switch smallerexpr := fexpr.(type) {
			case *sqlparser.JoinTableExpr:
				return nil, "", 0, nil, nil, sFuncs, ErrParseMalformedJoin
			case *sqlparser.AliasedTableExpr:
				alias = smallerexpr.As.CompliantName()
				if alias == "" {
					alias = sqlparser.GetTableName(smallerexpr.Expr).CompliantName()
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
		return columnNames, alias, limit, whereClause, fnNames, sFuncs, nil
	}
	return nil, "", 0, nil, nil, sFuncs, nil
}

type columnKv struct {
	Key   string
	Value int
}

func columnsIndex(reqColNames []string, f format.Select) ([]columnKv, error) {
	var (
		columnsKv  []columnKv
		columnsMap = make(map[string]int)
		columns    = f.Header()
	)
	if f.HasHeader() {
		err := checkForDuplicates(columns, columnsMap)
		if format.IsInt(reqColNames[0]) {
			err = ErrMissingHeaders
		}
		if err != nil {
			return nil, err
		}
		for k, v := range columnsMap {
			columnsKv = append(columnsKv, columnKv{
				Key:   k,
				Value: v,
			})
		}
	} else {
		for i := range columns {
			columnsKv = append(columnsKv, columnKv{
				Key:   "_" + strconv.Itoa(i),
				Value: i,
			})
		}
	}
	sort.Slice(columnsKv, func(i, j int) bool {
		return columnsKv[i].Value < columnsKv[j].Value
	})
	return columnsKv, nil
}

// This is the main function, It goes row by row and for records which validate
// the where clause it currently prints the appropriate row given the requested
// columns.
func processSelectReq(reqColNames []string, alias string, wc sqlparser.Expr, lrecords int64, fnNames []string, rowCh chan Row, fn SelectFuncs, f format.Select) {
	counter := -1
	filtrCount := 0
	functionFlag := false

	// Values used to store our aggregation values.
	aggVals := make([]float64, len(reqColNames))
	if lrecords == 0 {
		lrecords = math.MaxInt64
	}

	var results []string
	var columnsKv []columnKv
	if f.Type() == format.CSV {
		var err error
		columnsKv, err = columnsIndex(reqColNames, f)
		if err != nil {
			rowCh <- Row{
				err: err,
			}
			return
		}
		results = make([]string, len(columnsKv))
	}

	for {
		record, err := f.Read()
		if err != nil {
			rowCh <- Row{
				err: err,
			}
			return
		}
		if record == nil {
			if functionFlag {
				rowCh <- Row{
					record: aggFuncToStr(aggVals, f) + "\n",
				}
			}
			close(rowCh)
			return
		}

		// For JSON multi-line input type columns needs
		// to be handled for each record.
		if f.Type() == format.JSON {
			columnsKv, err = columnsIndex(reqColNames, f)
			if err != nil {
				rowCh <- Row{
					err: err,
				}
				return
			}
			results = make([]string, len(columnsKv))
		}

		f.UpdateBytesProcessed(int64(len(record)))

		// Return in case the number of record reaches the LIMIT
		// defined in select query
		if int64(filtrCount) == lrecords {
			close(rowCh)
			return
		}

		// The call to the where function clause, ensures that
		// the rows we print match our where clause.
		condition, err := matchesMyWhereClause(record, alias, wc)
		if err != nil {
			rowCh <- Row{
				err: err,
			}
			return
		}

		if condition {
			// if its an asterix we just print everything in the row
			if reqColNames[0] == "*" && fnNames[0] == "" {
				switch f.OutputType() {
				case format.CSV:
					for i, kv := range columnsKv {
						results[i] = gjson.GetBytes(record, kv.Key).String()
					}
					rowCh <- Row{
						record: strings.Join(results, f.OutputFieldDelimiter()) + f.OutputRecordDelimiter(),
					}
				case format.JSON:
					rowCh <- Row{
						record: string(record) + f.OutputRecordDelimiter(),
					}
				}
			} else if alias != "" {
				// This is for dealing with the case of if we have to deal with a
				// request for a column with an index e.g A_1.
				if format.IsInt(reqColNames[0]) {
					// This checks whether any aggregation function was called as now we
					// no longer will go through printing each row, and only print at the end
					if len(fnNames) > 0 && fnNames[0] != "" {
						functionFlag = true
						aggregationFns(counter, filtrCount, aggVals, reqColNames, fnNames, record)
					} else {
						// The code below finds the appropriate columns of the row given the
						// indicies provided in the SQL request.
						var rowStr string
						rowStr, err = processColNameIndex(record, reqColNames, f)
						if err != nil {
							rowCh <- Row{
								err: err,
							}
							return
						}
						rowCh <- Row{
							record: rowStr + "\n",
						}
					}
				} else {
					// This code does aggregation if we were provided column names in the
					// form of actual names rather an indices.
					if len(fnNames) > 0 && fnNames[0] != "" {
						functionFlag = true
						aggregationFns(counter, filtrCount, aggVals, reqColNames, fnNames, record)
					} else {
						// This code prints the appropriate part of the row given the filter
						// and select request, if the select request was based on column
						// names rather than indices.
						var rowStr string
						rowStr, err = processColNameLiteral(record, reqColNames, fn, f)
						if err != nil {
							rowCh <- Row{
								err: err,
							}
							return
						}
						rowCh <- Row{
							record: rowStr + "\n",
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
		for i := range reqColNames {
			// The code below basically cleans the column name of its alias and other
			// syntax, so that we can extract its pure name.
			reqColNames[i] = cleanCol(reqColNames[i], alias)
		}
	case format.JSON:
		// JSON doesnt have columns so no cleaning required
	}

	return nil
}

// processColNameIndex is the function which creates the row for an index based query.
func processColNameIndex(record []byte, reqColNames []string, f format.Select) (string, error) {
	var row []string
	for _, colName := range reqColNames {
		// COALESCE AND NULLIF do not support index based access.
		if reqColNames[0] == "0" {
			return "", format.ErrInvalidColumnIndex
		}
		cindex, err := strconv.Atoi(colName)
		if err != nil {
			return "", ErrMissingHeaders
		}
		if cindex > len(f.Header()) {
			return "", format.ErrInvalidColumnIndex
		}

		// Subtract 1 because SELECT indexing is not 0 based, it
		// starts at 1 generating the key like "_1".
		row = append(row, gjson.GetBytes(record, string("_"+strconv.Itoa(cindex-1))).String())
	}
	rowStr := strings.Join(row, f.OutputFieldDelimiter())
	if len(rowStr) > MaxCharsPerRecord {
		return "", ErrOverMaxRecordSize
	}
	return rowStr, nil
}

// processColNameLiteral is the function which creates the row for an name based query.
func processColNameLiteral(record []byte, reqColNames []string, fn SelectFuncs, f format.Select) (string, error) {
	row := make([]string, len(reqColNames))
	for i, colName := range reqColNames {
		// this is the case to deal with COALESCE.
		if colName == "" && isValidFunc(fn.index, i) {
			row[i] = evaluateFuncExpr(fn.funcExpr[i], "", record)
			continue
		}
		row[i] = gjson.GetBytes(record, colName).String()
	}
	rowStr := strings.Join(row, f.OutputFieldDelimiter())
	if len(rowStr) > MaxCharsPerRecord {
		return "", ErrOverMaxRecordSize
	}
	return rowStr, nil
}

// aggregationFns is a function which performs the actual aggregation
// methods on the given row, it uses an array defined in the main parsing
// function to keep track of values.
func aggregationFns(counter int, filtrCount int, aggVals []float64, storeReqCols []string, storeFns []string, record []byte) error {
	for i, storeFn := range storeFns {
		switch storeFn {
		case "":
			continue
		case "count":
			aggVals[i]++
		default:
			// Column names are provided as an index it'll use
			// this if statement instead.
			var convAggFloat float64
			if format.IsInt(storeReqCols[i]) {
				index, _ := strconv.Atoi(storeReqCols[i])
				convAggFloat = gjson.GetBytes(record, "_"+strconv.Itoa(index)).Float()
			} else {
				// Named columns rather than indices.
				convAggFloat = gjson.GetBytes(record, storeReqCols[i]).Float()
			}
			switch storeFn {
			case "min":
				if counter == -1 {
					aggVals[i] = math.MaxFloat64
				}
				if convAggFloat < aggVals[i] {
					aggVals[i] = convAggFloat
				}
			case "max":
				// Calculate the max.
				if counter == -1 {
					aggVals[i] = math.SmallestNonzeroFloat64
				}
				if convAggFloat > aggVals[i] {
					aggVals[i] = convAggFloat
				}
			case "sum":
				// Calculate the sum.
				aggVals[i] += convAggFloat
			case "avg":
				// Calculating the average.
				if filtrCount == 0 {
					aggVals[i] = convAggFloat
				} else {
					aggVals[i] = (convAggFloat + (aggVals[i] * float64(filtrCount))) / float64((filtrCount + 1))
				}
			default:
				return ErrParseNonUnaryAgregateFunctionCall
			}
		}
	}
	return nil
}
