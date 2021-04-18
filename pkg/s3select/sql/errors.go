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

import "fmt"

type s3Error struct {
	code       string
	message    string
	statusCode int
	cause      error
}

func (err *s3Error) Cause() error {
	return err.cause
}

func (err *s3Error) ErrorCode() string {
	return err.code
}

func (err *s3Error) ErrorMessage() string {
	return err.message
}

func (err *s3Error) HTTPStatusCode() int {
	return err.statusCode
}

func (err *s3Error) Error() string {
	return err.message
}

func errInvalidDataType(err error) *s3Error {
	return &s3Error{
		code:       "InvalidDataType",
		message:    "The SQL expression contains an invalid data type.",
		statusCode: 400,
		cause:      err,
	}
}

func errIncorrectSQLFunctionArgumentType(err error) *s3Error {
	return &s3Error{
		code:       "IncorrectSqlFunctionArgumentType",
		message:    "Incorrect type of arguments in function call.",
		statusCode: 400,
		cause:      err,
	}
}

func errLikeInvalidInputs(err error) *s3Error {
	return &s3Error{
		code:       "LikeInvalidInputs",
		message:    "Invalid argument given to the LIKE clause in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errQueryParseFailure(err error) *s3Error {
	return &s3Error{
		code:       "ParseSelectFailure",
		message:    err.Error(),
		statusCode: 400,
		cause:      err,
	}
}

func errQueryAnalysisFailure(err error) *s3Error {
	return &s3Error{
		code:       "InvalidQuery",
		message:    err.Error(),
		statusCode: 400,
		cause:      err,
	}
}

func errBadTableName(err error) *s3Error {
	return &s3Error{
		code:       "BadTableName",
		message:    fmt.Sprintf("The table name is not supported: %v", err),
		statusCode: 400,
		cause:      err,
	}
}

func errDataSource(err error) *s3Error {
	return &s3Error{
		code:       "DataSourcePathUnsupported",
		message:    fmt.Sprintf("Data source: %v", err),
		statusCode: 400,
		cause:      err,
	}
}
