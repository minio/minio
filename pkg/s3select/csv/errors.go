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

package csv

import "errors"

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

func errCSVParsingError(err error) *s3Error {
	return &s3Error{
		code:       "CSVParsingError",
		message:    "Encountered an error parsing the CSV file. Check the file and try again.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidTextEncodingError() *s3Error {
	return &s3Error{
		code:       "InvalidTextEncoding",
		message:    "UTF-8 encoding is required.",
		statusCode: 400,
		cause:      errors.New("invalid utf8 encoding"),
	}
}
