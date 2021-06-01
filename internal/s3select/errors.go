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

package s3select

// SelectError - represents s3 select error specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html#RESTObjectSELECTContent-responses-special-errors.
type SelectError interface {
	Cause() error
	ErrorCode() string
	ErrorMessage() string
	HTTPStatusCode() int
	Error() string
}

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

func errMalformedXML(err error) *s3Error {
	return &s3Error{
		code:       "MalformedXML",
		message:    "The XML provided was not well-formed or did not validate against our published schema. Check the service documentation and try again: " + err.Error(),
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidCompressionFormat(err error) *s3Error {
	return &s3Error{
		code:       "InvalidCompressionFormat",
		message:    "The file is not in a supported compression format. Only GZIP and BZIP2 are supported.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidBZIP2CompressionFormat(err error) *s3Error {
	return &s3Error{
		code:       "InvalidCompressionFormat",
		message:    "BZIP2 is not applicable to the queried object. Please correct the request and try again.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidGZIPCompressionFormat(err error) *s3Error {
	return &s3Error{
		code:       "InvalidCompressionFormat",
		message:    "GZIP is not applicable to the queried object. Please correct the request and try again.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidDataSource(err error) *s3Error {
	return &s3Error{
		code:       "InvalidDataSource",
		message:    "Invalid data source type. Only CSV, JSON, and Parquet are supported.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidRequestParameter(err error) *s3Error {
	return &s3Error{
		code:       "InvalidRequestParameter",
		message:    "The value of a parameter in SelectRequest element is invalid. Check the service API documentation and try again.",
		statusCode: 400,
		cause:      err,
	}
}

func errObjectSerializationConflict(err error) *s3Error {
	return &s3Error{
		code:       "ObjectSerializationConflict",
		message:    "InputSerialization specifies more than one format (CSV, JSON, or Parquet), or OutputSerialization specifies more than one format (CSV or JSON). InputSerialization and OutputSerialization can only specify one format each.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidExpressionType(err error) *s3Error {
	return &s3Error{
		code:       "InvalidExpressionType",
		message:    "The ExpressionType is invalid. Only SQL expressions are supported.",
		statusCode: 400,
		cause:      err,
	}
}

func errMissingRequiredParameter(err error) *s3Error {
	return &s3Error{
		code:       "MissingRequiredParameter",
		message:    "The SelectRequest entity is missing a required parameter. Check the service documentation and try again.",
		statusCode: 400,
		cause:      err,
	}
}

func errTruncatedInput(err error) *s3Error {
	return &s3Error{
		code:       "TruncatedInput",
		message:    "Object decompression failed. Check that the object is properly compressed using the format specified in the request.",
		statusCode: 400,
		cause:      err,
	}
}
