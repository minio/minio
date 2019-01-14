/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package sql

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

func errUnsupportedSQLStructure(err error) *s3Error {
	return &s3Error{
		code:       "UnsupportedSqlStructure",
		message:    "Encountered an unsupported SQL structure. Check the SQL Reference.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnsupportedSelect(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnsupportedSelect",
		message:    "The SQL expression contains an unsupported use of SELECT.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseAsteriskIsNotAloneInSelectList(err error) *s3Error {
	return &s3Error{
		code:       "ParseAsteriskIsNotAloneInSelectList",
		message:    "Other expressions are not allowed in the SELECT list when '*' is used without dot notation in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseInvalidContextForWildcardInSelectList(err error) *s3Error {
	return &s3Error{
		code:       "ParseInvalidContextForWildcardInSelectList",
		message:    "Invalid use of * in SELECT list in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidDataType(err error) *s3Error {
	return &s3Error{
		code:       "InvalidDataType",
		message:    "The SQL expression contains an invalid data type.",
		statusCode: 400,
		cause:      err,
	}
}

func errUnsupportedFunction(err error) *s3Error {
	return &s3Error{
		code:       "UnsupportedFunction",
		message:    "Encountered an unsupported SQL function.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseNonUnaryAgregateFunctionCall(err error) *s3Error {
	return &s3Error{
		code:       "ParseNonUnaryAgregateFunctionCall",
		message:    "Only one argument is supported for aggregate functions in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errIncorrectSQLFunctionArgumentType(err error) *s3Error {
	return &s3Error{
		code:       "IncorrectSqlFunctionArgumentType",
		message:    "Incorrect type of arguments in function call in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorInvalidArguments(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorInvalidArguments",
		message:    "Incorrect number of arguments in the function call in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errUnsupportedSQLOperation(err error) *s3Error {
	return &s3Error{
		code:       "UnsupportedSqlOperation",
		message:    "Encountered an unsupported SQL operation.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnknownOperator(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnknownOperator",
		message:    "The SQL expression contains an invalid operator.",
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

func errExternalEvalException(err error) *s3Error {
	return &s3Error{
		code:       "ExternalEvalException",
		message:    "The query cannot be evaluated. Check the file and try again.",
		statusCode: 400,
		cause:      err,
	}
}

func errValueParseFailure(err error) *s3Error {
	return &s3Error{
		code:       "ValueParseFailure",
		message:    "Time stamp parse failure in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorBindingDoesNotExist(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorBindingDoesNotExist",
		message:    "A column name or a path provided does not exist in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errInternalError(err error) *s3Error {
	return &s3Error{
		code:       "InternalError",
		message:    "Encountered an internal error.",
		statusCode: 500,
		cause:      err,
	}
}

func errParseInvalidTypeParam(err error) *s3Error {
	return &s3Error{
		code:       "ParseInvalidTypeParam",
		message:    "The SQL expression contains an invalid parameter value.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnsupportedSyntax(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnsupportedSyntax",
		message:    "The SQL expression contains unsupported syntax.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidKeyPath(err error) *s3Error {
	return &s3Error{
		code:       "InvalidKeyPath",
		message:    "Key path in the SQL expression is invalid.",
		statusCode: 400,
		cause:      err,
	}
}
