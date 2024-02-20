//go:build ignore
// +build ignore

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

// /////////////////////////////////////////////////////////////////////
//
//	Validation errors.
//
// /////////////////////////////////////////////////////////////////////
func errExpressionTooLong(err error) *s3Error {
	return &s3Error{
		code:       "ExpressionTooLong",
		message:    "The SQL expression is too long: The maximum byte-length for the SQL expression is 256 KB.",
		statusCode: 400,
		cause:      err,
	}
}

func errColumnTooLong(err error) *s3Error {
	return &s3Error{
		code:       "ColumnTooLong",
		message:    "The length of a column in the result is greater than maxCharsPerColumn of 1 MB.",
		statusCode: 400,
		cause:      err,
	}
}

func errOverMaxColumn(err error) *s3Error {
	return &s3Error{
		code:       "OverMaxColumn",
		message:    "The number of columns in the result is greater than the maximum allowable number of columns.",
		statusCode: 400,
		cause:      err,
	}
}

func errOverMaxRecordSize(err error) *s3Error {
	return &s3Error{
		code:       "OverMaxRecordSize",
		message:    "The length of a record in the input or result is greater than maxCharsPerRecord of 1 MB.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidColumnIndex(err error) *s3Error {
	return &s3Error{
		code:       "InvalidColumnIndex",
		message:    "Column index in the SQL expression is invalid.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidTextEncoding(err error) *s3Error {
	return &s3Error{
		code:       "InvalidTextEncoding",
		message:    "Invalid encoding type. Only UTF-8 encoding is supported.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidTableAlias(err error) *s3Error {
	return &s3Error{
		code:       "InvalidTableAlias",
		message:    "The SQL expression contains an invalid table alias.",
		statusCode: 400,
		cause:      err,
	}
}

func errUnsupportedSyntax(err error) *s3Error {
	return &s3Error{
		code:       "UnsupportedSyntax",
		message:    "Encountered invalid syntax.",
		statusCode: 400,
		cause:      err,
	}
}

func errAmbiguousFieldName(err error) *s3Error {
	return &s3Error{
		code:       "AmbiguousFieldName",
		message:    "Field name matches to multiple fields in the file. Check the SQL expression and the file, and try again.",
		statusCode: 400,
		cause:      err,
	}
}

func errIntegerOverflow(err error) *s3Error {
	return &s3Error{
		code:       "IntegerOverflow",
		message:    "Integer overflow or underflow in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errIllegalSQLFunctionArgument(err error) *s3Error {
	return &s3Error{
		code:       "IllegalSqlFunctionArgument",
		message:    "Illegal argument was used in the SQL function.",
		statusCode: 400,
		cause:      err,
	}
}

func errMultipleDataSourcesUnsupported(err error) *s3Error {
	return &s3Error{
		code:       "MultipleDataSourcesUnsupported",
		message:    "Multiple data sources are not supported.",
		statusCode: 400,
		cause:      err,
	}
}

func errMissingHeaders(err error) *s3Error {
	return &s3Error{
		code:       "MissingHeaders",
		message:    "Some headers in the query are missing from the file. Check the file and try again.",
		statusCode: 400,
		cause:      err,
	}
}

func errUnrecognizedFormatException(err error) *s3Error {
	return &s3Error{
		code:       "UnrecognizedFormatException",
		message:    "Encountered an invalid record type.",
		statusCode: 400,
		cause:      err,
	}
}

// ////////////////////////////////////////////////////////////////////////////////////
//
//	SQL parsing errors.
//
// ////////////////////////////////////////////////////////////////////////////////////
func errLexerInvalidChar(err error) *s3Error {
	return &s3Error{
		code:       "LexerInvalidChar",
		message:    "The SQL expression contains an invalid character.",
		statusCode: 400,
		cause:      err,
	}
}

func errLexerInvalidOperator(err error) *s3Error {
	return &s3Error{
		code:       "LexerInvalidOperator",
		message:    "The SQL expression contains an invalid literal.",
		statusCode: 400,
		cause:      err,
	}
}

func errLexerInvalidLiteral(err error) *s3Error {
	return &s3Error{
		code:       "LexerInvalidLiteral",
		message:    "The SQL expression contains an invalid operator.",
		statusCode: 400,
		cause:      err,
	}
}

func errLexerInvalidIONLiteral(err error) *s3Error {
	return &s3Error{
		code:       "LexerInvalidIONLiteral",
		message:    "The SQL expression contains an invalid operator.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedDatePart(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedDatePart",
		message:    "Did not find the expected date part in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedKeyword(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedKeyword",
		message:    "Did not find the expected keyword in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedTokenType(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedTokenType",
		message:    "Did not find the expected token in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpected2TokenTypes(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpected2TokenTypes",
		message:    "Did not find the expected token in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedNumber(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedNumber",
		message:    "Did not find the expected number in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedRightParenBuiltinFunctionCall(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedRightParenBuiltinFunctionCall",
		message:    "Did not find the expected right parenthesis character in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedTypeName(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedTypeName",
		message:    "Did not find the expected type name in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedWhenClause(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedWhenClause",
		message:    "Did not find the expected WHEN clause in the SQL expression. CASE is not supported.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnsupportedToken(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnsupportedToken",
		message:    "The SQL expression contains an unsupported token.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnsupportedLiteralsGroupBy(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnsupportedLiteralsGroupBy",
		message:    "The SQL expression contains an unsupported use of GROUP BY.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedMember(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedMember",
		message:    "The SQL expression contains an unsupported use of MEMBER.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnsupportedCase(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnsupportedCase",
		message:    "The SQL expression contains an unsupported use of CASE.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnsupportedCaseClause(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnsupportedCaseClause",
		message:    "The SQL expression contains an unsupported use of CASE.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnsupportedAlias(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnsupportedAlias",
		message:    "The SQL expression contains an unsupported use of ALIAS.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseInvalidPathComponent(err error) *s3Error {
	return &s3Error{
		code:       "ParseInvalidPathComponent",
		message:    "The SQL expression contains an invalid path component.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseMissingIdentAfterAt(err error) *s3Error {
	return &s3Error{
		code:       "ParseMissingIdentAfterAt",
		message:    "Did not find the expected identifier after the @ symbol in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnexpectedOperator(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnexpectedOperator",
		message:    "The SQL expression contains an unexpected operator.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnexpectedTerm(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnexpectedTerm",
		message:    "The SQL expression contains an unexpected term.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnexpectedToken(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnexpectedToken",
		message:    "The SQL expression contains an unexpected token.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnExpectedKeyword(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnExpectedKeyword",
		message:    "The SQL expression contains an unexpected keyword.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedExpression(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedExpression",
		message:    "Did not find the expected SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedLeftParenAfterCast(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedLeftParenAfterCast",
		message:    "Did not find the expected left parenthesis after CAST in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedLeftParenValueConstructor(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedLeftParenValueConstructor",
		message:    "Did not find expected the left parenthesis in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedLeftParenBuiltinFunctionCall(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedLeftParenBuiltinFunctionCall",
		message:    "Did not find the expected left parenthesis in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedArgumentDelimiter(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedArgumentDelimiter",
		message:    "Did not find the expected argument delimiter in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseCastArity(err error) *s3Error {
	return &s3Error{
		code:       "ParseCastArity",
		message:    "The SQL expression CAST has incorrect arity.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseEmptySelect(err error) *s3Error {
	return &s3Error{
		code:       "ParseEmptySelect",
		message:    "The SQL expression contains an empty SELECT.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseSelectMissingFrom(err error) *s3Error {
	return &s3Error{
		code:       "ParseSelectMissingFrom",
		message:    "The SQL expression contains a missing FROM after SELECT list.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedIdentForGroupName(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedIdentForGroupName",
		message:    "GROUP is not supported in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedIdentForAlias(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedIdentForAlias",
		message:    "Did not find the expected identifier for the alias in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseUnsupportedCallWithStar(err error) *s3Error {
	return &s3Error{
		code:       "ParseUnsupportedCallWithStar",
		message:    "Only COUNT with (*) as a parameter is supported in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseMalformedJoin(err error) *s3Error {
	return &s3Error{
		code:       "ParseMalformedJoin",
		message:    "JOIN is not supported in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseExpectedIdentForAt(err error) *s3Error {
	return &s3Error{
		code:       "ParseExpectedIdentForAt",
		message:    "Did not find the expected identifier for AT name in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errParseCannotMixSqbAndWildcardInSelectList(err error) *s3Error {
	return &s3Error{
		code:       "ParseCannotMixSqbAndWildcardInSelectList",
		message:    "Cannot mix [] and * in the same expression in a SELECT list in SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

// ////////////////////////////////////////////////////////////////////////////////////
//
//	CAST() related errors.
//
// ////////////////////////////////////////////////////////////////////////////////////
func errCastFailed(err error) *s3Error {
	return &s3Error{
		code:       "CastFailed",
		message:    "Attempt to convert from one data type to another using CAST failed in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errInvalidCast(err error) *s3Error {
	return &s3Error{
		code:       "InvalidCast",
		message:    "Attempt to convert from one data type to another using CAST failed in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorInvalidTimestampFormatPattern(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorInvalidTimestampFormatPattern",
		message:    "Invalid time stamp format string in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorInvalidTimestampFormatPatternAdditionalFieldsRequired(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorInvalidTimestampFormatPattern",
		message:    "Time stamp format pattern requires additional fields in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorInvalidTimestampFormatPatternSymbolForParsing(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorInvalidTimestampFormatPatternSymbolForParsing",
		message:    "Time stamp format pattern contains a valid format symbol that cannot be applied to time stamp parsing in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorTimestampFormatPatternDuplicateFields(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorTimestampFormatPatternDuplicateFields",
		message:    "Time stamp format pattern contains multiple format specifiers representing the time stamp field in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorTimestampFormatPatternHourClockAmPmMismatch(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorTimestampFormatPatternHourClockAmPmMismatch",
		message:    "Time stamp format pattern contains a 12-hour hour of day format symbol but doesn't also contain an AM/PM field, or it contains a 24-hour hour of day format specifier and contains an AM/PM field in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorUnterminatedTimestampFormatPatternToken(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorUnterminatedTimestampFormatPatternToken",
		message:    "Time stamp format pattern contains unterminated token in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorInvalidTimestampFormatPatternToken(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorInvalidTimestampFormatPatternToken",
		message:    "Time stamp format pattern contains an invalid token in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

func errEvaluatorInvalidTimestampFormatPatternSymbol(err error) *s3Error {
	return &s3Error{
		code:       "EvaluatorInvalidTimestampFormatPatternSymbol",
		message:    "Time stamp format pattern contains an invalid symbol in the SQL expression.",
		statusCode: 400,
		cause:      err,
	}
}

// //////////////////////////////////////////////////////////////////////
//
//	Generic S3 HTTP handler errors.
//
// //////////////////////////////////////////////////////////////////////
func errBusy(err error) *s3Error {
	return &s3Error{
		code:       "Busy",
		message:    "The service is unavailable. Please retry.",
		statusCode: 503,
		cause:      err,
	}
}

func errUnauthorizedAccess(err error) *s3Error {
	return &s3Error{
		code:       "UnauthorizedAccess",
		message:    "You are not authorized to perform this operation",
		statusCode: 401,
		cause:      err,
	}
}

func errEmptyRequestBody(err error) *s3Error {
	return &s3Error{
		code:       "EmptyRequestBody",
		message:    "Request body cannot be empty.",
		statusCode: 400,
		cause:      err,
	}
}

func errUnsupportedRangeHeader(err error) *s3Error {
	return &s3Error{
		code:       "UnsupportedRangeHeader",
		message:    "Range header is not supported for this operation.",
		statusCode: 400,
		cause:      err,
	}
}

func errUnsupportedStorageClass(err error) *s3Error {
	return &s3Error{
		code:       "UnsupportedStorageClass",
		message:    "Encountered an invalid storage class. Only STANDARD, STANDARD_IA, and ONEZONE_IA storage classes are supported.",
		statusCode: 400,
		cause:      err,
	}
}
