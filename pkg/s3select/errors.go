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
	"errors"

	"github.com/minio/minio/pkg/s3select/format"
)

//S3 errors below

// ErrBusy is an error if the service is too busy.
var ErrBusy = errors.New("The service is unavailable. Please retry")

// ErrUnauthorizedAccess is an error if you lack the appropriate credentials to
// access the object.
var ErrUnauthorizedAccess = errors.New("You are not authorized to perform this operation")

// ErrExpressionTooLong is an error if your SQL expression too long for
// processing.
var ErrExpressionTooLong = errors.New("The SQL expression is too long: The maximum byte-length for the SQL expression is 256 KB")

// ErrIllegalSQLFunctionArgument is an error if you provide an illegal argument
// in the SQL function.
var ErrIllegalSQLFunctionArgument = errors.New("Illegal argument was used in the SQL function")

// ErrInvalidKeyPath is an error if you provide a key in the SQL expression that
// is invalid.
var ErrInvalidKeyPath = errors.New("Key path in the SQL expression is invalid")

// ErrColumnTooLong is an error if your query results in a column that is
// greater than the max amount of characters per column of 1mb
var ErrColumnTooLong = errors.New("The length of a column in the result is greater than maxCharsPerColumn of 1 MB")

// ErrOverMaxColumn is an error if the number of columns from the resulting
// query is greater than 1Mb.
var ErrOverMaxColumn = errors.New("The number of columns in the result is greater than maxColumnNumber of 1 MB")

// ErrOverMaxRecordSize is an error if the length of a record in the result is
// greater than 1 Mb.
var ErrOverMaxRecordSize = errors.New("The length of a record in the result is greater than maxCharsPerRecord of 1 MB")

// ErrMissingHeaders is an error if some of the headers that are requested in
// the Select Query are not present in the file.
var ErrMissingHeaders = errors.New("Some headers in the query are missing from the file. Check the file and try again")

// ErrInvalidCompressionFormat is an error if an unsupported compression type is
// utilized with the select object query.
var ErrInvalidCompressionFormat = errors.New("The file is not in a supported compression format. Only GZIP is supported at this time")

// ErrInvalidFileHeaderInfo is an error if the argument provided to the
// FileHeader Argument is incorrect.
var ErrInvalidFileHeaderInfo = errors.New("The FileHeaderInfo is invalid. Only NONE, USE, and IGNORE are supported")

// ErrInvalidJSONType is an error if the json format provided as an argument is
// invalid.
var ErrInvalidJSONType = errors.New("The JsonType is invalid. Only DOCUMENT and LINES are supported at this time")

// ErrInvalidQuoteFields is an error if the arguments provided to the
// QuoteFields options are not valid.
var ErrInvalidQuoteFields = errors.New("The QuoteFields is invalid. Only ALWAYS and ASNEEDED are supported")

// ErrInvalidRequestParameter is an error if the value of a parameter in the
// request element is not valid.
var ErrInvalidRequestParameter = errors.New("The value of a parameter in Request element is invalid. Check the service API documentation and try again")

// ErrExternalEvalException is an error that arises if the query can not be
// evaluated.
var ErrExternalEvalException = errors.New("The query cannot be evaluated. Check the file and try again")

// ErrInvalidDataType is an error that occurs if the SQL expression contains an
// invalid data type.
var ErrInvalidDataType = errors.New("The SQL expression contains an invalid data type")

// ErrUnrecognizedFormatException is an error that arises if there is an invalid
// record type.
var ErrUnrecognizedFormatException = errors.New("Encountered an invalid record type")

// ErrInvalidTextEncoding is an error if the text encoding is not valid.
var ErrInvalidTextEncoding = errors.New("Invalid encoding type. Only UTF-8 encoding is supported at this time")

// ErrInvalidTableAlias is an error that arises if the table alias provided in
// the SQL expression is invalid.
var ErrInvalidTableAlias = errors.New("The SQL expression contains an invalid table alias")

// ErrMultipleDataSourcesUnsupported is an error that arises if multiple data
// sources are provided.
var ErrMultipleDataSourcesUnsupported = errors.New("Multiple data sources are not supported")

// ErrMissingRequiredParameter is an error that arises if a required argument
// is omitted from the Request.
var ErrMissingRequiredParameter = errors.New("The Request entity is missing a required parameter. Check the service documentation and try again")

// ErrObjectSerializationConflict is an error that arises if an unsupported
// output seralization is provided.
var ErrObjectSerializationConflict = errors.New("The Request entity can only contain one of CSV or JSON. Check the service documentation and try again")

// ErrUnsupportedSQLOperation is an error that arises if an unsupported SQL
// operation is used.
var ErrUnsupportedSQLOperation = errors.New("Encountered an unsupported SQL operation")

// ErrUnsupportedSQLStructure is an error that occurs if an unsupported SQL
// structure is used.
var ErrUnsupportedSQLStructure = errors.New("Encountered an unsupported SQL structure. Check the SQL Reference")

// ErrUnsupportedStorageClass is an error that occurs if an invalid storace
// class is present.
var ErrUnsupportedStorageClass = errors.New("Encountered an invalid storage class. Only STANDARD, STANDARD_IA, and ONEZONE_IA storage classes are supported at this time")

// ErrUnsupportedSyntax is an error that occurs if invalid syntax is present in
// the query.
var ErrUnsupportedSyntax = errors.New("Encountered invalid syntax")

// ErrUnsupportedRangeHeader is an error that occurs if a range header is
// provided.
var ErrUnsupportedRangeHeader = errors.New("Range header is not supported for this operation")

// ErrLexerInvalidChar is an error that occurs if the SQL expression contains an
// invalid character.
var ErrLexerInvalidChar = errors.New("The SQL expression contains an invalid character")

// ErrLexerInvalidOperator is an error that occurs if an invalid operator is
// used.
var ErrLexerInvalidOperator = errors.New("The SQL expression contains an invalid operator")

// ErrLexerInvalidLiteral is an error that occurs if an invalid literal is used.
var ErrLexerInvalidLiteral = errors.New("The SQL expression contains an invalid literal")

// ErrLexerInvalidIONLiteral is an error that occurs if an invalid operator is
// used
var ErrLexerInvalidIONLiteral = errors.New("The SQL expression contains an invalid operator")

// ErrParseExpectedDatePart is an error that occurs if the date part is not
// found in the SQL expression.
var ErrParseExpectedDatePart = errors.New("Did not find the expected date part in the SQL expression")

// ErrParseExpectedKeyword is an error that occurs if the expected keyword was
// not found in the expression.
var ErrParseExpectedKeyword = errors.New("Did not find the expected keyword in the SQL expression")

// ErrParseExpectedTokenType is an error that occurs if the expected token is
// not found in the SQL expression.
var ErrParseExpectedTokenType = errors.New("Did not find the expected token in the SQL expression")

// ErrParseExpected2TokenTypes is an error that occurs if 2 token types are not
// found.
var ErrParseExpected2TokenTypes = errors.New("Did not find the expected token in the SQL expression")

// ErrParseExpectedNumber is an error that occurs if a number is expected but
// not found in the expression.
var ErrParseExpectedNumber = errors.New("Did not find the expected number in the SQL expression")

// ErrParseExpectedRightParenBuiltinFunctionCall is an error that occurs if a
// right parenthesis is missing.
var ErrParseExpectedRightParenBuiltinFunctionCall = errors.New("Did not find the expected right parenthesis character in the SQL expression")

// ErrParseExpectedTypeName is an error that occurs if a type name is expected
// but not found.
var ErrParseExpectedTypeName = errors.New("Did not find the expected type name in the SQL expression")

// ErrParseExpectedWhenClause is an error that occurs if a When clause is
// expected but not found.
var ErrParseExpectedWhenClause = errors.New("Did not find the expected WHEN clause in the SQL expression. CASE is not supported")

// ErrParseUnsupportedToken is an error that occurs if the SQL expression
// contains an unsupported token.
var ErrParseUnsupportedToken = errors.New("The SQL expression contains an unsupported token")

// ErrParseUnsupportedLiteralsGroupBy is an error that occurs if the SQL
// expression has an unsupported use of Group By.
var ErrParseUnsupportedLiteralsGroupBy = errors.New("The SQL expression contains an unsupported use of GROUP BY")

// ErrParseExpectedMember is an error that occurs if there is an unsupported use
// of member in the SQL expression.
var ErrParseExpectedMember = errors.New("The SQL expression contains an unsupported use of MEMBER")

// ErrParseUnsupportedSelect is an error that occurs if there is an unsupported
// use of Select.
var ErrParseUnsupportedSelect = errors.New("The SQL expression contains an unsupported use of SELECT")

// ErrParseUnsupportedCase is an error that occurs if there is an unsupported
// use of case.
var ErrParseUnsupportedCase = errors.New("The SQL expression contains an unsupported use of CASE")

// ErrParseUnsupportedCaseClause is an error that occurs if there is an
// unsupported use of case.
var ErrParseUnsupportedCaseClause = errors.New("The SQL expression contains an unsupported use of CASE")

// ErrParseUnsupportedAlias is an error that occurs if there is an unsupported
// use of Alias.
var ErrParseUnsupportedAlias = errors.New("The SQL expression contains an unsupported use of ALIAS")

// ErrParseUnsupportedSyntax is an error that occurs if there is an
// UnsupportedSyntax in the SQL expression.
var ErrParseUnsupportedSyntax = errors.New("The SQL expression contains unsupported syntax")

// ErrParseUnknownOperator is an error that occurs if there is an invalid
// operator present in the SQL expression.
var ErrParseUnknownOperator = errors.New("The SQL expression contains an invalid operator")

// ErrParseMissingIdentAfterAt is an error that occurs if the wrong symbol
// follows the "@" symbol in the SQL expression.
var ErrParseMissingIdentAfterAt = errors.New("Did not find the expected identifier after the @ symbol in the SQL expression")

// ErrParseUnexpectedOperator is an error that occurs if the SQL expression
// contains an unexpected operator.
var ErrParseUnexpectedOperator = errors.New("The SQL expression contains an unexpected operator")

// ErrParseUnexpectedTerm is an error that occurs if the SQL expression contains
// an unexpected term.
var ErrParseUnexpectedTerm = errors.New("The SQL expression contains an unexpected term")

// ErrParseUnexpectedToken is an error that occurs if the SQL expression
// contains an unexpected token.
var ErrParseUnexpectedToken = errors.New("The SQL expression contains an unexpected token")

// ErrParseUnexpectedKeyword is an error that occurs if the SQL expression
// contains an unexpected keyword.
var ErrParseUnexpectedKeyword = errors.New("The SQL expression contains an unexpected keyword")

// ErrParseExpectedExpression is an error that occurs if the SQL expression is
// not found.
var ErrParseExpectedExpression = errors.New("Did not find the expected SQL expression")

// ErrParseExpectedLeftParenAfterCast is an error that occurs if the left
// parenthesis is missing after a cast in the SQL expression.
var ErrParseExpectedLeftParenAfterCast = errors.New("Did not find the expected left parenthesis after CAST in the SQL expression")

// ErrParseExpectedLeftParenValueConstructor is an error that occurs if the left
// parenthesis is not found in the SQL expression.
var ErrParseExpectedLeftParenValueConstructor = errors.New("Did not find expected the left parenthesis in the SQL expression")

// ErrParseExpectedLeftParenBuiltinFunctionCall is an error that occurs if the
// left parenthesis is not found in the SQL expression function call.
var ErrParseExpectedLeftParenBuiltinFunctionCall = errors.New("Did not find the expected left parenthesis in the SQL expression")

// ErrParseExpectedArgumentDelimiter is an error that occurs if the argument
// delimiter for the SQL expression is not provided.
var ErrParseExpectedArgumentDelimiter = errors.New("Did not find the expected argument delimiter in the SQL expression")

// ErrParseCastArity is an error that occurs because the CAST has incorrect
// arity.
var ErrParseCastArity = errors.New("The SQL expression CAST has incorrect arity")

// ErrParseInvalidTypeParam is an error that occurs because there is an invalid
// parameter value.
var ErrParseInvalidTypeParam = errors.New("The SQL expression contains an invalid parameter value")

// ErrParseEmptySelect is an error that occurs because the SQL expression
// contains an empty Select
var ErrParseEmptySelect = errors.New("The SQL expression contains an empty SELECT")

// ErrParseSelectMissingFrom is an error that occurs because there is a missing
// From after the Select List.
var ErrParseSelectMissingFrom = errors.New("The SQL expression contains a missing FROM after SELECT list")

// ErrParseExpectedIdentForGroupName is an error that occurs because Group is
// not supported in the SQL expression.
var ErrParseExpectedIdentForGroupName = errors.New("GROUP is not supported in the SQL expression")

// ErrParseExpectedIdentForAlias is an error that occurs if expected identifier
// for alias is not in the SQL expression.
var ErrParseExpectedIdentForAlias = errors.New("Did not find the expected identifier for the alias in the SQL expression")

// ErrParseUnsupportedCallWithStar is an error that occurs if COUNT is used with
// an argument other than "*".
var ErrParseUnsupportedCallWithStar = errors.New("Only COUNT with (*) as a parameter is supported in the SQL expression")

// ErrParseNonUnaryAgregateFunctionCall is an error that occurs if more than one
// argument is provided as an argument for aggregation functions.
var ErrParseNonUnaryAgregateFunctionCall = errors.New("Only one argument is supported for aggregate functions in the SQL expression")

// ErrParseMalformedJoin is an error that occurs if a "join" operation is
// attempted in the SQL expression as this is not supported.
var ErrParseMalformedJoin = errors.New("JOIN is not supported in the SQL expression")

// ErrParseExpectedIdentForAt is an error that occurs if after "AT" an Alias
// identifier is not provided.
var ErrParseExpectedIdentForAt = errors.New("Did not find the expected identifier for AT name in the SQL expression")

// ErrParseAsteriskIsNotAloneInSelectList is an error that occurs if in addition
// to an asterix, more column names are provided as arguments in the SQL
// expression.
var ErrParseAsteriskIsNotAloneInSelectList = errors.New("Other expressions are not allowed in the SELECT list when '*' is used without dot notation in the SQL expression")

// ErrParseCannotMixSqbAndWildcardInSelectList is an error that occurs if list
// indexing and an asterix are mixed in the SQL expression.
var ErrParseCannotMixSqbAndWildcardInSelectList = errors.New("Cannot mix [] and * in the same expression in a SELECT list in SQL expression")

// ErrParseInvalidContextForWildcardInSelectList is an error that occurs if the
// asterix is used improperly within the SQL expression.
var ErrParseInvalidContextForWildcardInSelectList = errors.New("Invalid use of * in SELECT list in the SQL expression")

// ErrEvaluatorBindingDoesNotExist is an error that occurs if a column name or
// path provided in the expression does not exist.
var ErrEvaluatorBindingDoesNotExist = errors.New("A column name or a path provided does not exist in the SQL expression")

// ErrIncorrectSQLFunctionArgumentType is an error that occurs if the wrong
// argument is provided to a SQL function.
var ErrIncorrectSQLFunctionArgumentType = errors.New("Incorrect type of arguments in function call in the SQL expression")

// ErrAmbiguousFieldName is an error that occurs if the column name which is not
// case sensitive, is not descriptive enough to retrieve a singular column.
var ErrAmbiguousFieldName = errors.New("Field name matches to multiple fields in the file. Check the SQL expression and the file, and try again")

// ErrEvaluatorInvalidArguments is an error that occurs if there are not the
// correct number of arguments in a functional call to a SQL expression.
var ErrEvaluatorInvalidArguments = errors.New("Incorrect number of arguments in the function call in the SQL expression")

// ErrValueParseFailure is an error that occurs if the Time Stamp is not parsed
// correctly in the SQL expression.
var ErrValueParseFailure = errors.New("Time stamp parse failure in the SQL expression")

// ErrIntegerOverflow is an error that occurs if there is an IntegerOverflow or
// IntegerUnderFlow in the SQL expression.
var ErrIntegerOverflow = errors.New("Int overflow or underflow in the SQL expression")

// ErrLikeInvalidInputs is an error that occurs if invalid inputs are provided
// to the argument LIKE Clause.
var ErrLikeInvalidInputs = errors.New("Invalid argument given to the LIKE clause in the SQL expression")

// ErrCastFailed occurs if the attempt to convert data types in the cast is not
// done correctly.
var ErrCastFailed = errors.New("Attempt to convert from one data type to another using CAST failed in the SQL expression")

// ErrInvalidCast is an error that occurs if the attempt to convert data types
// failed and was done in an improper fashion.
var ErrInvalidCast = errors.New("Attempt to convert from one data type to another using CAST failed in the SQL expression")

// ErrEvaluatorInvalidTimestampFormatPattern is an error that occurs if the Time
// Stamp Format needs more additional fields to be filled.
var ErrEvaluatorInvalidTimestampFormatPattern = errors.New("Time stamp format pattern requires additional fields in the SQL expression")

// ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing is an error that
// occurs if the format of the time stamp can not be parsed.
var ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing = errors.New("Time stamp format pattern contains a valid format symbol that cannot be applied to time stamp parsing in the SQL expression")

// ErrEvaluatorTimestampFormatPatternDuplicateFields is an error that occurs if
// the time stamp format pattern contains multiple format specifications which
// can not be clearly resolved.
var ErrEvaluatorTimestampFormatPatternDuplicateFields = errors.New("Time stamp format pattern contains multiple format specifiers representing the time stamp field in the SQL expression")

//ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch is an error that
//occurs if the time stamp format pattern contains a 12 hour day of format but
//does not have an AM/PM field.
var ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch = errors.New("Time stamp format pattern contains a 12-hour hour of day format symbol but doesn't also contain an AM/PM field, or it contains a 24-hour hour of day format specifier and contains an AM/PM field in the SQL expression")

// ErrEvaluatorUnterminatedTimestampFormatPatternToken is an error that occurs
// if there is an unterminated token in the SQL expression for time stamp
// format.
var ErrEvaluatorUnterminatedTimestampFormatPatternToken = errors.New("Time stamp format pattern contains unterminated token in the SQL expression")

// ErrEvaluatorInvalidTimestampFormatPatternToken is an error that occurs if
// there is an invalid token in the time stamp format within the SQL expression.
var ErrEvaluatorInvalidTimestampFormatPatternToken = errors.New("Time stamp format pattern contains an invalid token in the SQL expression")

// ErrEvaluatorInvalidTimestampFormatPatternSymbol  is an error that occurs if
// the time stamp format pattern has an invalid symbol within the SQL
// expression.
var ErrEvaluatorInvalidTimestampFormatPatternSymbol = errors.New("Time stamp format pattern contains an invalid symbol in the SQL expression")

// S3 select API errors - TODO fix the errors.
var errorCodeResponse = map[error]string{
	ErrBusy:                                                   "Busy",
	ErrUnauthorizedAccess:                                     "UnauthorizedAccess",
	ErrExpressionTooLong:                                      "ExpressionTooLong",
	ErrIllegalSQLFunctionArgument:                             "IllegalSqlFunctionArgument",
	format.ErrInvalidColumnIndex:                              "InvalidColumnIndex",
	ErrInvalidKeyPath:                                         "InvalidKeyPath",
	ErrColumnTooLong:                                          "ColumnTooLong",
	ErrOverMaxColumn:                                          "OverMaxColumn",
	ErrOverMaxRecordSize:                                      "OverMaxRecordSize",
	ErrMissingHeaders:                                         "MissingHeaders",
	ErrInvalidCompressionFormat:                               "InvalidCompressionFormat",
	format.ErrTruncatedInput:                                  "TruncatedInput",
	ErrInvalidFileHeaderInfo:                                  "InvalidFileHeaderInfo",
	ErrInvalidJSONType:                                        "InvalidJsonType",
	ErrInvalidQuoteFields:                                     "InvalidQuoteFields",
	ErrInvalidRequestParameter:                                "InvalidRequestParameter",
	format.ErrCSVParsingError:                                 "CSVParsingError",
	format.ErrJSONParsingError:                                "JSONParsingError",
	ErrExternalEvalException:                                  "ExternalEvalException",
	ErrInvalidDataType:                                        "InvalidDataType",
	ErrUnrecognizedFormatException:                            "UnrecognizedFormatException",
	ErrInvalidTextEncoding:                                    "InvalidTextEncoding",
	ErrInvalidTableAlias:                                      "InvalidTableAlias",
	ErrMultipleDataSourcesUnsupported:                         "MultipleDataSourcesUnsupported",
	ErrMissingRequiredParameter:                               "MissingRequiredParameter",
	ErrObjectSerializationConflict:                            "ObjectSerializationConflict",
	ErrUnsupportedSQLOperation:                                "UnsupportedSqlOperation",
	ErrUnsupportedSQLStructure:                                "UnsupportedSqlStructure",
	ErrUnsupportedStorageClass:                                "UnsupportedStorageClass",
	ErrUnsupportedSyntax:                                      "UnsupportedSyntax",
	ErrUnsupportedRangeHeader:                                 "UnsupportedRangeHeader",
	ErrLexerInvalidChar:                                       "LexerInvalidChar",
	ErrLexerInvalidOperator:                                   "LexerInvalidOperator",
	ErrLexerInvalidLiteral:                                    "LexerInvalidLiteral",
	ErrLexerInvalidIONLiteral:                                 "LexerInvalidIONLiteral",
	ErrParseExpectedDatePart:                                  "ParseExpectedDatePart",
	ErrParseExpectedKeyword:                                   "ParseExpectedKeyword",
	ErrParseExpectedTokenType:                                 "ParseExpectedTokenType",
	ErrParseExpected2TokenTypes:                               "ParseExpected2TokenTypes",
	ErrParseExpectedNumber:                                    "ParseExpectedNumber",
	ErrParseExpectedRightParenBuiltinFunctionCall:             "ParseExpectedRightParenBuiltinFunctionCall",
	ErrParseExpectedTypeName:                                  "ParseExpectedTypeName",
	ErrParseExpectedWhenClause:                                "ParseExpectedWhenClause",
	ErrParseUnsupportedToken:                                  "ParseUnsupportedToken",
	ErrParseUnsupportedLiteralsGroupBy:                        "ParseUnsupportedLiteralsGroupBy",
	ErrParseExpectedMember:                                    "ParseExpectedMember",
	ErrParseUnsupportedSelect:                                 "ParseUnsupportedSelect",
	ErrParseUnsupportedCase:                                   "ParseUnsupportedCase:",
	ErrParseUnsupportedCaseClause:                             "ParseUnsupportedCaseClause",
	ErrParseUnsupportedAlias:                                  "ParseUnsupportedAlias",
	ErrParseUnsupportedSyntax:                                 "ParseUnsupportedSyntax",
	ErrParseUnknownOperator:                                   "ParseUnknownOperator",
	format.ErrParseInvalidPathComponent:                       "ParseInvalidPathComponent",
	ErrParseMissingIdentAfterAt:                               "ParseMissingIdentAfterAt",
	ErrParseUnexpectedOperator:                                "ParseUnexpectedOperator",
	ErrParseUnexpectedTerm:                                    "ParseUnexpectedTerm",
	ErrParseUnexpectedToken:                                   "ParseUnexpectedToken",
	ErrParseUnexpectedKeyword:                                 "ParseUnexpectedKeyword",
	ErrParseExpectedExpression:                                "ParseExpectedExpression",
	ErrParseExpectedLeftParenAfterCast:                        "ParseExpectedLeftParenAfterCast",
	ErrParseExpectedLeftParenValueConstructor:                 "ParseExpectedLeftParenValueConstructor",
	ErrParseExpectedLeftParenBuiltinFunctionCall:              "ParseExpectedLeftParenBuiltinFunctionCall",
	ErrParseExpectedArgumentDelimiter:                         "ParseExpectedArgumentDelimiter",
	ErrParseCastArity:                                         "ParseCastArity",
	ErrParseInvalidTypeParam:                                  "ParseInvalidTypeParam",
	ErrParseEmptySelect:                                       "ParseEmptySelect",
	ErrParseSelectMissingFrom:                                 "ParseSelectMissingFrom",
	ErrParseExpectedIdentForGroupName:                         "ParseExpectedIdentForGroupName",
	ErrParseExpectedIdentForAlias:                             "ParseExpectedIdentForAlias",
	ErrParseUnsupportedCallWithStar:                           "ParseUnsupportedCallWithStar",
	ErrParseNonUnaryAgregateFunctionCall:                      "ParseNonUnaryAgregateFunctionCall",
	ErrParseMalformedJoin:                                     "ParseMalformedJoin",
	ErrParseExpectedIdentForAt:                                "ParseExpectedIdentForAt",
	ErrParseAsteriskIsNotAloneInSelectList:                    "ParseAsteriskIsNotAloneInSelectList",
	ErrParseCannotMixSqbAndWildcardInSelectList:               "ParseCannotMixSqbAndWildcardInSelectList",
	ErrParseInvalidContextForWildcardInSelectList:             "ParseInvalidContextForWildcardInSelectList",
	ErrEvaluatorBindingDoesNotExist:                           "EvaluatorBindingDoesNotExist",
	ErrIncorrectSQLFunctionArgumentType:                       "IncorrectSqlFunctionArgumentType",
	ErrAmbiguousFieldName:                                     "AmbiguousFieldName",
	ErrEvaluatorInvalidArguments:                              "EvaluatorInvalidArguments",
	ErrValueParseFailure:                                      "ValueParseFailure",
	ErrIntegerOverflow:                                        "IntegerOverflow",
	ErrLikeInvalidInputs:                                      "LikeInvalidInputs",
	ErrCastFailed:                                             "CastFailed",
	ErrInvalidCast:                                            "Attempt to convert from one data type to another using CAST failed in the SQL expression.",
	ErrEvaluatorInvalidTimestampFormatPattern:                 "EvaluatorInvalidTimestampFormatPattern",
	ErrEvaluatorInvalidTimestampFormatPatternSymbolForParsing: "EvaluatorInvalidTimestampFormatPatternSymbolForParsing",
	ErrEvaluatorTimestampFormatPatternDuplicateFields:         "EvaluatorTimestampFormatPatternDuplicateFields",
	ErrEvaluatorTimestampFormatPatternHourClockAmPmMismatch:   "EvaluatorTimestampFormatPatternHourClockAmPmMismatch",
	ErrEvaluatorUnterminatedTimestampFormatPatternToken:       "EvaluatorUnterminatedTimestampFormatPatternToken",
	ErrEvaluatorInvalidTimestampFormatPatternToken:            "EvaluatorInvalidTimestampFormatPatternToken",
	ErrEvaluatorInvalidTimestampFormatPatternSymbol:           "EvaluatorInvalidTimestampFormatPatternSymbol",
}
