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

package format

import "errors"

// ErrTruncatedInput is an error if the object is not compressed properly and an
// error occurs during decompression.
var ErrTruncatedInput = errors.New("Object decompression failed. Check that the object is properly compressed using the format specified in the request")

// ErrCSVParsingError is an error if the CSV presents an error while being
// parsed.
var ErrCSVParsingError = errors.New("Encountered an Error parsing the CSV file. Check the file and try again")

// ErrInvalidColumnIndex is an error if you provide a column index which is not
// valid.
var ErrInvalidColumnIndex = errors.New("Column index in the SQL expression is invalid")

// ErrParseInvalidPathComponent is an error that occurs if there is an invalid
// path component.
var ErrParseInvalidPathComponent = errors.New("The SQL expression contains an invalid path component")

// ErrJSONParsingError is an error if while parsing the JSON an error arises.
var ErrJSONParsingError = errors.New("Encountered an error parsing the JSON file. Check the file and try again")
