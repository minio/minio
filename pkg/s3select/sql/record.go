/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

import "github.com/bcicen/jstream"

// SelectObjectFormat specifies the format of the underlying data
type SelectObjectFormat int

const (
	// SelectFmtUnknown - unknown format (default value)
	SelectFmtUnknown SelectObjectFormat = iota
	// SelectFmtCSV - CSV format
	SelectFmtCSV
	// SelectFmtJSON - JSON format
	SelectFmtJSON
	// SelectFmtParquet - Parquet format
	SelectFmtParquet
)

// Record - is a type containing columns and their values.
type Record interface {
	Get(name string) (*Value, error)
	Set(name string, value *Value) error
	MarshalCSV(fieldDelimiter rune) ([]byte, error)
	MarshalJSON() ([]byte, error)

	// Returns underlying representation
	Raw() (SelectObjectFormat, interface{})

	// Replaces the underlying data
	Replace(k jstream.KVS) error
}
