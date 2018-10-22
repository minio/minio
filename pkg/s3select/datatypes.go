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
	"encoding/xml"
)

// CSVFileHeaderInfo -Can be either USE IGNORE OR NONE, defines what to do with
// the first row
type CSVFileHeaderInfo string

// Constants for file header info.
const (
	CSVFileHeaderInfoNone   CSVFileHeaderInfo = "NONE"
	CSVFileHeaderInfoIgnore                   = "IGNORE"
	CSVFileHeaderInfoUse                      = "USE"
)

// The maximum character per record is set to be 1 MB.
const (
	MaxCharsPerRecord = 1000000
)

// SelectCompressionType - ONLY GZIP is supported
type SelectCompressionType string

// JSONType determines json input serialization type.
type JSONType string

// Constants for compression types under select API.
const (
	SelectCompressionNONE SelectCompressionType = "NONE"
	SelectCompressionGZIP                       = "GZIP"
	SelectCompressionBZIP                       = "BZIP2"
)

// CSVQuoteFields - Can be either Always or AsNeeded
type CSVQuoteFields string

// Constants for csv quote styles.
const (
	CSVQuoteFieldsAlways   CSVQuoteFields = "Always"
	CSVQuoteFieldsAsNeeded                = "AsNeeded"
)

// QueryExpressionType - Currently can only be SQL
type QueryExpressionType string

// Constants for expression type.
const (
	QueryExpressionTypeSQL QueryExpressionType = "SQL"
)

// Constants for JSONTypes.
const (
	JSONTypeDocument JSONType = "DOCUMENT"
	JSONLinesType             = "LINES"
)

// ObjectSelectRequest - represents the input select body
type ObjectSelectRequest struct {
	XMLName            xml.Name `xml:"SelectObjectContentRequest" json:"-"`
	Expression         string
	ExpressionType     QueryExpressionType
	InputSerialization struct {
		CompressionType SelectCompressionType
		Parquet         *struct{}
		CSV             *struct {
			FileHeaderInfo       CSVFileHeaderInfo
			RecordDelimiter      string
			FieldDelimiter       string
			QuoteCharacter       string
			QuoteEscapeCharacter string
			Comments             string
		}
		JSON *struct {
			Type JSONType
		}
	}
	OutputSerialization struct {
		CSV *struct {
			QuoteFields          CSVQuoteFields
			RecordDelimiter      string
			FieldDelimiter       string
			QuoteCharacter       string
			QuoteEscapeCharacter string
		}
		JSON *struct {
			RecordDelimiter string
		}
	}
	RequestProgress struct {
		Enabled bool
	}
}
