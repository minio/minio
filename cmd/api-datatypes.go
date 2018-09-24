/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package cmd

import (
	"encoding/xml"
)

const (
	// Response request id.
	responseRequestIDKey = "x-amz-request-id"
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

// SelectCompressionType - ONLY GZIP is supported
type SelectCompressionType string

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

// JSONType determines json input serialization type.
type JSONType string

// Constants for JSONTypes.
const (
	JSONDocumentType JSONType = "Document"
	JSONLinesType             = "Lines"
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

// ObjectIdentifier carries key name for the object to delete.
type ObjectIdentifier struct {
	ObjectName string `xml:"Key"`
}

// createBucketConfiguration container for bucket configuration request from client.
// Used for parsing the location from the request body for MakeBucketbucket.
type createBucketLocationConfiguration struct {
	XMLName  xml.Name `xml:"CreateBucketConfiguration" json:"-"`
	Location string   `xml:"LocationConstraint"`
}

// DeleteObjectsRequest - xml carrying the object key names which needs to be deleted.
type DeleteObjectsRequest struct {
	// Element to enable quiet mode for the request
	Quiet bool
	// List of objects to be deleted
	Objects []ObjectIdentifier `xml:"Object"`
}
