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

import "encoding/xml"

// Select Interface helper methods, implementing features needed for
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
type Select interface {
	Type() Type
	OutputType() Type
	Read() ([]byte, error)
	Header() []string
	HasHeader() bool
	OutputFieldDelimiter() string
	OutputRecordDelimiter() string
	UpdateBytesProcessed(int64)
	Expression() string
	UpdateBytesReturned(int64)
	CreateStatXML() (string, error)
	CreateProgressXML() (string, error)
	ColNameErrs(columnNames []string) error
	Progress() bool
}

// Progress represents a struct that represents the format for XML of the
// progress messages
type Progress struct {
	XMLName        xml.Name `xml:"Progress" json:"-"`
	BytesScanned   int64    `xml:"BytesScanned"`
	BytesProcessed int64    `xml:"BytesProcessed"`
	BytesReturned  int64    `xml:"BytesReturned"`
}

// Stats represents a struct that represents the format for XML of the stat
// messages
type Stats struct {
	XMLName        xml.Name `xml:"Stats" json:"-"`
	BytesScanned   int64    `xml:"BytesScanned"`
	BytesProcessed int64    `xml:"BytesProcessed"`
	BytesReturned  int64    `xml:"BytesReturned"`
}

// Type different types of support data format types.
type Type string

// Different data format types.
const (
	JSON Type = "json"
	CSV  Type = "csv"
)
