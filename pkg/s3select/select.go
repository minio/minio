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

package s3select

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/minio/minio/pkg/s3select/csv"
	"github.com/minio/minio/pkg/s3select/json"
	"github.com/minio/minio/pkg/s3select/parquet"
	"github.com/minio/minio/pkg/s3select/sql"
)

type recordReader interface {
	Read() (sql.Record, error)
	Close() error
}

const (
	csvFormat     = "csv"
	jsonFormat    = "json"
	parquetFormat = "parquet"
)

// CompressionType - represents value inside <CompressionType/> in request XML.
type CompressionType string

const (
	noneType  CompressionType = "none"
	gzipType  CompressionType = "gzip"
	bzip2Type CompressionType = "bzip2"
)

const (
	maxRecordSize = 1 << 20 // 1 MiB
)

// UnmarshalXML - decodes XML data.
func (c *CompressionType) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		return errMalformedXML(err)
	}

	parsedType := CompressionType(strings.ToLower(s))
	if s == "" {
		parsedType = noneType
	}

	switch parsedType {
	case noneType, gzipType, bzip2Type:
	default:
		return errInvalidCompressionFormat(fmt.Errorf("invalid compression format '%v'", s))
	}

	*c = parsedType
	return nil
}

// InputSerialization - represents elements inside <InputSerialization/> in request XML.
type InputSerialization struct {
	CompressionType CompressionType    `xml:"CompressionType"`
	CSVArgs         csv.ReaderArgs     `xml:"CSV"`
	JSONArgs        json.ReaderArgs    `xml:"JSON"`
	ParquetArgs     parquet.ReaderArgs `xml:"Parquet"`
	unmarshaled     bool
	format          string
}

// IsEmpty - returns whether input serialization is empty or not.
func (input *InputSerialization) IsEmpty() bool {
	return !input.unmarshaled
}

// UnmarshalXML - decodes XML data.
func (input *InputSerialization) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type subInputSerialization InputSerialization
	parsedInput := subInputSerialization{}
	if err := d.DecodeElement(&parsedInput, &start); err != nil {
		return errMalformedXML(err)
	}

	// If no compression is specified, set to noneType
	if parsedInput.CompressionType == CompressionType("") {
		parsedInput.CompressionType = noneType
	}

	found := 0
	if !parsedInput.CSVArgs.IsEmpty() {
		parsedInput.format = csvFormat
		found++
	}
	if !parsedInput.JSONArgs.IsEmpty() {
		parsedInput.format = jsonFormat
		found++
	}
	if !parsedInput.ParquetArgs.IsEmpty() {
		if parsedInput.CompressionType != "" && parsedInput.CompressionType != noneType {
			return errInvalidRequestParameter(fmt.Errorf("CompressionType must be NONE for Parquet format"))
		}

		parsedInput.format = parquetFormat
		found++
	}

	if found != 1 {
		return errInvalidDataSource(nil)
	}

	*input = InputSerialization(parsedInput)
	input.unmarshaled = true
	return nil
}

// OutputSerialization - represents elements inside <OutputSerialization/> in request XML.
type OutputSerialization struct {
	CSVArgs     csv.WriterArgs  `xml:"CSV"`
	JSONArgs    json.WriterArgs `xml:"JSON"`
	unmarshaled bool
	format      string
}

// IsEmpty - returns whether output serialization is empty or not.
func (output *OutputSerialization) IsEmpty() bool {
	return !output.unmarshaled
}

// UnmarshalXML - decodes XML data.
func (output *OutputSerialization) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type subOutputSerialization OutputSerialization
	parsedOutput := subOutputSerialization{}
	if err := d.DecodeElement(&parsedOutput, &start); err != nil {
		return errMalformedXML(err)
	}

	found := 0
	if !parsedOutput.CSVArgs.IsEmpty() {
		parsedOutput.format = csvFormat
		found++
	}
	if !parsedOutput.JSONArgs.IsEmpty() {
		parsedOutput.format = jsonFormat
		found++
	}
	if found != 1 {
		return errObjectSerializationConflict(fmt.Errorf("either CSV or JSON should be present in OutputSerialization"))
	}

	*output = OutputSerialization(parsedOutput)
	output.unmarshaled = true
	return nil
}

// RequestProgress - represents elements inside <RequestProgress/> in request XML.
type RequestProgress struct {
	Enabled bool `xml:"Enabled"`
}

// S3Select - filters the contents on a simple structured query language (SQL) statement. It
// represents elements inside <SelectRequest/> in request XML specified in detail at
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html.
type S3Select struct {
	XMLName        xml.Name            `xml:"SelectRequest"`
	Expression     string              `xml:"Expression"`
	ExpressionType string              `xml:"ExpressionType"`
	Input          InputSerialization  `xml:"InputSerialization"`
	Output         OutputSerialization `xml:"OutputSerialization"`
	Progress       RequestProgress     `xml:"RequestProgress"`

	statement      *sql.SelectStatement
	progressReader *progressReader
	recordReader   recordReader
}

var (
	legacyXMLName = "SelectObjectContentRequest"
)

// UnmarshalXML - decodes XML data.
func (s3Select *S3Select) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// S3 also supports the older SelectObjectContentRequest tag,
	// though it is no longer found in documentation. This is
	// checked and renamed below to allow older clients to also
	// work.
	if start.Name.Local == legacyXMLName {
		start.Name = xml.Name{Space: "", Local: "SelectRequest"}
	}

	// Make subtype to avoid recursive UnmarshalXML().
	type subS3Select S3Select
	parsedS3Select := subS3Select{}
	if err := d.DecodeElement(&parsedS3Select, &start); err != nil {
		if _, ok := err.(*s3Error); ok {
			return err
		}

		return errMalformedXML(err)
	}

	parsedS3Select.ExpressionType = strings.ToLower(parsedS3Select.ExpressionType)
	if parsedS3Select.ExpressionType != "sql" {
		return errInvalidExpressionType(fmt.Errorf("invalid expression type '%v'", parsedS3Select.ExpressionType))
	}

	if parsedS3Select.Input.IsEmpty() {
		return errMissingRequiredParameter(fmt.Errorf("InputSerialization must be provided"))
	}

	if parsedS3Select.Output.IsEmpty() {
		return errMissingRequiredParameter(fmt.Errorf("OutputSerialization must be provided"))
	}

	statement, err := sql.ParseSelectStatement(parsedS3Select.Expression)
	if err != nil {
		return err
	}

	parsedS3Select.statement = &statement

	*s3Select = S3Select(parsedS3Select)
	return nil
}

func (s3Select *S3Select) outputRecord() sql.Record {
	switch s3Select.Output.format {
	case csvFormat:
		return csv.NewRecord()
	case jsonFormat:
		return json.NewRecord(sql.SelectFmtJSON)
	}

	panic(fmt.Errorf("unknown output format '%v'", s3Select.Output.format))
}

func (s3Select *S3Select) getProgress() (bytesScanned, bytesProcessed int64) {
	if s3Select.progressReader != nil {
		return s3Select.progressReader.Stats()
	}

	return -1, -1
}

// Open - opens S3 object by using callback for SQL selection query.
// Currently CSV, JSON and Apache Parquet formats are supported.
func (s3Select *S3Select) Open(getReader func(offset, length int64) (io.ReadCloser, error)) error {
	switch s3Select.Input.format {
	case csvFormat:
		rc, err := getReader(0, -1)
		if err != nil {
			return err
		}

		s3Select.progressReader, err = newProgressReader(rc, s3Select.Input.CompressionType)
		if err != nil {
			return err
		}

		s3Select.recordReader, err = csv.NewReader(s3Select.progressReader, &s3Select.Input.CSVArgs)
		if err != nil {
			return err
		}

		return nil
	case jsonFormat:
		rc, err := getReader(0, -1)
		if err != nil {
			return err
		}

		s3Select.progressReader, err = newProgressReader(rc, s3Select.Input.CompressionType)
		if err != nil {
			return err
		}

		s3Select.recordReader = json.NewReader(s3Select.progressReader, &s3Select.Input.JSONArgs)
		return nil
	case parquetFormat:
		var err error
		s3Select.recordReader, err = parquet.NewReader(getReader, &s3Select.Input.ParquetArgs)
		return err
	}

	panic(fmt.Errorf("unknown input format '%v'", s3Select.Input.format))
}

func (s3Select *S3Select) marshal(record sql.Record) ([]byte, error) {
	switch s3Select.Output.format {
	case csvFormat:
		data, err := record.MarshalCSV([]rune(s3Select.Output.CSVArgs.FieldDelimiter)[0])
		if err != nil {
			return nil, err
		}

		return append(data, []byte(s3Select.Output.CSVArgs.RecordDelimiter)...), nil
	case jsonFormat:
		data, err := record.MarshalJSON()
		if err != nil {
			return nil, err
		}

		return append(data, []byte(s3Select.Output.JSONArgs.RecordDelimiter)...), nil
	}

	panic(fmt.Errorf("unknown output format '%v'", s3Select.Output.format))
}

// Evaluate - filters and sends records read from opened reader as per select statement to http response writer.
func (s3Select *S3Select) Evaluate(w http.ResponseWriter) {
	getProgressFunc := s3Select.getProgress
	if !s3Select.Progress.Enabled {
		getProgressFunc = nil
	}
	writer := newMessageWriter(w, getProgressFunc)

	var inputRecord sql.Record
	var outputRecord sql.Record
	var err error
	var data []byte
	sendRecord := func() bool {
		if outputRecord == nil {
			return true
		}

		if data, err = s3Select.marshal(outputRecord); err != nil {
			return false
		}

		if len(data) > maxRecordSize {
			writer.FinishWithError("OverMaxRecordSize", "The length of a record in the input or result is greater than maxCharsPerRecord of 1 MB.")
			return false
		}

		if err = writer.SendRecord(data); err != nil {
			// FIXME: log this error.
			err = nil
			return false
		}

		return true
	}

	for {
		if s3Select.statement.LimitReached() {
			if err = writer.Finish(s3Select.getProgress()); err != nil {
				// FIXME: log this error.
				err = nil
			}
			break
		}

		if inputRecord, err = s3Select.recordReader.Read(); err != nil {
			if err != io.EOF {
				break
			}

			if s3Select.statement.IsAggregated() {
				outputRecord = s3Select.outputRecord()
				if err = s3Select.statement.AggregateResult(outputRecord); err != nil {
					break
				}

				if !sendRecord() {
					break
				}
			}

			if err = writer.Finish(s3Select.getProgress()); err != nil {
				// FIXME: log this error.
				err = nil
			}
			break
		}

		if inputRecord, err = s3Select.statement.EvalFrom(s3Select.Input.format, inputRecord); err != nil {
			break
		}

		if s3Select.statement.IsAggregated() {
			if err = s3Select.statement.AggregateRow(inputRecord); err != nil {
				break
			}
		} else {
			outputRecord = s3Select.outputRecord()
			if outputRecord, err = s3Select.statement.Eval(inputRecord, outputRecord); err != nil {
				break
			}

			if !sendRecord() {
				break
			}
		}
	}

	if err != nil {
		_ = writer.FinishWithError("InternalError", err.Error())
	}
}

// Close - closes opened S3 object.
func (s3Select *S3Select) Close() error {
	return s3Select.recordReader.Close()
}

// NewS3Select - creates new S3Select by given request XML reader.
func NewS3Select(r io.Reader) (*S3Select, error) {
	s3Select := &S3Select{}
	if err := xml.NewDecoder(r).Decode(s3Select); err != nil {
		return nil, err
	}

	return s3Select, nil
}
