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

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/config"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/s3select/csv"
	"github.com/minio/minio/internal/s3select/json"
	"github.com/minio/minio/internal/s3select/parquet"
	"github.com/minio/minio/internal/s3select/simdj"
	"github.com/minio/minio/internal/s3select/sql"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/simdjson-go"
	"github.com/pierrec/lz4/v4"
)

type recordReader interface {
	// Read a record.
	// dst is optional but will be used if valid.
	Read(dst sql.Record) (sql.Record, error)
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
	gzipType  CompressionType = "GZIP"
	bzip2Type CompressionType = "BZIP2"

	zstdType   CompressionType = "ZSTD"
	lz4Type    CompressionType = "LZ4"
	s2Type     CompressionType = "S2"
	snappyType CompressionType = "SNAPPY"
)

const (
	maxRecordSize = 1 << 20 // 1 MiB
)

var parquetSupport bool

func init() {
	parquetSupport = env.Get("MINIO_API_SELECT_PARQUET", config.EnableOff) == config.EnableOn
}

var bufPool = bpool.Pool[*bytes.Buffer]{
	New: func() *bytes.Buffer {
		// make a buffer with a reasonable capacity.
		return bytes.NewBuffer(make([]byte, 0, maxRecordSize))
	},
}

var bufioWriterPool = bpool.Pool[*bufio.Writer]{
	New: func() *bufio.Writer {
		// io.Discard is just used to create the writer. Actual destination
		// writer is set later by Reset() before using it.
		return bufio.NewWriter(xioutil.Discard)
	},
}

// UnmarshalXML - decodes XML data.
func (c *CompressionType) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		return errMalformedXML(err)
	}

	parsedType := CompressionType(strings.ToUpper(s))
	if s == "" || parsedType == "NONE" {
		parsedType = noneType
	}

	switch parsedType {
	case noneType, gzipType, bzip2Type, snappyType, s2Type, zstdType, lz4Type:
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
	if parsedInput.CompressionType == "" {
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

// ScanRange represents the ScanRange parameter.
type ScanRange struct {
	// Start is the byte offset to read from (from the start of the file).
	Start *uint64 `xml:"Start"`
	// End is the offset of the last byte that should be returned when Start
	// is set, otherwise it is the offset from EOF to start reading.
	End *uint64 `xml:"End"`
}

// Validate if the scan range is valid.
func (s *ScanRange) Validate() error {
	if s == nil {
		return nil
	}
	if s.Start == nil && s.End == nil {
		// This parameter is optional, but when specified, it must not be empty.
		// Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html#AmazonS3-SelectObjectContent-request-ScanRange
		return errors.New("ScanRange: No Start or End specified")
	}
	if s.Start == nil || s.End == nil {
		return nil
	}
	if *s.Start > *s.End {
		return errors.New("ScanRange: Start cannot be after end")
	}
	return nil
}

// StartLen returns start offset plus length from range.
func (s *ScanRange) StartLen() (start, length int64, err error) {
	if s == nil {
		return 0, -1, nil
	}
	err = s.Validate()
	if err != nil {
		return 0, 0, err
	}

	if s.End == nil && s.Start == nil {
		// Not valid, but should be caught above.
		return 0, -1, nil
	}
	if s.End == nil {
		start := int64(*s.Start)
		if start < 0 {
			return 0, 0, errors.New("ScanRange: Start after EOF")
		}
		return start, -1, nil
	}
	if s.Start == nil {
		// Suffix length
		end := int64(*s.End)
		if end < 0 {
			return 0, 0, errors.New("ScanRange: End bigger than file")
		}
		// Suffix length
		return -end, -1, nil
	}
	start = int64(*s.Start)
	end := int64(*s.End)
	return start, end - start + 1, nil
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
	ScanRange      *ScanRange          `xml:"ScanRange"`

	statement      *sql.SelectStatement
	progressReader *progressReader
	recordReader   recordReader
}

var legacyXMLName = "SelectObjectContentRequest"

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
	if err := parsedS3Select.ScanRange.Validate(); err != nil {
		return errInvalidScanRangeParameter(err)
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
func (s3Select *S3Select) Open(rsc io.ReadSeekCloser) error {
	offset, length, err := s3Select.ScanRange.StartLen()
	if err != nil {
		return err
	}
	seekDirection := io.SeekStart
	if offset < 0 {
		seekDirection = io.SeekEnd
	}
	switch s3Select.Input.format {
	case csvFormat:
		_, err = rsc.Seek(offset, seekDirection)
		if err != nil {
			return err
		}
		var rc io.ReadCloser = rsc
		if length != -1 {
			rc = newLimitedReadCloser(rsc, length)
		}

		s3Select.progressReader, err = newProgressReader(rc, s3Select.Input.CompressionType)
		if err != nil {
			rsc.Close()
			return err
		}

		s3Select.recordReader, err = csv.NewReader(s3Select.progressReader, &s3Select.Input.CSVArgs)
		if err != nil {
			// Close all reader resources opened so far.
			s3Select.progressReader.Close()

			var stErr bzip2.StructuralError
			if errors.As(err, &stErr) {
				return errInvalidCompression(err, s3Select.Input.CompressionType)
			}
			// Test these compressor errors
			errs := []error{
				gzip.ErrHeader, gzip.ErrChecksum,
				s2.ErrCorrupt, s2.ErrUnsupported, s2.ErrCRC,
				zstd.ErrBlockTooSmall, zstd.ErrMagicMismatch, zstd.ErrWindowSizeExceeded, zstd.ErrUnknownDictionary, zstd.ErrWindowSizeTooSmall,
				lz4.ErrInvalidFrame, lz4.ErrInvalidBlockChecksum, lz4.ErrInvalidFrameChecksum, lz4.ErrInvalidFrameChecksum,
				lz4.ErrInvalidHeaderChecksum, lz4.ErrInvalidSourceShortBuffer, lz4.ErrInternalUnhandledState,
			}
			for _, e := range errs {
				if errors.Is(err, e) {
					return errInvalidCompression(err, s3Select.Input.CompressionType)
				}
			}
			return err
		}
		return nil
	case jsonFormat:
		_, err = rsc.Seek(offset, seekDirection)
		if err != nil {
			return err
		}
		var rc io.ReadCloser = rsc
		if length != -1 {
			rc = newLimitedReadCloser(rsc, length)
		}

		s3Select.progressReader, err = newProgressReader(rc, s3Select.Input.CompressionType)
		if err != nil {
			rsc.Close()
			return err
		}

		if strings.EqualFold(s3Select.Input.JSONArgs.ContentType, "lines") {
			if simdjson.SupportedCPU() {
				s3Select.recordReader = simdj.NewReader(s3Select.progressReader, &s3Select.Input.JSONArgs)
			} else {
				s3Select.recordReader = json.NewPReader(s3Select.progressReader, &s3Select.Input.JSONArgs)
			}
		} else {
			// Document mode.
			s3Select.recordReader = json.NewReader(s3Select.progressReader, &s3Select.Input.JSONArgs)
		}

		return nil
	case parquetFormat:
		if !parquetSupport {
			return errors.New("parquet format parsing not enabled on server")
		}
		if offset != 0 || length != -1 {
			// Offsets do not make sense in parquet files.
			return errors.New("parquet format does not support offsets")
		}
		var err error
		s3Select.recordReader, err = parquet.NewParquetReader(rsc, &s3Select.Input.ParquetArgs)
		return err
	}

	return fmt.Errorf("unknown input format '%v'", s3Select.Input.format)
}

func (s3Select *S3Select) marshal(buf *bytes.Buffer, record sql.Record) error {
	switch s3Select.Output.format {
	case csvFormat:
		// Use bufio Writer to prevent csv.Writer from allocating a new buffer.
		bufioWriter := bufioWriterPool.Get()
		defer func() {
			bufioWriter.Reset(xioutil.Discard)
			bufioWriterPool.Put(bufioWriter)
		}()

		bufioWriter.Reset(buf)
		opts := sql.WriteCSVOpts{
			FieldDelimiter: []rune(s3Select.Output.CSVArgs.FieldDelimiter)[0],
			Quote:          []rune(s3Select.Output.CSVArgs.QuoteCharacter)[0],
			QuoteEscape:    []rune(s3Select.Output.CSVArgs.QuoteEscapeCharacter)[0],
			AlwaysQuote:    strings.EqualFold(s3Select.Output.CSVArgs.QuoteFields, "always"),
		}
		err := record.WriteCSV(bufioWriter, opts)
		if err != nil {
			return err
		}
		err = bufioWriter.Flush()
		if err != nil {
			return err
		}
		if buf.Bytes()[buf.Len()-1] == '\n' {
			buf.Truncate(buf.Len() - 1)
		}
		buf.WriteString(s3Select.Output.CSVArgs.RecordDelimiter)

		return nil
	case jsonFormat:
		err := record.WriteJSON(buf)
		if err != nil {
			return err
		}
		// Trim trailing newline from non-simd output
		if buf.Bytes()[buf.Len()-1] == '\n' {
			buf.Truncate(buf.Len() - 1)
		}
		buf.WriteString(s3Select.Output.JSONArgs.RecordDelimiter)

		return nil
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

	var outputQueue []sql.Record

	// Create queue based on the type.
	if s3Select.statement.IsAggregated() {
		outputQueue = make([]sql.Record, 0, 1)
	} else {
		outputQueue = make([]sql.Record, 0, 100)
	}
	var err error
	sendRecord := func() bool {
		buf := bufPool.Get()
		buf.Reset()

		for _, outputRecord := range outputQueue {
			if outputRecord == nil {
				continue
			}
			before := buf.Len()
			if err = s3Select.marshal(buf, outputRecord); err != nil {
				bufPool.Put(buf)
				return false
			}
			if buf.Len()-before > maxRecordSize {
				writer.FinishWithError("OverMaxRecordSize", "The length of a record in the input or result is greater than maxCharsPerRecord of 1 MB.")
				bufPool.Put(buf)
				return false
			}
		}

		if err = writer.SendRecord(buf); err != nil {
			// FIXME: log this error.
			err = nil
			bufPool.Put(buf)
			return false
		}
		outputQueue = outputQueue[:0]
		return true
	}

	var rec sql.Record
OuterLoop:
	for {
		if s3Select.statement.LimitReached() {
			if !sendRecord() {
				break
			}
			if err = writer.Finish(s3Select.getProgress()); err != nil {
				// FIXME: log this error.
				err = nil
			}
			break
		}

		if rec, err = s3Select.recordReader.Read(rec); err != nil {
			if err != io.EOF {
				break
			}

			if s3Select.statement.IsAggregated() {
				outputRecord := s3Select.outputRecord()
				if err = s3Select.statement.AggregateResult(outputRecord); err != nil {
					break
				}
				outputQueue = append(outputQueue, outputRecord)
			}

			if !sendRecord() {
				break
			}

			if err = writer.Finish(s3Select.getProgress()); err != nil {
				// FIXME: log this error.
				err = nil
			}
			break
		}

		var inputRecords []*sql.Record
		if inputRecords, err = s3Select.statement.EvalFrom(s3Select.Input.format, rec); err != nil {
			break
		}

		for _, inputRecord := range inputRecords {
			if s3Select.statement.IsAggregated() {
				if err = s3Select.statement.AggregateRow(*inputRecord); err != nil {
					break OuterLoop
				}
			} else {
				var outputRecord sql.Record
				// We will attempt to reuse the records in the table.
				// The type of these should not change.
				// The queue should always have at least one entry left for this to work.
				outputQueue = outputQueue[:len(outputQueue)+1]
				if t := outputQueue[len(outputQueue)-1]; t != nil {
					// If the output record is already set, we reuse it.
					outputRecord = t
					outputRecord.Reset()
				} else {
					// Create new one
					outputRecord = s3Select.outputRecord()
					outputQueue[len(outputQueue)-1] = outputRecord
				}
				outputRecord, err = s3Select.statement.Eval(*inputRecord, outputRecord)
				if outputRecord == nil || err != nil {
					// This should not be written.
					// Remove it from the queue.
					outputQueue = outputQueue[:len(outputQueue)-1]
					if err != nil {
						break OuterLoop
					}
					continue
				}

				outputQueue[len(outputQueue)-1] = outputRecord
				if s3Select.statement.LimitReached() {
					if !sendRecord() {
						break
					}
					if err = writer.Finish(s3Select.getProgress()); err != nil {
						// FIXME: log this error.
						err = nil
					}
					return
				}

				if len(outputQueue) < cap(outputQueue) {
					continue
				}

				if !sendRecord() {
					break OuterLoop
				}
			}
		}
	}

	if err != nil {
		_ = writer.FinishWithError("InternalError", err.Error())
	}
}

// Close - closes opened S3 object.
func (s3Select *S3Select) Close() error {
	if s3Select.recordReader == nil {
		return nil
	}
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

//////////////////
// Helpers
/////////////////

// limitedReadCloser is like io.LimitedReader, but also implements io.Closer.
type limitedReadCloser struct {
	io.LimitedReader
	io.Closer
}

func newLimitedReadCloser(r io.ReadCloser, n int64) *limitedReadCloser {
	return &limitedReadCloser{
		LimitedReader: io.LimitedReader{R: r, N: n},
		Closer:        r,
	}
}

// ObjectSegmentReaderFn is a function that returns a reader for a contiguous
// suffix segment of an object starting at the given (non-negative) offset.
type ObjectSegmentReaderFn func(offset int64) (io.ReadCloser, error)

// ObjectReadSeekCloser implements ReadSeekCloser interface for reading objects.
// It uses a function that returns a io.ReadCloser for the object.
type ObjectReadSeekCloser struct {
	segmentReader ObjectSegmentReaderFn

	size   int64 // actual object size regardless of compression/encryption
	offset int64
	reader io.ReadCloser

	// reader can be closed idempotently multiple times
	closerOnce sync.Once
	// Error storing reader.Close()
	closerErr error
}

// NewObjectReadSeekCloser creates a new ObjectReadSeekCloser.
func NewObjectReadSeekCloser(segmentReader ObjectSegmentReaderFn, actualSize int64) *ObjectReadSeekCloser {
	return &ObjectReadSeekCloser{
		segmentReader: segmentReader,
		size:          actualSize,
		offset:        0,
		reader:        nil,
	}
}

// Seek call to implement io.Seeker
func (rsc *ObjectReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	// fmt.Printf("actual: %v offset: %v (%v) whence: %v\n", rsc.size, offset, rsc.offset, whence)
	switch whence {
	case io.SeekStart:
		rsc.offset = offset
	case io.SeekCurrent:
		rsc.offset += offset
	case io.SeekEnd:
		rsc.offset = rsc.size + offset
	}
	if rsc.offset < 0 {
		return rsc.offset, errors.New("seek to invalid negative offset")
	}
	if rsc.offset >= rsc.size {
		return rsc.offset, errors.New("seek past end of object")
	}
	if rsc.reader != nil {
		_ = rsc.reader.Close()
		rsc.reader = nil
	}
	return rsc.offset, nil
}

// Read call to implement io.Reader
func (rsc *ObjectReadSeekCloser) Read(p []byte) (n int, err error) {
	if rsc.reader == nil {
		rsc.reader, err = rsc.segmentReader(rsc.offset)
		if err != nil {
			return 0, err
		}
	}
	return rsc.reader.Read(p)
}

// Close call to implement io.Closer. Calling Read/Seek after Close reopens the
// object for reading and a subsequent Close call is required to ensure
// resources are freed.
func (rsc *ObjectReadSeekCloser) Close() error {
	rsc.closerOnce.Do(func() {
		if rsc.reader != nil {
			rsc.closerErr = rsc.reader.Close()
			rsc.reader = nil
		}
	})
	return rsc.closerErr
}
