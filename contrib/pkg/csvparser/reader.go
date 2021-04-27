// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in https://golang.org/LICENSE

// Package csv reads and writes comma-separated values (CSV) files.
// There are many kinds of CSV files; this package supports the format
// described in RFC 4180.
//
// A csv file contains zero or more records of one or more fields per record.
// Each record is separated by the newline character. The final record may
// optionally be followed by a newline character.
//
//	field1,field2,field3
//
// White space is considered part of a field.
//
// Carriage returns before newline characters are silently removed.
//
// Blank lines are ignored. A line with only whitespace characters (excluding
// the ending newline character) is not considered a blank line.
//
// Fields which start and stop with the quote character " are called
// quoted-fields. The beginning and ending quote are not part of the
// field.
//
// The source:
//
//	normal string,"quoted-field"
//
// results in the fields
//
//	{`normal string`, `quoted-field`}
//
// Within a quoted-field a quote character followed by a second quote
// character is considered a single quote.
//
//	"the ""word"" is true","a ""quoted-field"""
//
// results in
//
//	{`the "word" is true`, `a "quoted-field"`}
//
// Newlines and commas may be included in a quoted-field
//
//	"Multi-line
//	field","comma is ,"
//
// results in
//
//	{`Multi-line
//	field`, `comma is ,`}
//
// Modified to be used with MinIO. Main modifications include
// - Configurable 'quote' parameter
// - Performance improvements
//    benchmark                                            old ns/op     new ns/op     delta
//    BenchmarkRead-8                                      2807          2189          -22.02%
//    BenchmarkReadWithFieldsPerRecord-8                   2802          2179          -22.23%
//    BenchmarkReadWithoutFieldsPerRecord-8                2824          2181          -22.77%
//    BenchmarkReadLargeFields-8                           3584          3371          -5.94%
//    BenchmarkReadReuseRecord-8                           2044          1480          -27.59%
//    BenchmarkReadReuseRecordWithFieldsPerRecord-8        2056          1483          -27.87%
//    BenchmarkReadReuseRecordWithoutFieldsPerRecord-8     2047          1482          -27.60%
//    BenchmarkReadReuseRecordLargeFields-8                2777          2594          -6.59%
package csv

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

// A ParseError is returned for parsing errors.
// Line numbers are 1-indexed and columns are 0-indexed.
type ParseError struct {
	StartLine int   // Line where the record starts
	Line      int   // Line where the error occurred
	Column    int   // Column (rune index) where the error occurred
	Err       error // The actual error
}

func (e *ParseError) Error() string {
	if e.Err == ErrFieldCount {
		return fmt.Sprintf("record on line %d: %v", e.Line, e.Err)
	}
	if e.StartLine != e.Line {
		return fmt.Sprintf("record on line %d; parse error on line %d, column %d: %v", e.StartLine, e.Line, e.Column, e.Err)
	}
	return fmt.Sprintf("parse error on line %d, column %d: %v", e.Line, e.Column, e.Err)
}

// Unwrap returns the underlying error
func (e *ParseError) Unwrap() error { return e.Err }

// These are the errors that can be returned in ParseError.Err.
var (
	ErrTrailingComma = errors.New("extra delimiter at end of line") // Deprecated: No longer used.
	ErrBareQuote     = errors.New("bare \" in non-quoted-field")
	ErrQuote         = errors.New("extraneous or missing \" in quoted-field")
	ErrFieldCount    = errors.New("wrong number of fields")
)

var errInvalidDelim = errors.New("csv: invalid field or comment delimiter")

func validDelim(r rune) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

// A Reader reads records from a CSV-encoded file.
//
// As returned by NewReader, a Reader expects input conforming to RFC 4180.
// The exported fields can be changed to customize the details before the
// first call to Read or ReadAll.
//
// The Reader converts all \r\n sequences in its input to plain \n,
// including in multiline field values, so that the returned data does
// not depend on which line-ending convention an input file uses.
type Reader struct {
	// Comma is the field delimiter.
	// It is set to comma (',') by NewReader.
	// Comma must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	Comma rune

	// Quote is a single rune used for marking fields limits
	Quote []rune

	// QuoteEscape is a single rune to escape the quote character
	QuoteEscape rune

	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	// Comment must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	// It must also not be equal to Comma.
	Comment rune

	// FieldsPerRecord is the number of expected fields per record.
	// If FieldsPerRecord is positive, Read requires each record to
	// have the given number of fields. If FieldsPerRecord is 0, Read sets it to
	// the number of fields in the first record, so that future records must
	// have the same field count. If FieldsPerRecord is negative, no check is
	// made and records may have a variable number of fields.
	FieldsPerRecord int

	// If LazyQuotes is true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	LazyQuotes bool

	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	TrimLeadingSpace bool

	// ReuseRecord controls whether calls to Read may return a slice sharing
	// the backing array of the previous call's returned slice for performance.
	// By default, each call to Read returns newly allocated memory owned by the caller.
	ReuseRecord bool

	TrailingComma bool // Deprecated: No longer used.

	r *bufio.Reader

	// numLine is the current line being read in the CSV file.
	numLine int

	// rawBuffer is a line buffer only used by the readLine method.
	rawBuffer []byte

	// recordBuffer holds the unescaped fields, one after another.
	// The fields can be accessed by using the indexes in fieldIndexes.
	// E.g., For the row `a,"b","c""d",e`, recordBuffer will contain `abc"de`
	// and fieldIndexes will contain the indexes [1, 2, 5, 6].
	recordBuffer []byte

	// fieldIndexes is an index of fields inside recordBuffer.
	// The i'th field ends at offset fieldIndexes[i] in recordBuffer.
	fieldIndexes []int

	// lastRecord is a record cache and only used when ReuseRecord == true.
	lastRecord []string

	// Caching some values between Read() calls for performance gain
	cached               bool
	cachedQuoteEscapeLen int
	cachedQuoteLen       int
	cachedEncodedQuote   []byte
	cachedCommaLen       int
	cachedQuotes         string
}

// NewReader returns a new Reader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		Comma:       ',',
		Quote:       []rune(`"`),
		QuoteEscape: '"',
		r:           bufio.NewReader(r),
	}
}

// Read reads one record (a slice of fields) from r.
// If the record has an unexpected number of fields,
// Read returns the record along with the error ErrFieldCount.
// Except for that case, Read always returns either a non-nil
// record or a non-nil error, but not both.
// If there is no data left to be read, Read returns nil, io.EOF.
// If ReuseRecord is true, the returned slice may be shared
// between multiple calls to Read.
func (r *Reader) Read() (record []string, err error) {
	if r.ReuseRecord {
		record, err = r.readRecord(r.lastRecord)
		r.lastRecord = record
	} else {
		record, err = r.readRecord(nil)
	}
	return record, err
}

// ReadAll reads all the remaining records from r.
// Each record is a slice of fields.
// A successful call returns err == nil, not err == io.EOF. Because ReadAll is
// defined to read until EOF, it does not treat end of file as an error to be
// reported.
func (r *Reader) ReadAll() (records [][]string, err error) {
	for {
		record, err := r.readRecord(nil)
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

// readLine reads the next line (with the trailing endline).
// If EOF is hit without a trailing endline, it will be omitted.
// If some bytes were read, then the error is never io.EOF.
// The result is only valid until the next call to readLine.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		r.rawBuffer = append(r.rawBuffer[:0], line...)
		for err == bufio.ErrBufferFull {
			line, err = r.r.ReadSlice('\n')
			r.rawBuffer = append(r.rawBuffer, line...)
		}
		line = r.rawBuffer
	}
	if len(line) > 0 && err == io.EOF {
		err = nil
		// For backwards compatibility, drop trailing \r before EOF.
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	r.numLine++
	// Normalize \r\n to \n on all input lines.
	if n := len(line); n >= 2 && line[n-2] == '\r' && line[n-1] == '\n' {
		line[n-2] = '\n'
		line = line[:n-1]
	}
	return line, err
}

// lengthNL reports the number of bytes for the trailing \n.
func lengthNL(b []byte) int {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		return 1
	}
	return 0
}

// nextRune returns the next rune in b or utf8.RuneError.
func nextRune(b []byte) rune {
	r, _ := utf8.DecodeRune(b)
	return r
}

func encodeRune(r rune) []byte {
	rlen := utf8.RuneLen(r)
	p := make([]byte, rlen)
	_ = utf8.EncodeRune(p, r)
	return p
}

func (r *Reader) readRecord(dst []string) ([]string, error) {
	if r.Comma == r.Comment || !validDelim(r.Comma) || (r.Comment != 0 && !validDelim(r.Comment)) {
		return nil, errInvalidDelim
	}

	// Read line (automatically skipping past empty lines and any comments).
	var line, fullLine []byte
	var errRead error
	for errRead == nil {
		line, errRead = r.readLine()
		if r.Comment != 0 && nextRune(line) == r.Comment {
			line = nil
			continue // Skip comment lines
		}
		if errRead == nil && len(line) == lengthNL(line) {
			line = nil
			continue // Skip empty lines
		}
		fullLine = line
		break
	}
	if errRead == io.EOF {
		return nil, errRead
	}

	if !r.cached {
		r.cachedQuoteEscapeLen = utf8.RuneLen(r.QuoteEscape)
		if len(r.Quote) > 0 {
			r.cachedQuoteLen = utf8.RuneLen(r.Quote[0])
			r.cachedEncodedQuote = encodeRune(r.Quote[0])
			r.cachedQuotes += string(r.Quote[0])
		}
		r.cachedCommaLen = utf8.RuneLen(r.Comma)
		r.cachedQuotes += string(r.QuoteEscape)
		r.cached = true
	}

	// Parse each field in the record.
	var err error
	recLine := r.numLine // Starting line for record
	r.recordBuffer = r.recordBuffer[:0]
	r.fieldIndexes = r.fieldIndexes[:0]
parseField:
	for {
		if r.TrimLeadingSpace {
			line = bytes.TrimLeftFunc(line, unicode.IsSpace)
		}
		if len(line) == 0 || r.cachedQuoteLen == 0 || nextRune(line) != r.Quote[0] {
			// Non-quoted string field
			i := bytes.IndexRune(line, r.Comma)
			field := line
			if i >= 0 {
				field = field[:i]
			} else {
				field = field[:len(field)-lengthNL(field)]
			}
			// Check to make sure a quote does not appear in field.
			if !r.LazyQuotes {
				if j := bytes.IndexRune(field, r.Quote[0]); j >= 0 {
					col := utf8.RuneCount(fullLine[:len(fullLine)-len(line[j:])])
					err = &ParseError{StartLine: recLine, Line: r.numLine, Column: col, Err: ErrBareQuote}
					break parseField
				}
			}
			r.recordBuffer = append(r.recordBuffer, field...)
			r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
			if i >= 0 {
				line = line[i+r.cachedCommaLen:]
				continue parseField
			}
			break parseField
		} else {
			// Quoted string field
			line = line[r.cachedQuoteLen:]
			for {
				i := bytes.IndexAny(line, r.cachedQuotes)
				if i >= 0 {
					// Hit next quote or escape quote
					r.recordBuffer = append(r.recordBuffer, line[:i]...)

					escape := nextRune(line[i:]) == r.QuoteEscape
					if escape {
						line = line[i+r.cachedQuoteEscapeLen:]
					} else {
						line = line[i+r.cachedQuoteLen:]
					}

					switch rn := nextRune(line); {
					case escape && r.QuoteEscape != r.Quote[0]:
						r.recordBuffer = append(r.recordBuffer, encodeRune(rn)...)
						line = line[utf8.RuneLen(rn):]
					case rn == r.Quote[0]:
						// `""` sequence (append quote).
						r.recordBuffer = append(r.recordBuffer, r.cachedEncodedQuote...)
						line = line[r.cachedQuoteLen:]
					case rn == r.Comma:
						// `",` sequence (end of field).
						line = line[r.cachedCommaLen:]
						r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
						continue parseField
					case lengthNL(line) == len(line):
						// `"\n` sequence (end of line).
						r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
						break parseField
					case r.LazyQuotes:
						// `"` sequence (bare quote).
						r.recordBuffer = append(r.recordBuffer, r.cachedEncodedQuote...)
					default:
						// `"*` sequence (invalid non-escaped quote).
						col := utf8.RuneCount(fullLine[:len(fullLine)-len(line)-r.cachedQuoteLen])
						err = &ParseError{StartLine: recLine, Line: r.numLine, Column: col, Err: ErrQuote}
						break parseField
					}
				} else if len(line) > 0 {
					// Hit end of line (copy all data so far).
					r.recordBuffer = append(r.recordBuffer, line...)
					if errRead != nil {
						break parseField
					}
					line, errRead = r.readLine()
					if errRead == io.EOF {
						errRead = nil
					}
					fullLine = line
				} else {
					// Abrupt end of file (EOF or error).
					if !r.LazyQuotes && errRead == nil {
						col := utf8.RuneCount(fullLine)
						err = &ParseError{StartLine: recLine, Line: r.numLine, Column: col, Err: ErrQuote}
						break parseField
					}
					r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
					break parseField
				}
			}
		}
	}
	if err == nil {
		err = errRead
	}

	// Create a single string and create slices out of it.
	// This pins the memory of the fields together, but allocates once.
	str := string(r.recordBuffer) // Convert to string once to batch allocations
	dst = dst[:0]
	if cap(dst) < len(r.fieldIndexes) {
		dst = make([]string, len(r.fieldIndexes))
	}
	dst = dst[:len(r.fieldIndexes)]
	var preIdx int
	for i, idx := range r.fieldIndexes {
		dst[i] = str[preIdx:idx]
		preIdx = idx
	}

	// Check or update the expected fields per record.
	if r.FieldsPerRecord > 0 {
		if len(dst) != r.FieldsPerRecord && err == nil {
			err = &ParseError{StartLine: recLine, Line: recLine, Err: ErrFieldCount}
		}
	} else if r.FieldsPerRecord == 0 {
		r.FieldsPerRecord = len(dst)
	}
	return dst, err
}
