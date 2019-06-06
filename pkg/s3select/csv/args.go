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

package csv

import (
	"encoding/xml"
	"fmt"
	"strings"
)

const (
	none   = "none"
	use    = "use"
	ignore = "ignore"

	defaultRecordDelimiter      = "\n"
	defaultFieldDelimiter       = ","
	defaultQuoteCharacter       = `"`
	defaultQuoteEscapeCharacter = `"`
	defaultCommentCharacter     = "#"

	always   = "always"
	asneeded = "asneeded"
)

// ReaderArgs - represents elements inside <InputSerialization><CSV> in request XML.
type ReaderArgs struct {
	FileHeaderInfo             string `xml:"FileHeaderInfo"`
	RecordDelimiter            string `xml:"RecordDelimiter"`
	FieldDelimiter             string `xml:"FieldDelimiter"`
	QuoteCharacter             string `xml:"QuoteCharacter"`
	QuoteEscapeCharacter       string `xml:"QuoteEscapeCharacter"`
	CommentCharacter           string `xml:"Comments"`
	AllowQuotedRecordDelimiter bool   `xml:"AllowQuotedRecordDelimiter"`
	unmarshaled                bool
}

// IsEmpty - returns whether reader args is empty or not.
func (args *ReaderArgs) IsEmpty() bool {
	return !args.unmarshaled
}

// UnmarshalXML - decodes XML data.
func (args *ReaderArgs) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type subReaderArgs ReaderArgs
	parsedArgs := subReaderArgs{}
	if err := d.DecodeElement(&parsedArgs, &start); err != nil {
		return err
	}

	parsedArgs.FileHeaderInfo = strings.ToLower(parsedArgs.FileHeaderInfo)
	switch parsedArgs.FileHeaderInfo {
	case "":
		parsedArgs.FileHeaderInfo = none
	case none, use, ignore:
	default:
		return errInvalidFileHeaderInfo(fmt.Errorf("invalid FileHeaderInfo '%v'", parsedArgs.FileHeaderInfo))
	}

	switch len(parsedArgs.RecordDelimiter) {
	case 0:
		parsedArgs.RecordDelimiter = defaultRecordDelimiter
	case 1, 2:
	default:
		return fmt.Errorf("invalid RecordDelimiter '%v'", parsedArgs.RecordDelimiter)
	}

	switch len(parsedArgs.FieldDelimiter) {
	case 0:
		parsedArgs.FieldDelimiter = defaultFieldDelimiter
	case 1:
	default:
		return fmt.Errorf("invalid FieldDelimiter '%v'", parsedArgs.FieldDelimiter)
	}

	switch parsedArgs.QuoteCharacter {
	case "":
		parsedArgs.QuoteCharacter = defaultQuoteCharacter
	case defaultQuoteCharacter:
	default:
		return fmt.Errorf("unsupported QuoteCharacter '%v'", parsedArgs.QuoteCharacter)
	}

	switch parsedArgs.QuoteEscapeCharacter {
	case "":
		parsedArgs.QuoteEscapeCharacter = defaultQuoteEscapeCharacter
	case defaultQuoteEscapeCharacter:
	default:
		return fmt.Errorf("unsupported QuoteEscapeCharacter '%v'", parsedArgs.QuoteEscapeCharacter)
	}

	switch parsedArgs.CommentCharacter {
	case "":
		parsedArgs.CommentCharacter = defaultCommentCharacter
	case defaultCommentCharacter:
	default:
		return fmt.Errorf("unsupported Comments '%v'", parsedArgs.CommentCharacter)
	}

	if parsedArgs.AllowQuotedRecordDelimiter {
		return fmt.Errorf("flag AllowQuotedRecordDelimiter is unsupported at the moment")
	}

	*args = ReaderArgs(parsedArgs)
	args.unmarshaled = true
	return nil
}

// WriterArgs - represents elements inside <OutputSerialization><CSV/> in request XML.
type WriterArgs struct {
	QuoteFields          string `xml:"QuoteFields"`
	RecordDelimiter      string `xml:"RecordDelimiter"`
	FieldDelimiter       string `xml:"FieldDelimiter"`
	QuoteCharacter       string `xml:"QuoteCharacter"`
	QuoteEscapeCharacter string `xml:"QuoteEscapeCharacter"`
	unmarshaled          bool
}

// IsEmpty - returns whether writer args is empty or not.
func (args *WriterArgs) IsEmpty() bool {
	return !args.unmarshaled
}

// UnmarshalXML - decodes XML data.
func (args *WriterArgs) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type subWriterArgs WriterArgs
	parsedArgs := subWriterArgs{}
	if err := d.DecodeElement(&parsedArgs, &start); err != nil {
		return err
	}

	parsedArgs.QuoteFields = strings.ToLower(parsedArgs.QuoteFields)
	switch parsedArgs.QuoteFields {
	case "":
		parsedArgs.QuoteFields = asneeded
	case always, asneeded:
	default:
		return errInvalidQuoteFields(fmt.Errorf("invalid QuoteFields '%v'", parsedArgs.QuoteFields))
	}

	switch len(parsedArgs.RecordDelimiter) {
	case 0:
		parsedArgs.RecordDelimiter = defaultRecordDelimiter
	case 1, 2:
	default:
		return fmt.Errorf("invalid RecordDelimiter '%v'", parsedArgs.RecordDelimiter)
	}

	switch len(parsedArgs.FieldDelimiter) {
	case 0:
		parsedArgs.FieldDelimiter = defaultFieldDelimiter
	case 1:
	default:
		return fmt.Errorf("invalid FieldDelimiter '%v'", parsedArgs.FieldDelimiter)
	}

	switch parsedArgs.QuoteCharacter {
	case "":
		parsedArgs.QuoteCharacter = defaultQuoteCharacter
	case defaultQuoteCharacter:
	default:
		return fmt.Errorf("unsupported QuoteCharacter '%v'", parsedArgs.QuoteCharacter)
	}

	switch parsedArgs.QuoteEscapeCharacter {
	case "":
		parsedArgs.QuoteEscapeCharacter = defaultQuoteEscapeCharacter
	case defaultQuoteEscapeCharacter:
	default:
		return fmt.Errorf("unsupported QuoteEscapeCharacter '%v'", parsedArgs.QuoteEscapeCharacter)
	}

	*args = WriterArgs(parsedArgs)
	args.unmarshaled = true
	return nil
}
