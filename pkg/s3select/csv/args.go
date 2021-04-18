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

package csv

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	none = "none"
	use  = "use"

	defaultRecordDelimiter      = "\n"
	defaultFieldDelimiter       = ","
	defaultQuoteCharacter       = `"`
	defaultQuoteEscapeCharacter = `"`
	defaultCommentCharacter     = "#"

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
func (args *ReaderArgs) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	args.FileHeaderInfo = none
	args.RecordDelimiter = defaultRecordDelimiter
	args.FieldDelimiter = defaultFieldDelimiter
	args.QuoteCharacter = defaultQuoteCharacter
	args.QuoteEscapeCharacter = defaultQuoteEscapeCharacter
	args.CommentCharacter = defaultCommentCharacter
	args.AllowQuotedRecordDelimiter = false

	for {
		// Read tokens from the XML document in a stream.
		t, err := d.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch se := t.(type) {
		case xml.StartElement:
			tagName := se.Name.Local
			switch tagName {
			case "AllowQuotedRecordDelimiter":
				var b bool
				if err = d.DecodeElement(&b, &se); err != nil {
					return err
				}
				args.AllowQuotedRecordDelimiter = b
			default:
				var s string
				if err = d.DecodeElement(&s, &se); err != nil {
					return err
				}
				switch tagName {
				case "FileHeaderInfo":
					args.FileHeaderInfo = strings.ToLower(s)
				case "RecordDelimiter":
					args.RecordDelimiter = s
				case "FieldDelimiter":
					args.FieldDelimiter = s
				case "QuoteCharacter":
					if utf8.RuneCountInString(s) > 1 {
						return fmt.Errorf("unsupported QuoteCharacter '%v'", s)
					}
					args.QuoteCharacter = s
				case "QuoteEscapeCharacter":
					switch utf8.RuneCountInString(s) {
					case 0:
						args.QuoteEscapeCharacter = defaultQuoteEscapeCharacter
					case 1:
						args.QuoteEscapeCharacter = s
					default:
						return fmt.Errorf("unsupported QuoteEscapeCharacter '%v'", s)
					}
				case "Comments":
					args.CommentCharacter = s
				default:
					return errors.New("unrecognized option")
				}
			}
		}
	}

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

	args.QuoteFields = asneeded
	args.RecordDelimiter = defaultRecordDelimiter
	args.FieldDelimiter = defaultFieldDelimiter
	args.QuoteCharacter = defaultQuoteCharacter
	args.QuoteEscapeCharacter = defaultQuoteCharacter

	for {
		// Read tokens from the XML document in a stream.
		t, err := d.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch se := t.(type) {
		case xml.StartElement:
			var s string
			if err = d.DecodeElement(&s, &se); err != nil {
				return err
			}
			switch se.Name.Local {
			case "QuoteFields":
				args.QuoteFields = strings.ToLower(s)
			case "RecordDelimiter":
				args.RecordDelimiter = s
			case "FieldDelimiter":
				args.FieldDelimiter = s
			case "QuoteCharacter":
				switch utf8.RuneCountInString(s) {
				case 0:
					args.QuoteCharacter = "\x00"
				case 1:
					args.QuoteCharacter = s
				default:
					return fmt.Errorf("unsupported QuoteCharacter '%v'", s)
				}
			case "QuoteEscapeCharacter":
				switch utf8.RuneCountInString(s) {
				case 0:
					args.QuoteEscapeCharacter = defaultQuoteEscapeCharacter
				case 1:
					args.QuoteEscapeCharacter = s
				default:
					return fmt.Errorf("unsupported QuoteCharacter '%v'", s)
				}
			default:
				return errors.New("unrecognized option")
			}
		}
	}

	args.unmarshaled = true
	return nil
}
