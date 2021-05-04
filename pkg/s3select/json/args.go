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

package json

import (
	"encoding/xml"
	"fmt"
	"strings"
)

const (
	document = "document"
	lines    = "lines"

	defaultRecordDelimiter = "\n"
)

// ReaderArgs - represents elements inside <InputSerialization><JSON/> in request XML.
type ReaderArgs struct {
	ContentType string `xml:"Type"`
	unmarshaled bool
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

	parsedArgs.ContentType = strings.ToLower(parsedArgs.ContentType)
	switch parsedArgs.ContentType {
	case document, lines:
	default:
		return errInvalidJSONType(fmt.Errorf("invalid ContentType '%v'", parsedArgs.ContentType))
	}

	*args = ReaderArgs(parsedArgs)
	args.unmarshaled = true
	return nil
}

// WriterArgs - represents elements inside <OutputSerialization><JSON/> in request XML.
type WriterArgs struct {
	RecordDelimiter string `xml:"RecordDelimiter"`
	unmarshaled     bool
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

	switch len(parsedArgs.RecordDelimiter) {
	case 0:
		parsedArgs.RecordDelimiter = defaultRecordDelimiter
	case 1, 2:
	default:
		return fmt.Errorf("invalid RecordDelimiter '%v'", parsedArgs.RecordDelimiter)
	}

	*args = WriterArgs(parsedArgs)
	args.unmarshaled = true
	return nil
}
