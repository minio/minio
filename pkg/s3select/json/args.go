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
