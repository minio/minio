// Copyright (c) 2024 MinIO, Inc.
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

package lifecycle

import (
	"encoding/xml"
	"time"
)

var errInvalidDaysDeletedObjExpiration = Errorf("Days must be a positive integer with DeletedObjectExpiration")

// DeletedObjectExpiration used to xml encode/decode ILM action by the same name
type DeletedObjectExpiration struct {
	XMLName xml.Name `xml:"DeletedObjectExpiration"`
	Days    int      `xml:"Days,omitempty"`
}

// Empty returns if a DeletedObjectExpiration XML element is empty.
// Used to detect if lifecycle.Rule contained a DeletedObjectExpiration element.
func (de DeletedObjectExpiration) Empty() bool {
	return de.Days == 0
}

// UnmarshalXML decodes a single XML element into a DeletedObjectExpiration value
func (de *DeletedObjectExpiration) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	type delMarkerExpiration DeletedObjectExpiration
	var dexp delMarkerExpiration
	err := dec.DecodeElement(&dexp, &start)
	if err != nil {
		return err
	}

	if dexp.Days <= 0 {
		return errInvalidDaysDeletedObjExpiration
	}

	*de = DeletedObjectExpiration(dexp)
	return nil
}

// MarshalXML encodes a DeletedObjectExpiration value into an XML element
func (de DeletedObjectExpiration) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if de.Empty() {
		return nil
	}

	type delMarkerExpiration DeletedObjectExpiration
	return enc.EncodeElement(delMarkerExpiration(de), start)
}

// NextDue returns upcoming DeletedObjectExpiration date for obj if
// applicable, returns false otherwise.
func (de DeletedObjectExpiration) NextDue(obj ObjectOpts) (time.Time, bool) {
	if !obj.IsLatest || !obj.DeleteMarker {
		return time.Time{}, false
	}

	return ExpectedExpiryTime(obj.ModTime, de.Days), true
}
