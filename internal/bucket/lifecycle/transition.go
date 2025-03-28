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

package lifecycle

import (
	"encoding/xml"
	"time"
)

var (
	errTransitionInvalidDays     = Errorf("Days must be 0 or greater when used with Transition")
	errTransitionInvalidDate     = Errorf("Date must be provided in ISO 8601 format")
	errTransitionInvalid         = Errorf("Exactly one of Days (0 or greater) or Date (positive ISO 8601 format) should be present in Transition.")
	errTransitionDateNotMidnight = Errorf("'Date' must be at midnight GMT")
)

// TransitionDate is a embedded type containing time.Time to unmarshal
// Date in Transition
type TransitionDate struct {
	time.Time
}

// UnmarshalXML parses date from Transition and validates date format
func (tDate *TransitionDate) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var dateStr string
	err := d.DecodeElement(&dateStr, &startElement)
	if err != nil {
		return err
	}
	// While AWS documentation mentions that the date specified
	// must be present in ISO 8601 format, in reality they allow
	// users to provide RFC 3339 compliant dates.
	trnDate, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return errTransitionInvalidDate
	}
	// Allow only date timestamp specifying midnight GMT
	hr, m, sec := trnDate.Clock()
	nsec := trnDate.Nanosecond()
	loc := trnDate.Location()
	if hr != 0 || m != 0 || sec != 0 || nsec != 0 || loc.String() != time.UTC.String() {
		return errTransitionDateNotMidnight
	}

	*tDate = TransitionDate{trnDate}
	return nil
}

// MarshalXML encodes expiration date if it is non-zero and encodes
// empty string otherwise
func (tDate TransitionDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if tDate.IsZero() {
		return nil
	}
	return e.EncodeElement(tDate.Format(time.RFC3339), startElement)
}

// TransitionDays is a type alias to unmarshal Days in Transition
type TransitionDays int

// UnmarshalXML parses number of days from Transition and validates if
// >= 0
func (tDays *TransitionDays) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var days int
	err := d.DecodeElement(&days, &startElement)
	if err != nil {
		return err
	}

	if days < 0 {
		return errTransitionInvalidDays
	}
	*tDays = TransitionDays(days)

	return nil
}

// MarshalXML encodes number of days to expire if it is non-zero and
// encodes empty string otherwise
func (tDays TransitionDays) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	return e.EncodeElement(int(tDays), startElement)
}

// Transition - transition actions for a rule in lifecycle configuration.
type Transition struct {
	XMLName      xml.Name       `xml:"Transition"`
	Days         TransitionDays `xml:"Days,omitempty"`
	Date         TransitionDate `xml:"Date,omitempty"`
	StorageClass string         `xml:"StorageClass,omitempty"`

	set bool
}

// IsEnabled returns if transition is enabled.
func (t Transition) IsEnabled() bool {
	return t.set
}

// MarshalXML encodes transition field into an XML form.
func (t Transition) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if !t.set {
		return nil
	}
	type transitionWrapper Transition
	return enc.EncodeElement(transitionWrapper(t), start)
}

// UnmarshalXML decodes transition field from the XML form.
func (t *Transition) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	type transitionWrapper Transition
	var trw transitionWrapper
	err := d.DecodeElement(&trw, &startElement)
	if err != nil {
		return err
	}
	*t = Transition(trw)
	t.set = true
	return nil
}

// Validate - validates the "Transition" element
func (t Transition) Validate() error {
	if !t.set {
		return nil
	}

	if !t.IsDateNull() && t.Days > 0 {
		return errTransitionInvalid
	}

	if t.StorageClass == "" {
		return errXMLNotWellFormed
	}
	return nil
}

// IsDateNull returns true if date field is null
func (t Transition) IsDateNull() bool {
	return t.Date.IsZero()
}

// IsNull returns true if both date and days fields are null
func (t Transition) IsNull() bool {
	return t.StorageClass == ""
}

// NextDue returns upcoming transition date for obj and true if applicable,
// returns false otherwise.
func (t Transition) NextDue(obj ObjectOpts) (time.Time, bool) {
	if !obj.IsLatest || t.IsNull() {
		return time.Time{}, false
	}

	if !t.IsDateNull() {
		return t.Date.Time, true
	}

	// Days == 0 indicates immediate tiering, i.e object is eligible for tiering since its creation.
	if t.Days == 0 {
		return obj.ModTime, true
	}
	return ExpectedExpiryTime(obj.ModTime, int(t.Days)), true
}
