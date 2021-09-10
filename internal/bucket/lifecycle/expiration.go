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
	errLifecycleInvalidDate         = Errorf("Date must be provided in ISO 8601 format")
	errLifecycleInvalidDays         = Errorf("Days must be positive integer when used with Expiration")
	errLifecycleInvalidExpiration   = Errorf("Exactly one of Days (positive integer) or Date (positive ISO 8601 format) should be present inside Expiration.")
	errLifecycleInvalidDeleteMarker = Errorf("Delete marker cannot be specified with Days or Date in a Lifecycle Expiration Policy")
	errLifecycleDateNotMidnight     = Errorf("'Date' must be at midnight GMT")
)

// ExpirationDays is a type alias to unmarshal Days in Expiration
type ExpirationDays int

// UnmarshalXML parses number of days from Expiration and validates if
// greater than zero
func (eDays *ExpirationDays) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var numDays int
	err := d.DecodeElement(&numDays, &startElement)
	if err != nil {
		return err
	}
	if numDays <= 0 {
		return errLifecycleInvalidDays
	}
	*eDays = ExpirationDays(numDays)
	return nil
}

// MarshalXML encodes number of days to expire if it is non-zero and
// encodes empty string otherwise
func (eDays ExpirationDays) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if eDays == 0 {
		return nil
	}
	return e.EncodeElement(int(eDays), startElement)
}

// ExpirationDate is a embedded type containing time.Time to unmarshal
// Date in Expiration
type ExpirationDate struct {
	time.Time
}

// UnmarshalXML parses date from Expiration and validates date format
func (eDate *ExpirationDate) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var dateStr string
	err := d.DecodeElement(&dateStr, &startElement)
	if err != nil {
		return err
	}
	// While AWS documentation mentions that the date specified
	// must be present in ISO 8601 format, in reality they allow
	// users to provide RFC 3339 compliant dates.
	expDate, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return errLifecycleInvalidDate
	}
	// Allow only date timestamp specifying midnight GMT
	hr, min, sec := expDate.Clock()
	nsec := expDate.Nanosecond()
	loc := expDate.Location()
	if !(hr == 0 && min == 0 && sec == 0 && nsec == 0 && loc.String() == time.UTC.String()) {
		return errLifecycleDateNotMidnight
	}

	*eDate = ExpirationDate{expDate}
	return nil
}

// MarshalXML encodes expiration date if it is non-zero and encodes
// empty string otherwise
func (eDate ExpirationDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if eDate.Time.IsZero() {
		return nil
	}
	return e.EncodeElement(eDate.Format(time.RFC3339), startElement)
}

// ExpireDeleteMarker represents value of ExpiredObjectDeleteMarker field in Expiration XML element.
type ExpireDeleteMarker struct {
	val bool
	set bool
}

// Expiration - expiration actions for a rule in lifecycle configuration.
type Expiration struct {
	XMLName      xml.Name           `xml:"Expiration"`
	Days         ExpirationDays     `xml:"Days,omitempty"`
	Date         ExpirationDate     `xml:"Date,omitempty"`
	DeleteMarker ExpireDeleteMarker `xml:"ExpiredObjectDeleteMarker"`

	set bool
}

// MarshalXML encodes delete marker boolean into an XML form.
func (b ExpireDeleteMarker) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if !b.set {
		return nil
	}
	return e.EncodeElement(b.val, startElement)
}

// UnmarshalXML decodes delete marker boolean from the XML form.
func (b *ExpireDeleteMarker) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var exp bool
	err := d.DecodeElement(&exp, &startElement)
	if err != nil {
		return err
	}
	b.val = exp
	b.set = true
	return nil
}

// MarshalXML encodes expiration field into an XML form.
func (e Expiration) MarshalXML(enc *xml.Encoder, startElement xml.StartElement) error {
	if !e.set {
		return nil
	}
	type expirationWrapper Expiration
	return enc.EncodeElement(expirationWrapper(e), startElement)
}

// UnmarshalXML decodes expiration field from the XML form.
func (e *Expiration) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	type expirationWrapper Expiration
	var exp expirationWrapper
	err := d.DecodeElement(&exp, &startElement)
	if err != nil {
		return err
	}
	*e = Expiration(exp)
	e.set = true
	return nil
}

// Validate - validates the "Expiration" element
func (e Expiration) Validate() error {
	if !e.set {
		return nil
	}

	// DeleteMarker cannot be specified if date or dates are specified.
	if (!e.IsDaysNull() || !e.IsDateNull()) && e.DeleteMarker.set {
		return errLifecycleInvalidDeleteMarker
	}

	if !e.DeleteMarker.set && e.IsDaysNull() && e.IsDateNull() {
		return errXMLNotWellFormed
	}

	// Both expiration days and date are specified
	if !e.IsDaysNull() && !e.IsDateNull() {
		return errLifecycleInvalidExpiration
	}

	return nil
}

// IsDaysNull returns true if days field is null
func (e Expiration) IsDaysNull() bool {
	return e.Days == ExpirationDays(0)
}

// IsDateNull returns true if date field is null
func (e Expiration) IsDateNull() bool {
	return e.Date.Time.IsZero()
}

// IsNull returns true if both date and days fields are null
func (e Expiration) IsNull() bool {
	return e.IsDaysNull() && e.IsDateNull()
}
