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

package lifecycle

import (
	"encoding/xml"
	"time"
)

var (
	errLifecycleInvalidDate       = Errorf("Date must be provided in ISO 8601 format")
	errLifecycleInvalidDays       = Errorf("Days must be positive integer when used with Expiration")
	errLifecycleInvalidExpiration = Errorf("At least one of Days or Date should be present inside Expiration")
	errLifecycleDateNotMidnight   = Errorf("'Date' must be at midnight GMT")
)

// ExpirationDays is non-zero number of days.
type ExpirationDays uint

// MarshalXML encodes to XML data.
func (days ExpirationDays) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if days == 0 {
		return errLifecycleInvalidDays
	}

	return e.EncodeElement(uint(days), start)
}

// UnmarshalXML decodes XML data.
func (days *ExpirationDays) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var ui uint
	if err := d.DecodeElement(&ui, &start); err != nil {
		return err
	}

	if ui == 0 {
		return errLifecycleInvalidDays
	}

	*days = ExpirationDays(ui)
	return nil
}

// ExpirationDate is a embedded type containing time.Time to unmarshal
// Date in Expiration
type ExpirationDate struct {
	time.Time
}

// MarshalXML encodes to XML data.
func (edate ExpirationDate) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if edate.IsZero() {
		return errLifecycleInvalidDate
	}

	expected := time.Date(edate.Year(), edate.Month(), edate.Day(), 0, 0, 0, 0, time.UTC)
	if !edate.Equal(expected) {
		return errLifecycleDateNotMidnight
	}

	return e.EncodeElement(edate.Format(time.RFC3339), start)
}

// UnmarshalXML decodes XML data.
func (edate *ExpirationDate) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}

	// While AWS documentation mentions that the date specified
	// must be present in ISO 8601 format, in reality they allow
	// users to provide RFC 3339 compliant dates.
	date, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return errLifecycleInvalidDate
	}

	expected := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	if !date.Equal(expected) {
		return errLifecycleDateNotMidnight
	}

	*edate = ExpirationDate{date}
	return nil
}

// Expiration - expiration actions for a rule in lifecycle configuration.
type Expiration struct {
	XMLName xml.Name        `xml:"Expiration"`
	Days    *ExpirationDays `xml:"Days,omitempty"`
	Date    *ExpirationDate `xml:"Date,omitempty"`
}

// MarshalXML encodes XML date.
func (ex Expiration) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if (ex.Days != nil) != (ex.Date != nil) { // ex.Days XOR ex.Date
		type subExpiration Expiration // sub-type to avoid recursively called MarshalXML()
		return e.EncodeElement(subExpiration(ex), start)
	}

	return errLifecycleInvalidExpiration
}

// UnmarshalXML decodes XML data.
func (ex *Expiration) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type subExpiration Expiration // sub-type to avoid recursively called UnmarshalXML()
	var se subExpiration
	if err := d.DecodeElement(&se, &start); err != nil {
		return err
	}

	if (se.Days != nil) != (se.Date != nil) { // se.Days XOR se.Date
		*ex = Expiration(se)
		return nil
	}

	return errLifecycleInvalidExpiration
}
