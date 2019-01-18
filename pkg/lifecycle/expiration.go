/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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
	"fmt"
)

const iso8601Format = "20060102T150405Z"

// Expiration - expiration actions for a rule in lifecycle configuration.
type Expiration struct {
	XMLName                   xml.Name `xml:"Expiration"`
	Days                      int      `xml:"Days,omitempty"`
	Date                      string   `xml:"Date,omitempty"`
	ExpiredObjectDeleteMarker bool     `xml:"ExpiredObjectDeleteMarker,omitempty"`
}

func (e Expiration) isDaysValid() error {
	if e.Days > 0 {
		return nil
	}
	return fmt.Errorf("Days must be positive integer when used with Expiration")
}

func (e Expiration) isAtleastOneActionPresent() error {
	if e.ExpiredObjectDeleteMarker == false && e.Date == "" && e.Days == 0 {
		return fmt.Errorf("At least one of Days, Date or ExpiredObjectDeleteMarker should be present inside Expiration")
	}
	return nil
}

// Validate - validates the "Expiration" element
func (e Expiration) Validate() error {
	if err := e.isDaysValid(); err != nil {
		return err
	}
	if err := isDateValid(e.Date); err != nil {
		return err
	}
	if err := e.isAtleastOneActionPresent(); err != nil {
		return err
	}
	return nil
}
