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

// Transition - transition actions for a rule in lifecycle configuration.
type Transition struct {
	XMLName      xml.Name `xml:"Transition"`
	Days         int      `xml:"Days,omitempty"`
	Date         string   `xml:"Date,omitempty"`
	StorageClass string   `xml:"StorageClass"`
}

func (t Transition) isDaysValid() error {
	if t.Days < 0 {
		return fmt.Errorf("Days must be non-negative integer when used with Transition")
	}
	return nil
}

func (t Transition) isAtleastOneActionPresent() error {
	if t.Date == "" && t.Days == 0 {
		return fmt.Errorf("At least one of Days or Date should be present inside Transition")
	}
	return nil
}

// Validate - validates the "Expiration" element
func (t Transition) Validate() error {
	if err := t.isAtleastOneActionPresent(); err != nil {
		return err
	}
	if err := t.isDaysValid(); err != nil {
		return err
	}
	if err := isDateValid(t.Date); err != nil {
		return err
	}
	if err := isStorageClassValid(t.StorageClass); err != nil {
		return err
	}
	return nil
}
