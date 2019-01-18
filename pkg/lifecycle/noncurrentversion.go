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

// NoncurrentVersionExpiration - an action for lifecycle configuration rule.
type NoncurrentVersionExpiration struct {
	XMLName        xml.Name `xml:"NoncurrentVersionExpiration"`
	NoncurrentDays int      `xml:"NoncurrentDays,omitempty"`
}

// NoncurrentVersionTransition - an action for lifecycle configuration rule.
type NoncurrentVersionTransition struct {
	NoncurrentDays int    `xml:"NoncurrentDays"`
	StorageClass   string `xml:"StorageClass"`
}

// Validate - validates the NoncurrentVersionExpiration element
func (n NoncurrentVersionExpiration) Validate() error {
	if n.NoncurrentDays > 0 {
		return nil
	}
	return fmt.Errorf("NoncurrentDays must be positive integer when used with NoncurrentVersionExpiration")
}

// Validate - validates the NoncurrentVersionTransition element
func (n NoncurrentVersionTransition) Validate() error {
	if n.NoncurrentDays < 0 {
		return fmt.Errorf("NoncurrentDays must be non-negative integer when used with NoncurrentVersionTransition")
	}
	if err := isStorageClassValid(n.StorageClass); err != nil {
		return err
	}
	return nil
}
