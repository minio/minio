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
	"fmt"
	"time"
)

const (
	// Reduced redundancy storage class
	reducedRedundancyStorageClass = "REDUCED_REDUNDANCY"
	// Standard storage class
	standardStorageClass = "STANDARD"
)

func isStorageClassValid(sc string) error {
	if sc != reducedRedundancyStorageClass && sc != standardStorageClass {
		return fmt.Errorf("StorageClass must be set to either %s or %s", standardStorageClass, reducedRedundancyStorageClass)
	}
	return nil
}

func isDateValid(date string) error {
	if _, err := time.Parse(iso8601Format, date); err != nil {
		return fmt.Errorf("Date must be provided in ISO 8601 format")
	}
	return nil
}
