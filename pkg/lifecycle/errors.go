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

import "fmt"

// IsLifeCycleError - checks whether given error is lifecycle error or not.
func IsLifeCycleError(err error) bool {
	switch err.(type) {
	case ErrMalformedXML, *ErrMalformedXML:
		return true
	case ErrNotImplemented, *ErrNotImplemented:
		return true
	case ErrInvalidID, *ErrInvalidID:
		return true
	case ErrInvalidDays, *ErrInvalidDays:
		return true
	}

	return false
}

// ErrMalformedXML - invalid XML
type ErrMalformedXML struct {
}

func (err ErrMalformedXML) Error() string {
	return fmt.Sprintf("The XML you provided was not well-formed or did not validate against our published schema")
}

// ErrNotImplemented - invalid XML
type ErrNotImplemented struct {
}

func (err ErrNotImplemented) Error() string {
	return fmt.Sprintf("A header you provided implies functionality that is not implemented")
}

// ErrInvalidID - invalid ID.
type ErrInvalidID struct {
	ID string
}

func (err ErrInvalidID) Error() string {
	return fmt.Sprintf("invalid ID value '%v', must be less than 255 characters", err.ID)
}

// ErrInvalidDays - invalid number of days specified.
type ErrInvalidDays struct {
}

func (err ErrInvalidDays) Error() string {
	return fmt.Sprintf("'Days' for Expiration action must be a positive integer")
}
