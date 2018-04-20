/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package net

import (
	"errors"
	"strconv"
)

// Port - network port
type Port uint16

// String - returns string representation of port.
func (p Port) String() string {
	return strconv.Itoa(int(p))
}

// ParsePort - parses string into Port
func ParsePort(s string) (p Port, err error) {
	var i int
	if i, err = strconv.Atoi(s); err != nil {
		return p, errors.New("invalid port number")
	}

	if i < 0 || i > 65535 {
		return p, errors.New("port must be between 0 to 65535")
	}

	return Port(i), nil
}
