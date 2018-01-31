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

package event

import (
	"encoding/json"
	"fmt"
	"strings"
)

// TargetID - holds identification and name strings of notification target.
type TargetID struct {
	ID   string
	Name string
}

// String - returns string representation.
func (tid TargetID) String() string {
	return tid.ID + ":" + tid.Name
}

// ToARN - converts to ARN.
func (tid TargetID) ToARN(region string) ARN {
	return ARN{TargetID: tid, region: region}
}

// MarshalJSON - encodes to JSON data.
func (tid TargetID) MarshalJSON() ([]byte, error) {
	return json.Marshal(tid.String())
}

// UnmarshalJSON - decodes JSON data.
func (tid *TargetID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	targetID, err := parseTargetID(s)
	if err != nil {
		return err
	}

	*tid = *targetID
	return nil
}

// parseTargetID - parses string to TargetID.
func parseTargetID(s string) (*TargetID, error) {
	tokens := strings.Split(s, ":")
	if len(tokens) != 2 {
		return nil, fmt.Errorf("invalid TargetID format '%v'", s)
	}

	return &TargetID{
		ID:   tokens[0],
		Name: tokens[1],
	}, nil
}
