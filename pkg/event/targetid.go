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
