// Copyright (c) 2015-2023 MinIO, Inc.
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
	"strings"
)

// ARN - SQS resource name representation.
type ARN struct {
	TargetID
	region string
}

// String - returns string representation.
func (arn ARN) String() string {
	if arn.ID == "" && arn.Name == "" && arn.region == "" {
		return ""
	}

	return "arn:minio:s3-object-lambda:" + arn.region + ":" + arn.TargetID.String()
}

// ParseARN - parses string to ARN.
func ParseARN(s string) (*ARN, error) {
	// ARN must be in the format of arn:minio:s3-object-lambda:<REGION>:<ID>:<TYPE>
	if !strings.HasPrefix(s, "arn:minio:s3-object-lambda:") {
		return nil, &ErrInvalidARN{s}
	}

	tokens := strings.Split(s, ":")
	if len(tokens) != 6 {
		return nil, &ErrInvalidARN{s}
	}

	if tokens[4] == "" || tokens[5] == "" {
		return nil, &ErrInvalidARN{s}
	}

	return &ARN{
		region: tokens[3],
		TargetID: TargetID{
			ID:   tokens[4],
			Name: tokens[5],
		},
	}, nil
}
