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
	"encoding/xml"
	"strings"
)

// ARN - SQS resource name representation.
type ARN struct {
	TargetID
	region string
}

// String - returns string representation.
func (arn ARN) String() string {
	if arn.TargetID.ID == "" && arn.TargetID.Name == "" && arn.region == "" {
		return ""
	}

	return "arn:minio:sqs:" + arn.region + ":" + arn.TargetID.String()
}

// MarshalXML - encodes to XML data.
func (arn ARN) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return e.EncodeElement(arn.String(), start)
}

// UnmarshalXML - decodes XML data.
func (arn *ARN) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}

	parsedARN, err := parseARN(s)
	if err != nil {
		return err
	}

	*arn = *parsedARN
	return nil
}

// parseARN - parses string to ARN.
func parseARN(s string) (*ARN, error) {
	// ARN must be in the format of arn:minio:sqs:<REGION>:<ID>:<TYPE>
	if !strings.HasPrefix(s, "arn:minio:sqs:") {
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
