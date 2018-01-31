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
