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

package replication

import (
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/minio/pkg/v3/wildcard"
)

// DestinationARNPrefix - destination ARN prefix as per AWS S3 specification.
const DestinationARNPrefix = "arn:aws:s3:::"

// DestinationARNMinIOPrefix - destination ARN prefix for MinIO.
const DestinationARNMinIOPrefix = "arn:minio:replication:"

// Destination - destination in ReplicationConfiguration.
type Destination struct {
	XMLName      xml.Name `xml:"Destination" json:"Destination"`
	Bucket       string   `xml:"Bucket" json:"Bucket"`
	StorageClass string   `xml:"StorageClass" json:"StorageClass"`
	ARN          string
	// EncryptionConfiguration TODO: not needed for MinIO
}

func (d Destination) isValidStorageClass() bool {
	if d.StorageClass == "" {
		return true
	}
	return d.StorageClass == "STANDARD" || d.StorageClass == "REDUCED_REDUNDANCY"
}

// IsValid - checks whether Destination is valid or not.
func (d Destination) IsValid() bool {
	return d.Bucket != "" || !d.isValidStorageClass()
}

func (d Destination) String() string {
	return d.ARN
}

// LegacyArn returns true if arn format has prefix "arn:aws:s3:::" which was
// used prior to multi-destination
func (d Destination) LegacyArn() bool {
	return strings.HasPrefix(d.ARN, DestinationARNPrefix)
}

// TargetArn returns true if arn format has prefix  "arn:minio:replication:::"
// used for multi-destination targets
func (d Destination) TargetArn() bool {
	return strings.HasPrefix(d.ARN, DestinationARNMinIOPrefix)
}

// MarshalXML - encodes to XML data.
func (d Destination) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	if err := e.EncodeElement(d.String(), xml.StartElement{Name: xml.Name{Local: "Bucket"}}); err != nil {
		return err
	}
	if d.StorageClass != "" {
		if err := e.EncodeElement(d.StorageClass, xml.StartElement{Name: xml.Name{Local: "StorageClass"}}); err != nil {
			return err
		}
	}
	return e.EncodeToken(xml.EndElement{Name: start.Name})
}

// UnmarshalXML - decodes XML data.
func (d *Destination) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) (err error) {
	// Make subtype to avoid recursive UnmarshalXML().
	type destination Destination
	dest := destination{}

	if err := dec.DecodeElement(&dest, &start); err != nil {
		return err
	}
	parsedDest, err := parseDestination(dest.Bucket)
	if err != nil {
		return err
	}
	if dest.StorageClass != "" {
		switch dest.StorageClass {
		case "STANDARD", "REDUCED_REDUNDANCY":
		default:
			return fmt.Errorf("unknown storage class %s", dest.StorageClass)
		}
	}
	parsedDest.StorageClass = dest.StorageClass
	*d = parsedDest
	return nil
}

// Validate - validates Resource is for given bucket or not.
func (d Destination) Validate(bucketName string) error {
	if !d.IsValid() {
		return Errorf("invalid destination")
	}

	if !wildcard.Match(d.Bucket, bucketName) {
		return Errorf("bucket name does not match")
	}
	return nil
}

// parseDestination - parses string to Destination.
func parseDestination(s string) (Destination, error) {
	if !strings.HasPrefix(s, DestinationARNPrefix) && !strings.HasPrefix(s, DestinationARNMinIOPrefix) {
		return Destination{}, Errorf("invalid destination '%s'", s)
	}

	bucketName := strings.TrimPrefix(s, DestinationARNPrefix)

	return Destination{
		Bucket: bucketName,
		ARN:    s,
	}, nil
}
