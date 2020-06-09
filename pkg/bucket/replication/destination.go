/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package replication

import (
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/minio/minio/pkg/wildcard"
)

// DestinationARNPrefix - destination ARN prefix as per AWS S3 specification.
const DestinationARNPrefix = "arn:aws:s3:::"

// Destination - destination in ReplicationConfiguration.
type Destination struct {
	XMLName      xml.Name `xml:"Destination" json:"Destination"`
	Bucket       string   `xml:"Bucket" json:"Bucket"`
	StorageClass string   `xml:"StorageClass" json:"StorageClass"`
	//EncryptionConfiguration TODO: not needed for MinIO
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
	return DestinationARNPrefix + d.Bucket
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
			return fmt.Errorf("unknown storage class %v", dest.StorageClass)
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
	if !strings.HasPrefix(s, DestinationARNPrefix) {
		return Destination{}, Errorf("invalid destination '%v'", s)
	}

	bucketName := strings.TrimPrefix(s, DestinationARNPrefix)

	return Destination{
		Bucket: bucketName,
	}, nil
}
