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

package tagging

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/url"
)

// S3 API limits for tags
// Ref: https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
const (
	maxTags           = 10
	maxTagKeyLength   = 128
	maxTagValueLength = 256
)

// errors returned by tagging package
var (
	ErrTooManyTags     = Errorf("Cannot have more than 10 object tags")
	ErrInvalidTagKey   = Errorf("The TagKey you have provided is invalid")
	ErrInvalidTagValue = Errorf("The TagValue you have provided is invalid")
	ErrInvalidTag      = Errorf("Cannot provide multiple Tags with the same key")
)

// Tagging - object tagging interface
type Tagging struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Tagging"`
	TagSet  TagSet   `xml:"TagSet"`
}

// Validate - validates the tagging configuration
func (t Tagging) Validate() error {
	// Tagging can't have more than 10 tags
	if len(t.TagSet.Tags) > maxTags {
		return ErrTooManyTags
	}
	// Validate all the rules in the tagging config
	for _, ts := range t.TagSet.Tags {
		if t.TagSet.ContainsDuplicate(ts.Key) {
			return ErrInvalidTag
		}
		if err := ts.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// String - returns a string in format "tag1=value1&tag2=value2" with all the
// tags in this Tagging Struct
func (t Tagging) String() string {
	var buf bytes.Buffer
	for _, tag := range t.TagSet.Tags {
		if buf.Len() > 0 {
			buf.WriteString("&")
		}
		buf.WriteString(tag.Key + "=")
		buf.WriteString(tag.Value)
	}
	return buf.String()
}

// FromString - returns a Tagging struct when given a string in format
// "tag1=value1&tag2=value2"
func FromString(tagStr string) (Tagging, error) {
	tags, err := url.ParseQuery(tagStr)
	if err != nil {
		return Tagging{}, err
	}
	var idx = 0
	parsedTags := make([]Tag, len(tags))
	for k := range tags {
		parsedTags[idx].Key = k
		parsedTags[idx].Value = tags.Get(k)
		idx++
	}
	return Tagging{
		TagSet: TagSet{
			Tags: parsedTags,
		},
	}, nil
}

// ParseTagging - parses incoming xml data in given reader
// into Tagging interface. After parsing, also validates the
// parsed fields based on S3 API constraints.
func ParseTagging(reader io.Reader) (*Tagging, error) {
	var t Tagging
	if err := xml.NewDecoder(reader).Decode(&t); err != nil {
		return nil, err
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return &t, nil
}
