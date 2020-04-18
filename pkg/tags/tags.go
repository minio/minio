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

package tags

import (
	"encoding/xml"
	"io"
	"net/url"
	"strings"
	"unicode/utf8"
)

// Error contains tag specific error.
type Error struct {
	code    string
	message string
}

// Code contains error code.
func (err Error) Code() string {
	return err.code
}

// Error contains error message.
func (err Error) Error() string {
	return err.message
}

var (
	errTooManyObjectTags = &Error{"BadRequest", "Tags cannot be more than 10"}
	errTooManyTags       = &Error{"BadRequest", "Tags cannot be more than 50"}
	errInvalidTagKey     = &Error{"InvalidTag", "The TagKey you have provided is invalid"}
	errInvalidTagValue   = &Error{"InvalidTag", "The TagValue you have provided is invalid"}
	errDuplicateTagKey   = &Error{"InvalidTag", "Cannot provide multiple Tags with the same key"}
)

// Tag comes with limitation as per
// https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html amd
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions
const (
	maxKeyLength      = 128
	maxValueLength    = 256
	maxObjectTagCount = 10
	maxTagCount       = 50
)

func checkKey(key string) error {
	if len(key) == 0 || utf8.RuneCountInString(key) > maxKeyLength || strings.Contains(key, "&") {
		return errInvalidTagKey
	}

	return nil
}

func checkValue(value string) error {
	if utf8.RuneCountInString(value) > maxValueLength || strings.Contains(value, "&") {
		return errInvalidTagValue
	}

	return nil
}

// Tag denotes key and value.
type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

func (t Tag) String() string {
	return t.Key + "=" + t.Value
}

// IsEmpty checks this tag for emptiness.
func (t Tag) IsEmpty() bool {
	return t.Key == ""
}

// Validate checks this tag has valid key and value.
func (t Tag) Validate() error {
	if err := checkKey(t.Key); err != nil {
		return err
	}

	return checkValue(t.Value)
}

// UnmarshalXML decodes XML data to tag.
func (t *Tag) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type subTag Tag // to avoid recursively calling UnmarshalXML()
	var st subTag
	if err := d.DecodeElement(&st, &start); err != nil {
		return err
	}

	if err := Tag(st).Validate(); err != nil {
		return err
	}

	*t = Tag(st)
	return nil
}

type tagSet struct {
	Tags []Tag `xml:"Tag"`
}

type tags struct {
	tagMap   map[string]string
	isObject bool
}

func (ts tags) String() string {
	s := []string{}
	for key, value := range ts.tagMap {
		s = append(s, key+"="+value)
	}

	return strings.Join(s, "&")
}

func (ts *tags) set(key, value string, failOnExist bool) error {
	if failOnExist {
		if _, found := ts.tagMap[key]; found {
			return errDuplicateTagKey
		}
	}

	if err := checkKey(key); err != nil {
		return err
	}

	if err := checkValue(value); err != nil {
		return err
	}

	if ts.isObject {
		if len(ts.tagMap) == maxObjectTagCount {
			return errTooManyObjectTags
		}
	} else if len(ts.tagMap) == maxTagCount {
		return errTooManyTags
	}

	ts.tagMap[key] = value
	return nil
}

func (ts tags) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	var tags tagSet
	for key, value := range ts.tagMap {
		tags.Tags = append(tags.Tags, Tag{key, value})
	}

	return e.EncodeElement(tags, start)
}

func (ts *tags) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var tags tagSet
	if err := d.DecodeElement(&tags, &start); err != nil {
		return err
	}

	if ts.isObject {
		if len(tags.Tags) > maxObjectTagCount {
			return errTooManyObjectTags
		}
	} else if len(tags.Tags) > maxTagCount {
		return errTooManyTags
	}

	m := map[string]string{}
	for _, tag := range tags.Tags {
		if _, found := m[tag.Key]; found {
			return errDuplicateTagKey
		}

		m[tag.Key] = tag.Value
	}

	ts.tagMap = m
	return nil
}

type tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	Tags    *tags    `xml:"TagSet"`
}

// Tags represents set of key/value pairs.
type Tags tagging

func (tags Tags) String() string {
	return tags.Tags.String()
}

// UnmarshalXML decodes XML data of tags in reader specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html#API_PutBucketTagging_RequestSyntax.
// If isObject is set, it validates for object tags.
func UnmarshalXML(reader io.Reader, isObject bool) (*Tags, error) {
	tags := &Tags{
		Tags: &tags{
			tagMap:   make(map[string]string),
			isObject: isObject,
		},
	}
	if err := xml.NewDecoder(reader).Decode(tags); err != nil {
		return nil, err
	}

	return tags, nil
}

// Parse decodes HTTP query formatted string into tags. A query formatted string is like "key1=value1&key2=value2".
func Parse(s string, isObject bool) (*Tags, error) {
	values, err := url.ParseQuery(s)
	if err != nil {
		return nil, err
	}

	tags := &Tags{
		Tags: &tags{
			tagMap:   make(map[string]string),
			isObject: isObject,
		},
	}

	for key := range values {
		if err := tags.Tags.set(key, values.Get(key), true); err != nil {
			return nil, err
		}
	}

	return tags, nil
}
