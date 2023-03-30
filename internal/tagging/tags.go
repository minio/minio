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

package tagging

import (
	"encoding/json"
	"net/url"
	"strings"

	xxml "github.com/minio/xxml"
)

// Tagging contains encoded tags that can be serialized to xml
// (using xxml only) or JSON.
type Tagging string

// stringsCut slices s around the first instance of sep,
// returning the text before and after sep.
// The found result reports whether sep appears in s.
// If sep does not appear in s, cut returns s, "", false.
func stringsCut(s, sep string) (before, after string, found bool) {
	if i := strings.Index(s, sep); i >= 0 {
		return s[:i], s[i+len(sep):], true
	}
	return s, "", false
}

// MarshalXML encodes tags to XML data.
// Format corresponds to https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
func (tags Tagging) MarshalXML(e *xxml.Encoder, start xxml.StartElement) (err error) {
	// Tag denotes key and value.
	type TagSet struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}

	tagName := xxml.Name{Space: "", Local: "Tag"}
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	tgs := string(tags)
	for tgs != "" {
		var key string
		key, tgs, _ = stringsCut(tgs, "&")
		if key == "" {
			continue
		}
		key, value, _ := stringsCut(key, "=")
		key, err := url.QueryUnescape(key)
		if err != nil {
			return err
		}

		value, err = url.QueryUnescape(value)
		if err != nil {
			return err
		}

		// tagList.Tags = append(tagList.Tags, tag{key, value})
		err = e.EncodeElement(TagSet{key, value}, xxml.StartElement{Name: tagName})
		if err != nil {
			return err
		}
	}

	return e.EncodeToken(start.End())
}

// MarshalJSON returns a JSON representation of the tags.
func (tags Tagging) MarshalJSON() ([]byte, error) {
	m, err := tags.Map()
	if err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

// Map returns a map representation of the tags.
func (tags Tagging) Map() (map[string]string, error) {
	if len(tags) == 0 {
		return map[string]string{}, nil
	}
	tgs := string(tags)
	guess := strings.Count(tgs, "=")
	res := make(map[string]string, guess)
	for tgs != "" {
		var key string
		key, tgs, _ = stringsCut(tgs, "&")
		if key == "" {
			continue
		}
		key, value, _ := stringsCut(key, "=")
		key, err := url.QueryUnescape(key)
		if err != nil {
			return nil, err
		}

		value, err = url.QueryUnescape(value)
		if err != nil {
			return nil, err
		}
		res[key] = value
	}
	return res, nil
}
