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

package net

import (
	"encoding/json"
	"errors"
	"net/url"
	"path"
)

// URL - improved JSON friendly url.URL.
type URL url.URL

// IsEmpty - checks URL is empty or not.
func (u URL) IsEmpty() bool {
	return u.String() == ""
}

// String - returns string representation of URL.
func (u URL) String() string {
	// if port number 80 and 443, remove for http and https scheme respectively
	if u.Host != "" {
		host, err := ParseHost(u.Host)
		if err != nil {
			panic(err)
		}
		switch {
		case u.Scheme == "http" && host.Port == 80:
			fallthrough
		case u.Scheme == "https" && host.Port == 443:
			u.Host = host.Name
		}
	}

	uu := url.URL(u)
	return uu.String()
}

// MarshalJSON - converts to JSON string data.
func (u URL) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// UnmarshalJSON - parses given data into URL.
func (u *URL) UnmarshalJSON(data []byte) (err error) {
	var s string
	if err = json.Unmarshal(data, &s); err != nil {
		return err
	}

	// Allow empty string
	if s == "" {
		*u = URL{}
		return nil
	}

	var ru *URL
	if ru, err = ParseURL(s); err != nil {
		return err
	}

	*u = *ru
	return nil
}

// ParseURL - parses string into URL.
func ParseURL(s string) (u *URL, err error) {
	var uu *url.URL
	if uu, err = url.Parse(s); err != nil {
		return nil, err
	}

	if uu.Host == "" {
		if uu.Scheme != "" {
			return nil, errors.New("scheme appears with empty host")
		}
	} else if _, err = ParseHost(uu.Host); err != nil {
		return nil, err
	}

	// Clean path in the URL.
	// Note: path.Clean() is used on purpose because in MS Windows filepath.Clean() converts
	// `/` into `\` ie `/foo` becomes `\foo`
	if uu.Path != "" {
		uu.Path = path.Clean(uu.Path)
	}

	v := URL(*uu)
	u = &v
	return u, nil
}
