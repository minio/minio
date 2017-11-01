/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"net"
	"regexp"
	"strings"
)

var hostLabelRegexp = regexp.MustCompile("^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$")

// Host - holds network host and its port.
type Host struct {
	Host       string
	PortNumber Port
	IsPortSet  bool
}

// IsEmpty - returns whether Host is empty or not
func (hp Host) IsEmpty() bool {
	return hp.Host == ""
}

// String - returns string representation of Host.
func (hp Host) String() string {
	if !hp.IsPortSet {
		return hp.Host
	}

	return hp.Host + ":" + hp.PortNumber.String()
}

// MarshalJSON - converts Host into JSON data
func (hp Host) MarshalJSON() ([]byte, error) {
	return json.Marshal(hp.String())
}

// UnmarshalJSON - parses data into Host.
func (hp *Host) UnmarshalJSON(data []byte) (err error) {
	var s string
	if err = json.Unmarshal(data, &s); err != nil {
		return err
	}

	// Allow empty string
	if s == "" {
		return nil
	}

	var h *Host
	if h, err = ParseHost(s); err != nil {
		return err
	}

	*hp = *h
	return nil
}

// ParseHost - parses string into Host
func ParseHost(s string) (h *Host, err error) {
	isValidHost := func(host string) bool {
		if host == "" {
			return false
		}

		if ip := net.ParseIP(host); ip != nil {
			return true
		}

		// host is not a valid IPv4 or IPv6 address
		// host may be a hostname
		// refer https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_host_names
		// why checks are done like below
		if len(host) < 1 || len(host) > 253 {
			return false
		}

		for _, label := range strings.Split(host, ".") {
			if len(label) < 1 || len(label) > 63 {
				return false
			}

			if !hostLabelRegexp.MatchString(label) {
				return false
			}
		}

		return true
	}

	isPortSet := true
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		if !strings.Contains(err.Error(), "missing port in address") {
			return nil, err
		}

		host = s
		portStr = ""
		isPortSet = false
	}

	var p Port
	if isPortSet {
		p, err = ParsePort(portStr)
		if err != nil {
			return nil, err
		}
	}

	if !isValidHost(host) {
		return nil, errors.New("invalid hostname")
	}

	return &Host{
		Host:       host,
		PortNumber: p,
		IsPortSet:  isPortSet,
	}, nil
}

// MustParseHost - parses given string to Host, else panics.
func MustParseHost(s string) *Host {
	h, err := ParseHost(s)
	if err != nil {
		panic(err)
	}
	return h
}
