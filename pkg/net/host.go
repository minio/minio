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
	"net"
	"regexp"
	"strings"
)

var hostLabelRegexp = regexp.MustCompile("^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$")

// Host - holds network host IP/name and its port.
type Host struct {
	Name      string
	Port      Port
	IsPortSet bool
}

// IsEmpty - returns whether Host is empty or not
func (host Host) IsEmpty() bool {
	return host.Name == ""
}

// String - returns string representation of Host.
func (host Host) String() string {
	if !host.IsPortSet {
		return host.Name
	}

	return net.JoinHostPort(host.Name, host.Port.String())
}

// Equal - checks whether given host is equal or not.
func (host Host) Equal(compHost Host) bool {
	return host.String() == compHost.String()
}

// MarshalJSON - converts Host into JSON data
func (host Host) MarshalJSON() ([]byte, error) {
	return json.Marshal(host.String())
}

// UnmarshalJSON - parses data into Host.
func (host *Host) UnmarshalJSON(data []byte) (err error) {
	var s string
	if err = json.Unmarshal(data, &s); err != nil {
		return err
	}

	// Allow empty string
	if s == "" {
		*host = Host{}
		return nil
	}

	var h *Host
	if h, err = ParseHost(s); err != nil {
		return err
	}

	*host = *h
	return nil
}

// ParseHost - parses string into Host
func ParseHost(s string) (*Host, error) {
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

	var port Port
	var isPortSet bool
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		if !strings.Contains(err.Error(), "missing port in address") {
			return nil, err
		}

		host = s
		portStr = ""
	} else {
		if port, err = ParsePort(portStr); err != nil {
			return nil, err
		}

		isPortSet = true
	}

	if i := strings.Index(host, "%"); i > -1 {
		host = host[:i]
	}

	if !isValidHost(host) {
		return nil, errors.New("invalid hostname")
	}

	return &Host{
		Name:      host,
		Port:      port,
		IsPortSet: isPortSet,
	}, nil
}
