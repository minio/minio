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
	if s == "" {
		return nil, errors.New("invalid argument")
	}
	isValidHost := func(host string) bool {
		if host == "" {
			return true
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
	} else {
		if port, err = ParsePort(portStr); err != nil {
			return nil, err
		}

		isPortSet = true
	}

	if host != "" {
		host, err = trimIPv6(host)
		if err != nil {
			return nil, err
		}
	}

	// IPv6 requires a link-local address on every network interface.
	// `%interface` should be preserved.
	trimmedHost := host

	if i := strings.LastIndex(trimmedHost, "%"); i > -1 {
		// `%interface` can be skipped for validity check though.
		trimmedHost = trimmedHost[:i]
	}

	if !isValidHost(trimmedHost) {
		return nil, errors.New("invalid hostname")
	}

	return &Host{
		Name:      host,
		Port:      port,
		IsPortSet: isPortSet,
	}, nil
}

// IPv6 can be embedded with square brackets.
func trimIPv6(host string) (string, error) {
	// `missing ']' in host` error is already handled in `SplitHostPort`
	if host[len(host)-1] == ']' {
		if host[0] != '[' {
			return "", errors.New("missing '[' in host")
		}
		return host[1:][:len(host)-2], nil
	}
	return host, nil
}
