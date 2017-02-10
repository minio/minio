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

package cmd

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio-go/pkg/set"
)

// IsValidDistribution - checks whether given count is a valid distribution for erasure coding.
func IsValidDistribution(count int) bool {
	return (count >= 4 && count <= 16 && count%2 == 0)
}

// CheckLocalServerAddr - checks if serverAddr is valid and local host.
func CheckLocalServerAddr(serverAddr string) error {
	serverAddr = strings.TrimSpace(serverAddr)
	if serverAddr == "" {
		// empty server address means local host.
		return nil
	}

	host, port, err := net.SplitHostPort(serverAddr)
	if err != nil {
		return err
	}

	// Check whether port is a valid port number.
	p, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("invalid port number in server address")
	} else if p < 1 {
		return fmt.Errorf("port number should be greater than zero in server address")
	}

	if host != "" {
		localIPs := mustGetLocalIP4()
		hostIPs, err := getHostIP4(host)
		if err != nil {
			return err
		}

		if localIPs.Intersection(hostIPs).IsEmpty() {
			return fmt.Errorf("host in server address should be this server")
		}
	}

	return nil
}

// IsEmptyPath - check whether given path is not empty.
func IsEmptyPath(path string) bool {
	return path == "" || path == "." || path == "/" || path == `\`
}

// EndpointType - enum for endpoint type.
type EndpointType int

const (
	// UnknownEndpointType - unknown endpoint type enum.
	UnknownEndpointType EndpointType = 0

	// PathEndpointType - path style endpoint type enum.
	PathEndpointType = 1

	// URLEndpointType - URL style endpoint type enum.
	URLEndpointType = 2
)

// Endpoint - any type of endpoint.
type Endpoint struct {
	Value      string
	URL        *url.URL
	IsDisabled bool
	IsLocal    bool
}

// Type - returns type of endpoint.
func (endpoint Endpoint) Type() EndpointType {
	if IsEmptyPath(endpoint.Value) {
		return UnknownEndpointType
	}

	if endpoint.URL != nil {
		return URLEndpointType
	}

	return PathEndpointType
}

// NewEndpoint - returns new endpoint based on given arguments.
func NewEndpoint(arg string) (Endpoint, error) {
	if IsEmptyPath(arg) {
		return Endpoint{}, fmt.Errorf("Empty or root endpoint is not supported")
	}

	u, err := url.Parse(arg)
	if err == nil && u.Host != "" {
		// URL style of endpoint.
		if !((u.Scheme == "http" || u.Scheme == "https") &&
			u.Opaque == "" && u.ForceQuery == false && u.RawQuery == "" && u.Fragment == "") {
			return Endpoint{}, fmt.Errorf("Unknown endpoint format")
		}

		host, port, err := splitHostPort(u.Host)
		if !(err == nil && host != "") {
			return Endpoint{}, fmt.Errorf("Invalid host in endpoint format")
		}

		if IsEmptyPath(u.Path) {
			return Endpoint{}, fmt.Errorf("Empty or root path is not supported in URL endpoint")
		}

		u.Host = net.JoinHostPort(host, port)
		arg = u.String()
	} else {
		u = nil
	}

	return Endpoint{
		Value: arg,
		URL:   u,
	}, nil
}

// EndpointList - list of same type of endpoint.
type EndpointList []Endpoint

// NewEndpointList - returns new endpoint list based on input args.
func NewEndpointList(args ...string) (endpoints EndpointList, err error) {
	// Check whether given args contain duplicates.
	if uniqueArgs := set.CreateStringSet(args...); len(uniqueArgs) != len(args) {
		return nil, fmt.Errorf("duplicate endpoints found")
	}

	// Check whether no. of args are valid for XL distribution.
	if !IsValidDistribution(len(args)) {
		return nil, fmt.Errorf("total endpoints %d found. For XL/Distribute, it should be 4, 6, 8, 10, 12, 14 or 16", len(args))
	}

	sort.Strings(args)

	var endpointType EndpointType
	var scheme string

	// Loop through args and adds to endpoint list.
	for i, arg := range args {
		endpoint, err := NewEndpoint(arg)
		if err != nil {
			return nil, fmt.Errorf("unknown endpoint format %s", arg)
		}

		// All endpoints have to be same type and scheme if applicable.
		if i == 0 {
			endpointType = endpoint.Type()
			if endpoint.URL != nil {
				scheme = endpoint.URL.Scheme
			}
		} else if endpoint.Type() != endpointType {
			return nil, fmt.Errorf("mixed style endpoints are not supported")
		} else if endpoint.URL != nil && scheme != endpoint.URL.Scheme {
			return nil, fmt.Errorf("mixed scheme is not supported")
		}

		endpoints = append(endpoints, endpoint)
	}

	return endpoints, nil
}
