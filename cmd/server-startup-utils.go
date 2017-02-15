/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
)

// getListenIPs - gets all the ips to listen on.
func getListenIPs(serverAddr string) (hosts []string, port string, err error) {
	var host string
	host, port, err = net.SplitHostPort(serverAddr)
	if err != nil {
		return nil, port, fmt.Errorf("Unable to parse host address %s", err)
	}
	if host == "" {
		var ipv4s []net.IP
		ipv4s, err = getInterfaceIPv4s()
		if err != nil {
			return nil, port, fmt.Errorf("Unable reverse sort ips from hosts %s", err)
		}
		for _, ip := range ipv4s {
			hosts = append(hosts, ip.String())
		}
		return hosts, port, nil
	} // if host != "" {

	// Proceed to append itself, since user requested a specific endpoint.
	hosts = append(hosts, host)

	// Success.
	return hosts, port, nil
}

// Finalizes the API endpoints based on the host list and port.
func finalizeAPIEndpoints(addr string) (endPoints []string, err error) {
	// Verify current scheme.
	scheme := httpScheme
	if globalIsSSL {
		scheme = httpsScheme
	}

	// Get list of listen ips and port.
	hosts, port, err1 := getListenIPs(addr)
	if err1 != nil {
		return nil, err1
	}

	// Construct proper endpoints.
	for _, host := range hosts {
		endPoints = append(endPoints, fmt.Sprintf("%s://%s:%s", scheme, host, port))
	}

	// Success.
	return endPoints, nil
}
