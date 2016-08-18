/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"net"
	"sort"
)

// byLastOctet implements sort.Interface used in sorting a list
// of ip address by their last octet value.
type byLastOctet []net.IP

func (n byLastOctet) Len() int      { return len(n) }
func (n byLastOctet) Swap(i, j int) { n[i], n[j] = n[j], n[i] }
func (n byLastOctet) Less(i, j int) bool {
	return []byte(n[i].To4())[3] < []byte(n[j].To4())[3]
}

// getIPsFromHosts - returns a reverse sorted list of ips based on the last octet value.
func getIPsFromHosts(hosts []string) (ips []net.IP) {
	for _, host := range hosts {
		ips = append(ips, net.ParseIP(host))
	}
	// Reverse sort ips by their last octet.
	sort.Sort(sort.Reverse(byLastOctet(ips)))
	return ips
}
