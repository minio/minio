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
	"fmt"
	"net"
	"sort"
)

// byLastOctetValue implements sort.Interface used in sorting a list
// of ip address by their last octet value.
type byLastOctetValue []net.IP

func (n byLastOctetValue) Len() int      { return len(n) }
func (n byLastOctetValue) Swap(i, j int) { n[i], n[j] = n[j], n[i] }
func (n byLastOctetValue) Less(i, j int) bool {
	return []byte(n[i].To4())[3] < []byte(n[j].To4())[3]
}

// getInterfaceIPv4s is synonymous to net.InterfaceAddrs()
// returns net.IP IPv4 only representation of the net.Addr.
// Additionally the returned list is sorted by their last
// octet value.
//
// [The logic to sort by last octet is implemented to
// prefer CIDRs with higher octects, this in-turn skips the
// localhost/loopback address to be not preferred as the
// first ip on the list. Subsequently this list helps us print
// a user friendly message with appropriate values].
func getInterfaceIPv4s() ([]net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("Unable to determine network interface address. %s", err)
	}
	// Go through each return network address and collate IPv4 addresses.
	var nips []net.IP
	for _, addr := range addrs {
		if addr.Network() == "ip+net" {
			var nip net.IP
			// Attempt to parse the addr through CIDR.
			nip, _, err = net.ParseCIDR(addr.String())
			if err != nil {
				return nil, fmt.Errorf("Unable to parse addrss %s, error %s", addr, err)
			}
			// Collect only IPv4 addrs.
			if nip.To4() != nil {
				nips = append(nips, nip)
			}
		}
	}
	// Sort the list of IPs by their last octet value.
	sort.Sort(sort.Reverse(byLastOctetValue(nips)))
	return nips, nil
}
