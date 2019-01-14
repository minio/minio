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

package condition

import (
	"fmt"
	"net"
	"net/http"
	"sort"
)

func toIPAddressFuncString(n name, key Key, values []*net.IPNet) string {
	valueStrings := []string{}
	for _, value := range values {
		valueStrings = append(valueStrings, value.String())
	}
	sort.Strings(valueStrings)

	return fmt.Sprintf("%v:%v:%v", n, key, valueStrings)
}

// ipAddressFunc - IP address function. It checks whether value by Key in given
// values is in IP network.  Here Key must be AWSSourceIP.
// For example,
//   - if values = [192.168.1.0/24], at evaluate() it returns whether IP address
//     in value map for AWSSourceIP falls in the network 192.168.1.10/24.
type ipAddressFunc struct {
	k      Key
	values []*net.IPNet
}

// evaluate() - evaluates to check whether IP address in values map for AWSSourceIP
// falls in one of network or not.
func (f ipAddressFunc) evaluate(values map[string][]string) bool {
	IPs := []net.IP{}
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	for _, s := range requestValue {
		IP := net.ParseIP(s)
		if IP == nil {
			panic(fmt.Errorf("invalid IP address '%v'", s))
		}

		IPs = append(IPs, IP)
	}

	for _, IP := range IPs {
		for _, IPNet := range f.values {
			if IPNet.Contains(IP) {
				return true
			}
		}
	}

	return false
}

// key() - returns condition key which is used by this condition function.
// Key is always AWSSourceIP.
func (f ipAddressFunc) key() Key {
	return f.k
}

// name() - returns "IpAddress" condition name.
func (f ipAddressFunc) name() name {
	return ipAddress
}

func (f ipAddressFunc) String() string {
	return toIPAddressFuncString(ipAddress, f.k, f.values)
}

// toMap - returns map representation of this function.
func (f ipAddressFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	for _, value := range f.values {
		values.Add(NewStringValue(value.String()))
	}

	return map[Key]ValueSet{
		f.k: values,
	}
}

// notIPAddressFunc - Not IP address function. It checks whether value by Key in given
// values is NOT in IP network.  Here Key must be AWSSourceIP.
// For example,
//   - if values = [192.168.1.0/24], at evaluate() it returns whether IP address
//     in value map for AWSSourceIP does not fall in the network 192.168.1.10/24.
type notIPAddressFunc struct {
	ipAddressFunc
}

// evaluate() - evaluates to check whether IP address in values map for AWSSourceIP
// does not fall in one of network.
func (f notIPAddressFunc) evaluate(values map[string][]string) bool {
	return !f.ipAddressFunc.evaluate(values)
}

// name() - returns "NotIpAddress" condition name.
func (f notIPAddressFunc) name() name {
	return notIPAddress
}

func (f notIPAddressFunc) String() string {
	return toIPAddressFuncString(notIPAddress, f.ipAddressFunc.k, f.ipAddressFunc.values)
}

func valuesToIPNets(n name, values ValueSet) ([]*net.IPNet, error) {
	IPNets := []*net.IPNet{}
	for v := range values {
		s, err := v.GetString()
		if err != nil {
			return nil, fmt.Errorf("value %v must be string representation of CIDR for %v condition", v, n)
		}

		var IPNet *net.IPNet
		_, IPNet, err = net.ParseCIDR(s)
		if err != nil {
			return nil, fmt.Errorf("value %v must be CIDR string for %v condition", s, n)
		}

		IPNets = append(IPNets, IPNet)
	}

	return IPNets, nil
}

// newIPAddressFunc - returns new IP address function.
func newIPAddressFunc(key Key, values ValueSet) (Function, error) {
	IPNets, err := valuesToIPNets(ipAddress, values)
	if err != nil {
		return nil, err
	}

	return NewIPAddressFunc(key, IPNets...)
}

// NewIPAddressFunc - returns new IP address function.
func NewIPAddressFunc(key Key, IPNets ...*net.IPNet) (Function, error) {
	if key != AWSSourceIP {
		return nil, fmt.Errorf("only %v key is allowed for %v condition", AWSSourceIP, ipAddress)
	}

	return &ipAddressFunc{key, IPNets}, nil
}

// newNotIPAddressFunc - returns new Not IP address function.
func newNotIPAddressFunc(key Key, values ValueSet) (Function, error) {
	IPNets, err := valuesToIPNets(notIPAddress, values)
	if err != nil {
		return nil, err
	}

	return NewNotIPAddressFunc(key, IPNets...)
}

// NewNotIPAddressFunc - returns new Not IP address function.
func NewNotIPAddressFunc(key Key, IPNets ...*net.IPNet) (Function, error) {
	if key != AWSSourceIP {
		return nil, fmt.Errorf("only %v key is allowed for %v condition", AWSSourceIP, notIPAddress)
	}

	return &notIPAddressFunc{ipAddressFunc{key, IPNets}}, nil
}
