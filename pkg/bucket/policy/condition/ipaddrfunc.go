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

package condition

import (
	"fmt"
	"net"
	"sort"
)

// ipaddrFunc - IP address function. It checks whether value by Key in given
// values is in IP network.  Here Key must be AWSSourceIP.
// For example,
//   - if values = [192.168.1.0/24], at evaluate() it returns whether IP address
//     in value map for AWSSourceIP falls in the network 192.168.1.10/24.
type ipaddrFunc struct {
	n      name
	k      Key
	values []*net.IPNet
	negate bool
}

func (f ipaddrFunc) eval(values map[string][]string) bool {
	rvalues := getValuesByKey(values, f.k.Name())
	IPs := []net.IP{}
	for _, s := range rvalues {
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

// evaluate() - evaluates to check whether IP address in values map for AWSSourceIP
// falls in one of network or not.
func (f ipaddrFunc) evaluate(values map[string][]string) bool {
	result := f.eval(values)
	if f.negate {
		return !result
	}
	return result
}

// key() - returns condition key which is used by this condition function.
// Key is always AWSSourceIP.
func (f ipaddrFunc) key() Key {
	return f.k
}

// name() - returns "IpAddress" condition name.
func (f ipaddrFunc) name() name {
	return f.n
}

func (f ipaddrFunc) String() string {
	valueStrings := []string{}
	for _, value := range f.values {
		valueStrings = append(valueStrings, value.String())
	}
	sort.Strings(valueStrings)

	return fmt.Sprintf("%v:%v:%v", f.n, f.k, valueStrings)
}

// toMap - returns map representation of this function.
func (f ipaddrFunc) toMap() map[Key]ValueSet {
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

func (f ipaddrFunc) clone() Function {
	values := []*net.IPNet{}
	for _, value := range f.values {
		_, IPNet, _ := net.ParseCIDR(value.String())
		values = append(values, IPNet)
	}
	return &ipaddrFunc{
		n:      f.n,
		k:      f.k,
		values: values,
		negate: f.negate,
	}
}

func valuesToIPNets(n string, values ValueSet) ([]*net.IPNet, error) {
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

func newIPAddrFunc(n string, key Key, values []*net.IPNet, negate bool) (Function, error) {
	if key != AWSSourceIP {
		return nil, fmt.Errorf("only %v key is allowed for %v condition", AWSSourceIP, n)
	}

	return &ipaddrFunc{
		n:      name{name: n},
		k:      key,
		values: values,
		negate: negate,
	}, nil
}

// newIPAddressFunc - returns new IP address function.
func newIPAddressFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	IPNets, err := valuesToIPNets(ipAddress, values)
	if err != nil {
		return nil, err
	}

	return NewIPAddressFunc(key, IPNets...)
}

// NewIPAddressFunc - returns new IP address function.
func NewIPAddressFunc(key Key, IPNets ...*net.IPNet) (Function, error) {
	return newIPAddrFunc(ipAddress, key, IPNets, false)
}

// newNotIPAddressFunc - returns new Not IP address function.
func newNotIPAddressFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	IPNets, err := valuesToIPNets(notIPAddress, values)
	if err != nil {
		return nil, err
	}

	return NewNotIPAddressFunc(key, IPNets...)
}

// NewNotIPAddressFunc - returns new Not IP address function.
func NewNotIPAddressFunc(key Key, IPNets ...*net.IPNet) (Function, error) {
	return newIPAddrFunc(notIPAddress, key, IPNets, true)
}
