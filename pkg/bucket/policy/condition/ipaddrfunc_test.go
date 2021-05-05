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
	"net"
	"reflect"
	"testing"
)

func TestIPAddrFuncEvaluate(t *testing.T) {
	case1Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNotIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"SourceIp": {"192.168.1.10"}}, true},
		{case1Function, map[string][]string{"SourceIp": {"192.168.2.10"}}, false},
		{case1Function, map[string][]string{}, false},
		{case1Function, map[string][]string{"delimiter": {"/"}}, false},
		{case2Function, map[string][]string{"SourceIp": {"192.168.1.10"}}, false},
		{case2Function, map[string][]string{"SourceIp": {"192.168.2.10"}}, true},
		{case2Function, map[string][]string{}, true},
		{case2Function, map[string][]string{"delimiter": {"/"}}, true},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}

	if _, err := newIPAddressFunc(S3Prefix, NewValueSet(NewStringValue("192.168.1.0/24")), ""); err == nil {
		t.Fatalf("expected error")
	}
}

func TestIPAddrFuncKey(t *testing.T) {
	case1Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, AWSSourceIP},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestIPAddrFuncName(t *testing.T) {
	case1Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNotIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult name
	}{
		{case1Function, name{name: ipAddress}},
		{case2Function, name{name: notIPAddress}},
	}

	for i, testCase := range testCases {
		result := testCase.function.name()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestIPAddrFuncToMap(t *testing.T) {
	case1Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{
		AWSSourceIP: NewValueSet(NewStringValue("192.168.1.0/24")),
	}

	case2Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24"), NewStringValue("10.1.10.1/32")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := map[Key]ValueSet{
		AWSSourceIP: NewValueSet(NewStringValue("192.168.1.0/24"), NewStringValue("10.1.10.1/32")),
	}

	testCases := []struct {
		f              Function
		expectedResult map[Key]ValueSet
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
		{&ipaddrFunc{}, nil},
	}

	for i, testCase := range testCases {
		result := testCase.f.toMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestIPAddrFuncClone(t *testing.T) {
	_, IPNet1, err := net.ParseCIDR("192.168.1.0/24")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	_, IPNet2, err := net.ParseCIDR("10.1.10.1/32")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := &ipaddrFunc{
		n:      name{name: ipAddress},
		k:      AWSSourceIP,
		values: []*net.IPNet{IPNet1},
		negate: false,
	}

	case2Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24"), NewStringValue("10.1.10.1/32")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := &ipaddrFunc{
		n:      name{name: ipAddress},
		k:      AWSSourceIP,
		values: []*net.IPNet{IPNet1, IPNet2},
		negate: false,
	}

	case3Function, err := newNotIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Result := &ipaddrFunc{
		n:      name{name: notIPAddress},
		k:      AWSSourceIP,
		values: []*net.IPNet{IPNet1},
		negate: true,
	}

	testCases := []struct {
		f              Function
		expectedResult Function
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
		{case3Function, case3Result},
	}

	for i, testCase := range testCases {
		result := testCase.f.clone()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNewIPAddressFunc(t *testing.T) {
	case1Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24"), NewStringValue("10.1.10.1/32")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		key            Key
		values         ValueSet
		expectedResult Function
		expectErr      bool
	}{
		{AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), case1Function, false},
		{AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24"), NewStringValue("10.1.10.1/32")), case2Function, false},
		// Unsupported key error.
		{S3Prefix, NewValueSet(NewStringValue("192.168.1.0/24")), nil, true},
		// Invalid value error.
		{AWSSourceIP, NewValueSet(NewStringValue("node1.example.org")), nil, true},
		// Invalid CIDR format error.
		{AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0.0/24")), nil, true},
	}

	for i, testCase := range testCases {
		result, err := newIPAddressFunc(testCase.key, testCase.values, "")
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result.String() != testCase.expectedResult.String() {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestNewNotIPAddressFunc(t *testing.T) {
	case1Function, err := newNotIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNotIPAddressFunc(AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24"), NewStringValue("10.1.10.1/32")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		key            Key
		values         ValueSet
		expectedResult Function
		expectErr      bool
	}{
		{AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24")), case1Function, false},
		{AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0/24"), NewStringValue("10.1.10.1/32")), case2Function, false},
		// Unsupported key error.
		{S3Prefix, NewValueSet(NewStringValue("192.168.1.0/24")), nil, true},
		// Invalid value error.
		{AWSSourceIP, NewValueSet(NewStringValue("node1.example.org")), nil, true},
		// Invalid CIDR format error.
		{AWSSourceIP, NewValueSet(NewStringValue("192.168.1.0.0/24")), nil, true},
	}

	for i, testCase := range testCases {
		result, err := newNotIPAddressFunc(testCase.key, testCase.values, "")
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result.String() != testCase.expectedResult.String() {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}
