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

import "testing"

// Tests various variants in supporting all the byte conversions.
func TestByteConv(t *testing.T) {
	// List of all tests for testing notation to corresponding
	// byte conversions.
	tests := []struct {
		in  string
		exp uint64
	}{
		// Using IEC notation.
		{"42", 42},
		{"42MB", 42000000},
		{"42MiB", 44040192},
		{"42mb", 42000000},
		{"42mib", 44040192},
		{"42MIB", 44040192},
		{"42 MB", 42000000},
		{"42 MiB", 44040192},
		{"42 mb", 42000000},
		{"42 mib", 44040192},
		{"42 MIB", 44040192},
		{"42.5MB", 42500000},
		{"42.5MiB", 44564480},
		{"42.5 MB", 42500000},
		{"42.5 MiB", 44564480},
		// Using SI notation.
		{"42M", 42000000},
		{"42Mi", 44040192},
		{"42m", 42000000},
		{"42mi", 44040192},
		{"42MI", 44040192},
		{"42 M", 42000000},
		{"42 Mi", 44040192},
		{"42 m", 42000000},
		{"42 mi", 44040192},
		{"42 MI", 44040192},
		// With decimal values.
		{"42.5M", 42500000},
		{"42.5Mi", 44564480},
		{"42.5 M", 42500000},
		{"42.5 Mi", 44564480},
		// With no more digits after '.'
		{"42.M", 42000000},
		{"42.Mi", 44040192},
		{"42. m", 42000000},
		{"42. mi", 44040192},
		{"42. M", 42000000},
		{"42. Mi", 44040192},
		// Large testing, breaks when too much larger than this.
		{"12.5 EB", uint64(12.5 * float64(EByte))},
		{"12.5 E", uint64(12.5 * float64(EByte))},
		{"12.5 EiB", uint64(12.5 * float64(EiByte))},
	}

	// Tests all notation variants.
	for _, p := range tests {
		got, err := strconvBytes(p.in)
		if err != nil {
			t.Errorf("Couldn't parse %v: %v", p.in, err)
		}
		if got != p.exp {
			t.Errorf("Expected %v for %v, got %v", p.exp, p.in, got)
		}
	}
}

// Validates different types of input errors.
func TestByteErrors(t *testing.T) {
	// Input with integer and double space between notations.
	got, err := strconvBytes("84 JB")
	if err == nil {
		t.Errorf("Expected error, got %v", got)
	}
	// Empty string.
	_, err = strconvBytes("")
	if err == nil {
		t.Errorf("Expected error parsing nothing")
	}
	// Too large.
	got, err = strconvBytes("16 EiB")
	if err == nil {
		t.Errorf("Expected error, got %v", got)
	}
}

// Add benchmarks here.

// Benchmarks for bytes converter.
func BenchmarkParseBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strconvBytes("16.5 GB")
	}
}
