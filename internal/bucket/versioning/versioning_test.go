// Copyright (c) 2015-2022 MinIO, Inc.
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

package versioning

import (
	"encoding/xml"
	"strings"
	"testing"
)

func TestParseConfig(t *testing.T) {
	testcases := []struct {
		input            string
		err              error
		excludedPrefixes []string
		excludeFolders   bool
	}{
		{
			input: `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                  <Status>Enabled</Status>
                                </VersioningConfiguration>`,
			err: nil,
		},
		{
			input: `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                  <Status>Enabled</Status>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_temporary/</Prefix>
                                  </ExcludedPrefixes>
                                </VersioningConfiguration>`,
			err:              nil,
			excludedPrefixes: []string{"path/to/my/workload/_staging/", "path/to/my/workload/_temporary/"},
		},
		{
			input: `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                  <Status>Suspended</Status>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging</Prefix>
                                  </ExcludedPrefixes>
                                </VersioningConfiguration>`,
			err: errExcludedPrefixNotSupported,
		},
		{
			input: `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                  <Status>Enabled</Status>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/ab/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/cd/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/ef/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/gh/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/ij/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/kl/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/mn/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/op/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/qr/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/st/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/uv/</Prefix>
                                  </ExcludedPrefixes>
                                </VersioningConfiguration>`,
			err: errTooManyExcludedPrefixes,
		},
		{
			input: `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                  <Status>Enabled</Status>
                                  <ExcludeFolders>true</ExcludeFolders>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_temporary/</Prefix>
                                  </ExcludedPrefixes>
                                </VersioningConfiguration>`,
			err:              nil,
			excludedPrefixes: []string{"path/to/my/workload/_staging/", "path/to/my/workload/_temporary/"},
			excludeFolders:   true,
		},
	}

	for i, tc := range testcases {
		var v *Versioning
		var err error
		v, err = ParseConfig(strings.NewReader(tc.input))
		if tc.err != err {
			t.Fatalf("Test %d: expected %v but got %v", i+1, tc.err, err)
		}
		if err != nil {
			if tc.err == nil {
				t.Fatalf("Test %d: failed due to %v", i+1, err)
			}
		} else {
			if err := v.Validate(); tc.err != err {
				t.Fatalf("Test %d: validation failed due to %v", i+1, err)
			}
			if len(tc.excludedPrefixes) > 0 {
				var mismatch bool
				if len(v.ExcludedPrefixes) != len(tc.excludedPrefixes) {
					t.Fatalf("Test %d: Expected length of excluded prefixes %d but got %d", i+1, len(tc.excludedPrefixes), len(v.ExcludedPrefixes))
				}
				var i int
				var eprefix string
				for i, eprefix = range tc.excludedPrefixes {
					if eprefix != v.ExcludedPrefixes[i].Prefix {
						mismatch = true
						break
					}
				}
				if mismatch {
					t.Fatalf("Test %d: Expected excluded prefix %s but got %s", i+1, tc.excludedPrefixes[i], v.ExcludedPrefixes[i].Prefix)
				}
			}
			if tc.excludeFolders != v.ExcludeFolders {
				t.Fatalf("Test %d: Expected ExcludeFoldersr=%v but got %v", i+1, tc.excludeFolders, v.ExcludeFolders)
			}
		}
	}
}

func TestMarshalXML(t *testing.T) {
	// Validates if Versioning with no excluded prefixes omits
	// ExcludedPrefixes tags
	v := Versioning{
		Status: Enabled,
	}
	buf, err := xml.Marshal(v)
	if err != nil {
		t.Fatalf("Failed to marshal %v: %v", v, err)
	}

	str := string(buf)
	if strings.Contains(str, "ExcludedPrefixes") {
		t.Fatalf("XML shouldn't contain ExcludedPrefixes tag - %s", str)
	}
}

func TestVersioningZero(t *testing.T) {
	var v Versioning
	if v.Enabled() {
		t.Fatalf("Expected to be disabled but got enabled")
	}
	if v.Suspended() {
		t.Fatalf("Expected to be disabled but got suspended")
	}
}

func TestExcludeFolders(t *testing.T) {
	v := Versioning{
		Status:         Enabled,
		ExcludeFolders: true,
	}
	testPrefixes := []string{"jobs/output/_temporary/", "jobs/output/", "jobs/"}
	for i, prefix := range testPrefixes {
		if v.PrefixEnabled(prefix) || !v.PrefixSuspended(prefix) {
			t.Fatalf("Test %d: Expected versioning to be excluded for %s", i+1, prefix)
		}
	}

	// Test applicability for regular objects
	if prefix := "prefix-1/obj-1"; !v.PrefixEnabled(prefix) || v.PrefixSuspended(prefix) {
		t.Fatalf("Expected versioning to be enabled for %s", prefix)
	}

	// Test when ExcludeFolders is disabled
	v.ExcludeFolders = false
	for i, prefix := range testPrefixes {
		if !v.PrefixEnabled(prefix) || v.PrefixSuspended(prefix) {
			t.Fatalf("Test %d: Expected versioning to be enabled for %s", i+1, prefix)
		}
	}
}

func TestExcludedPrefixesMatch(t *testing.T) {
	v := Versioning{
		Status:           Enabled,
		ExcludedPrefixes: []ExcludedPrefix{{"*/_temporary/"}},
	}

	if err := v.Validate(); err != nil {
		t.Fatalf("Invalid test versioning config %v: %v", v, err)
	}
	tests := []struct {
		prefix   string
		excluded bool
	}{
		{
			prefix:   "app1-jobs/output/_temporary/attempt1/data.csv",
			excluded: true,
		},
		{
			prefix:   "app1-jobs/output/final/attempt1/data.csv",
			excluded: false,
		},
	}

	for i, test := range tests {
		if v.PrefixSuspended(test.prefix) != test.excluded {
			if test.excluded {
				t.Fatalf("Test %d: Expected prefix %s to be excluded from versioning", i+1, test.prefix)
			} else {
				t.Fatalf("Test %d: Expected prefix %s to have versioning enabled", i+1, test.prefix)
			}
		}
	}
}
