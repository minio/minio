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
                                    <Prefix>path/to/my/workload/_staging/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_temporary/*</Prefix>
                                  </ExcludedPrefixes>
                                </VersioningConfiguration>`,
			err:              nil,
			excludedPrefixes: []string{"path/to/my/workload/_staging/*", "path/to/my/workload/_temporary/*"},
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
                                    <Prefix>path/to/my/workload/_staging/ab/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/cd/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/ef/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/gh/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/ij/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/kl/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/mn/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/op/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/qr/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/st/*</Prefix>
                                  </ExcludedPrefixes>
                                  <ExcludedPrefixes>
                                    <Prefix>path/to/my/workload/_staging/uv/*</Prefix>
                                  </ExcludedPrefixes>
                                </VersioningConfiguration>`,
			err: errTooManyExcludedPrefixes,
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
