package versioning

import (
	"strings"
	"testing"
)

func TestParseConfig(t *testing.T) {
	testcases := []struct {
		input string
		err   error
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
                                  <SuspendedPrefixes>
                                    <Prefix> path/to/my/workload/_staging </Prefix>
                                    <Prefix> path/to/my/workload/_temporary/ </Prefix>
                                  </SuspendedPrefixes>
                                </VersioningConfiguration>`,
			err: nil,
		},
		{
			input: `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                  <Status>Suspended</Status>
                                  <SuspendedPrefixes>
                                    <Prefix> path/to/my/workload/_staging </Prefix>
                                  </SuspendedPrefixes>
                                </VersioningConfiguration>`,
			err: errSuspendedPrefixNotSupported,
		},
		{
			input: `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                  <Status>Enabled</Status>
                                  <SuspendedPrefixes>
                                    <Prefix> path/to/my/workload/_staging/ab </Prefix>
                                    <Prefix> path/to/my/workload/_staging/cd </Prefix>
                                    <Prefix> path/to/my/workload/_staging/ef </Prefix>
                                    <Prefix> path/to/my/workload/_staging/gh </Prefix>
                                    <Prefix> path/to/my/workload/_staging/ij </Prefix>
                                    <Prefix> path/to/my/workload/_staging/kl </Prefix>
                                    <Prefix> path/to/my/workload/_staging/mn </Prefix>
                                    <Prefix> path/to/my/workload/_staging/op </Prefix>
                                    <Prefix> path/to/my/workload/_staging/qr </Prefix>
                                    <Prefix> path/to/my/workload/_staging/st </Prefix>
                                    <Prefix> path/to/my/workload/_staging/uv </Prefix>
                                  </SuspendedPrefixes>
                                </VersioningConfiguration>`,
			err: errTooManySuspendedPrefixes,
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
		}
	}
}
