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

package lock

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	xhttp "github.com/minio/minio/internal/http"
)

func TestParseMode(t *testing.T) {
	testCases := []struct {
		value        string
		expectedMode RetMode
	}{
		{
			value:        "governance",
			expectedMode: RetGovernance,
		},
		{
			value:        "complIAnce",
			expectedMode: RetCompliance,
		},
		{
			value:        "gce",
			expectedMode: "",
		},
	}

	for _, tc := range testCases {
		if parseRetMode(tc.value) != tc.expectedMode {
			t.Errorf("Expected Mode %s, got %s", tc.expectedMode, parseRetMode(tc.value))
		}
	}
}

func TestParseLegalHoldStatus(t *testing.T) {
	tests := []struct {
		value          string
		expectedStatus LegalHoldStatus
	}{
		{
			value:          "ON",
			expectedStatus: LegalHoldOn,
		},
		{
			value:          "Off",
			expectedStatus: LegalHoldOff,
		},
		{
			value:          "x",
			expectedStatus: "",
		},
	}

	for _, tt := range tests {
		actualStatus := parseLegalHoldStatus(tt.value)
		if actualStatus != tt.expectedStatus {
			t.Errorf("Expected legal hold status %s, got %s", tt.expectedStatus, actualStatus)
		}
	}
}

// TestUnmarshalDefaultRetention checks if default retention
// marshaling and unmarshalling work as expected
func TestUnmarshalDefaultRetention(t *testing.T) {
	days := uint64(4)
	years := uint64(1)
	zerodays := uint64(0)
	invalidDays := uint64(maximumRetentionDays + 1)
	tests := []struct {
		value       DefaultRetention
		expectedErr error
		expectErr   bool
	}{
		{
			value:       DefaultRetention{Mode: "retain"},
			expectedErr: fmt.Errorf("unknown retention mode retain"),
			expectErr:   true,
		},
		{
			value:       DefaultRetention{Mode: RetGovernance},
			expectedErr: fmt.Errorf("either Days or Years must be specified"),
			expectErr:   true,
		},
		{
			value:       DefaultRetention{Mode: RetGovernance, Days: &days},
			expectedErr: nil,
			expectErr:   false,
		},
		{
			value:       DefaultRetention{Mode: RetGovernance, Years: &years},
			expectedErr: nil,
			expectErr:   false,
		},
		{
			value:       DefaultRetention{Mode: RetGovernance, Days: &days, Years: &years},
			expectedErr: fmt.Errorf("either Days or Years must be specified, not both"),
			expectErr:   true,
		},
		{
			value:       DefaultRetention{Mode: RetGovernance, Days: &zerodays},
			expectedErr: fmt.Errorf("Default retention period must be a positive integer value for 'Days'"),
			expectErr:   true,
		},
		{
			value:       DefaultRetention{Mode: RetGovernance, Days: &invalidDays},
			expectedErr: fmt.Errorf("Default retention period too large for 'Days' %d", invalidDays),
			expectErr:   true,
		},
	}
	for _, tt := range tests {
		d, err := xml.MarshalIndent(&tt.value, "", "\t")
		if err != nil {
			t.Fatal(err)
		}
		var dr DefaultRetention
		err = xml.Unmarshal(d, &dr)
		//nolint:gocritic
		if tt.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", tt.expectedErr)
		} else if tt.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", tt.expectedErr, err)
		}
	}
}

func TestParseObjectLockConfig(t *testing.T) {
	tests := []struct {
		value       string
		expectedErr error
		expectErr   bool
	}{
		{
			value:       `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>yes</ObjectLockEnabled></ObjectLockConfiguration>`,
			expectedErr: fmt.Errorf("only 'Enabled' value is allowed to ObjectLockEnabled element"),
			expectErr:   true,
		},
		{
			value:       `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>0</Days></DefaultRetention></Rule></ObjectLockConfiguration>`,
			expectedErr: fmt.Errorf("Default retention period must be a positive integer value for 'Days'"),
			expectErr:   true,
		},
		{
			value:       `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>`,
			expectedErr: nil,
			expectErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			_, err := ParseObjectLockConfig(strings.NewReader(tt.value))
			//nolint:gocritic
			if tt.expectedErr == nil {
				if err != nil {
					t.Fatalf("error: expected = <nil>, got = %v", err)
				}
			} else if err == nil {
				t.Fatalf("error: expected = %v, got = <nil>", tt.expectedErr)
			} else if tt.expectedErr.Error() != err.Error() {
				t.Fatalf("error: expected = %v, got = %v", tt.expectedErr, err)
			}
		})
	}
}

func TestParseObjectRetention(t *testing.T) {
	tests := []struct {
		value       string
		expectedErr error
		expectErr   bool
	}{
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>string</Mode><RetainUntilDate>2020-01-02T15:04:05Z</RetainUntilDate></Retention>`,
			expectedErr: ErrUnknownWORMModeDirective,
			expectErr:   true,
		},
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>COMPLIANCE</Mode><RetainUntilDate>2017-01-02T15:04:05Z</RetainUntilDate></Retention>`,
			expectedErr: ErrPastObjectLockRetainDate,
			expectErr:   true,
		},
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>GOVERNANCE</Mode><RetainUntilDate>2057-01-02T15:04:05Z</RetainUntilDate></Retention>`,
			expectedErr: nil,
			expectErr:   false,
		},
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>GOVERNANCE</Mode><RetainUntilDate>2057-01-02T15:04:05.000Z</RetainUntilDate></Retention>`,
			expectedErr: nil,
			expectErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			_, err := ParseObjectRetention(strings.NewReader(tt.value))
			//nolint:gocritic
			if tt.expectedErr == nil {
				if err != nil {
					t.Fatalf("error: expected = <nil>, got = %v", err)
				}
			} else if err == nil {
				t.Fatalf("error: expected = %v, got = <nil>", tt.expectedErr)
			} else if tt.expectedErr.Error() != err.Error() {
				t.Fatalf("error: expected = %v, got = %v", tt.expectedErr, err)
			}
		})
	}
}

func TestIsObjectLockRequested(t *testing.T) {
	tests := []struct {
		header      http.Header
		expectedVal bool
	}{
		{
			header: http.Header{
				"Authorization":        []string{"AWS4-HMAC-SHA256 <cred_string>"},
				"X-Amz-Content-Sha256": []string{""},
				"Content-Encoding":     []string{""},
			},
			expectedVal: false,
		},
		{
			header: http.Header{
				AmzObjectLockLegalHold: []string{""},
			},
			expectedVal: true,
		},
		{
			header: http.Header{
				AmzObjectLockRetainUntilDate: []string{""},
				AmzObjectLockMode:            []string{""},
			},
			expectedVal: true,
		},
		{
			header: http.Header{
				AmzObjectLockBypassRetGovernance: []string{""},
			},
			expectedVal: false,
		},
	}
	for _, tt := range tests {
		actualVal := IsObjectLockRequested(tt.header)
		if actualVal != tt.expectedVal {
			t.Fatalf("error: expected %v, actual %v", tt.expectedVal, actualVal)
		}
	}
}

func TestIsObjectLockGovernanceBypassSet(t *testing.T) {
	tests := []struct {
		header      http.Header
		expectedVal bool
	}{
		{
			header: http.Header{
				"Authorization":        []string{"AWS4-HMAC-SHA256 <cred_string>"},
				"X-Amz-Content-Sha256": []string{""},
				"Content-Encoding":     []string{""},
			},
			expectedVal: false,
		},
		{
			header: http.Header{
				AmzObjectLockLegalHold: []string{""},
			},
			expectedVal: false,
		},
		{
			header: http.Header{
				AmzObjectLockRetainUntilDate: []string{""},
				AmzObjectLockMode:            []string{""},
			},
			expectedVal: false,
		},
		{
			header: http.Header{
				AmzObjectLockBypassRetGovernance: []string{""},
			},
			expectedVal: false,
		},
		{
			header: http.Header{
				AmzObjectLockBypassRetGovernance: []string{"true"},
			},
			expectedVal: true,
		},
	}
	for _, tt := range tests {
		actualVal := IsObjectLockGovernanceBypassSet(tt.header)
		if actualVal != tt.expectedVal {
			t.Fatalf("error: expected %v, actual %v", tt.expectedVal, actualVal)
		}
	}
}

func TestParseObjectLockRetentionHeaders(t *testing.T) {
	tests := []struct {
		header      http.Header
		expectedErr error
	}{
		{
			header: http.Header{
				"Authorization":        []string{"AWS4-HMAC-SHA256 <cred_string>"},
				"X-Amz-Content-Sha256": []string{""},
				"Content-Encoding":     []string{""},
			},
			expectedErr: ErrObjectLockInvalidHeaders,
		},
		{
			header: http.Header{
				xhttp.AmzObjectLockMode:            []string{"lock"},
				xhttp.AmzObjectLockRetainUntilDate: []string{"2017-01-02"},
			},
			expectedErr: ErrUnknownWORMModeDirective,
		},
		{
			header: http.Header{
				xhttp.AmzObjectLockMode: []string{"governance"},
			},
			expectedErr: ErrObjectLockInvalidHeaders,
		},
		{
			header: http.Header{
				xhttp.AmzObjectLockRetainUntilDate: []string{"2017-01-02"},
				xhttp.AmzObjectLockMode:            []string{"governance"},
			},
			expectedErr: ErrInvalidRetentionDate,
		},
		{
			header: http.Header{
				xhttp.AmzObjectLockRetainUntilDate: []string{"2017-01-02T15:04:05Z"},
				xhttp.AmzObjectLockMode:            []string{"governance"},
			},
			expectedErr: ErrPastObjectLockRetainDate,
		},
		{
			header: http.Header{
				xhttp.AmzObjectLockMode:            []string{"governance"},
				xhttp.AmzObjectLockRetainUntilDate: []string{"2017-01-02T15:04:05Z"},
			},
			expectedErr: ErrPastObjectLockRetainDate,
		},
		{
			header: http.Header{
				xhttp.AmzObjectLockMode:            []string{"governance"},
				xhttp.AmzObjectLockRetainUntilDate: []string{"2087-01-02T15:04:05Z"},
			},
			expectedErr: nil,
		},
		{
			header: http.Header{
				xhttp.AmzObjectLockMode:            []string{"governance"},
				xhttp.AmzObjectLockRetainUntilDate: []string{"2087-01-02T15:04:05.000Z"},
			},
			expectedErr: nil,
		},
	}

	for i, tt := range tests {
		_, _, err := ParseObjectLockRetentionHeaders(tt.header)
		//nolint:gocritic
		if tt.expectedErr == nil {
			if err != nil {
				t.Fatalf("Case %d error: expected = <nil>, got = %v", i, err)
			}
		} else if err == nil {
			t.Fatalf("Case %d error: expected = %v, got = <nil>", i, tt.expectedErr)
		} else if tt.expectedErr.Error() != err.Error() {
			t.Fatalf("Case %d error: expected = %v, got = %v", i, tt.expectedErr, err)
		}
	}
}

func TestGetObjectRetentionMeta(t *testing.T) {
	tests := []struct {
		metadata map[string]string
		expected ObjectRetention
	}{
		{
			metadata: map[string]string{
				"Authorization":        "AWS4-HMAC-SHA256 <cred_string>",
				"X-Amz-Content-Sha256": "",
				"Content-Encoding":     "",
			},
			expected: ObjectRetention{},
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-mode": "governance",
			},
			expected: ObjectRetention{Mode: RetGovernance},
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-retain-until-date": "2020-02-01",
			},
			expected: ObjectRetention{RetainUntilDate: RetentionDate{time.Date(2020, 2, 1, 12, 0, 0, 0, time.UTC)}},
		},
	}

	for i, tt := range tests {
		o := GetObjectRetentionMeta(tt.metadata)
		if o.Mode != tt.expected.Mode {
			t.Fatalf("Case %d expected %v, got %v", i, tt.expected.Mode, o.Mode)
		}
	}
}

func TestGetObjectLegalHoldMeta(t *testing.T) {
	tests := []struct {
		metadata map[string]string
		expected ObjectLegalHold
	}{
		{
			metadata: map[string]string{
				"x-amz-object-lock-mode": "governance",
			},
			expected: ObjectLegalHold{},
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-legal-hold": "on",
			},
			expected: ObjectLegalHold{Status: LegalHoldOn},
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-legal-hold": "off",
			},
			expected: ObjectLegalHold{Status: LegalHoldOff},
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-legal-hold": "X",
			},
			expected: ObjectLegalHold{Status: ""},
		},
	}

	for i, tt := range tests {
		o := GetObjectLegalHoldMeta(tt.metadata)
		if o.Status != tt.expected.Status {
			t.Fatalf("Case %d expected %v, got %v", i, tt.expected.Status, o.Status)
		}
	}
}

func TestParseObjectLegalHold(t *testing.T) {
	tests := []struct {
		value       string
		expectedErr error
		expectErr   bool
	}{
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>string</Status></LegalHold>`,
			expectedErr: ErrMalformedXML,
			expectErr:   true,
		},
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></LegalHold>`,
			expectedErr: nil,
			expectErr:   false,
		},
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><ObjectLockLegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></ObjectLockLegalHold>`,
			expectedErr: nil,
			expectErr:   false,
		},
		// invalid Status key
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><ObjectLockLegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><MyStatus>ON</MyStatus></ObjectLockLegalHold>`,
			expectedErr: errors.New("expected element type <Status> but have <MyStatus>"),
			expectErr:   true,
		},
		// invalid XML attr
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><UnknownLegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></UnknownLegalHold>`,
			expectedErr: errors.New("expected element type <LegalHold>/<ObjectLockLegalHold> but have <UnknownLegalHold>"),
			expectErr:   true,
		},
		{
			value:       `<?xml version="1.0" encoding="UTF-8"?><LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>On</Status></LegalHold>`,
			expectedErr: ErrMalformedXML,
			expectErr:   true,
		},
	}
	for i, tt := range tests {
		_, err := ParseObjectLegalHold(strings.NewReader(tt.value))
		//nolint:gocritic
		if tt.expectedErr == nil {
			if err != nil {
				t.Fatalf("Case %d error: expected = <nil>, got = %v", i, err)
			}
		} else if err == nil {
			t.Fatalf("Case %d error: expected = %v, got = <nil>", i, tt.expectedErr)
		} else if tt.expectedErr.Error() != err.Error() {
			t.Fatalf("Case %d error: expected = %v, got = %v", i, tt.expectedErr, err)
		}
	}
}

func TestFilterObjectLockMetadata(t *testing.T) {
	tests := []struct {
		metadata        map[string]string
		filterRetention bool
		filterLegalHold bool
		expected        map[string]string
	}{
		{
			metadata: map[string]string{
				"Authorization":        "AWS4-HMAC-SHA256 <cred_string>",
				"X-Amz-Content-Sha256": "",
				"Content-Encoding":     "",
			},
			expected: map[string]string{
				"Authorization":        "AWS4-HMAC-SHA256 <cred_string>",
				"X-Amz-Content-Sha256": "",
				"Content-Encoding":     "",
			},
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-mode": "governance",
			},
			expected: map[string]string{
				"x-amz-object-lock-mode": "governance",
			},
			filterRetention: false,
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-mode":              "governance",
				"x-amz-object-lock-retain-until-date": "2020-02-01",
			},
			expected:        map[string]string{},
			filterRetention: true,
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-legal-hold": "off",
			},
			expected:        map[string]string{},
			filterLegalHold: true,
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-legal-hold": "on",
			},
			expected:        map[string]string{"x-amz-object-lock-legal-hold": "on"},
			filterLegalHold: false,
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-legal-hold":        "on",
				"x-amz-object-lock-mode":              "governance",
				"x-amz-object-lock-retain-until-date": "2020-02-01",
			},
			expected:        map[string]string{},
			filterRetention: true,
			filterLegalHold: true,
		},
		{
			metadata: map[string]string{
				"x-amz-object-lock-legal-hold":        "on",
				"x-amz-object-lock-mode":              "governance",
				"x-amz-object-lock-retain-until-date": "2020-02-01",
			},
			expected: map[string]string{
				"x-amz-object-lock-legal-hold":        "on",
				"x-amz-object-lock-mode":              "governance",
				"x-amz-object-lock-retain-until-date": "2020-02-01",
			},
		},
	}

	for i, tt := range tests {
		o := FilterObjectLockMetadata(tt.metadata, tt.filterRetention, tt.filterLegalHold)
		if !reflect.DeepEqual(o, tt.expected) {
			t.Fatalf("Case %d expected %v, got %v", i, tt.metadata, o)
		}
	}
}

func TestToString(t *testing.T) {
	days := uint64(30)
	daysPtr := &days
	years := uint64(2)
	yearsPtr := &years

	tests := []struct {
		name string
		c    Config
		want string
	}{
		{
			name: "happy case",
			c: Config{
				ObjectLockEnabled: "Enabled",
			},
			want: "Enabled: true",
		},
		{
			name: "with default retention days",
			c: Config{
				ObjectLockEnabled: "Enabled",
				Rule: &struct {
					DefaultRetention DefaultRetention `xml:"DefaultRetention"`
				}{
					DefaultRetention: DefaultRetention{
						Mode: RetGovernance,
						Days: daysPtr,
					},
				},
			},
			want: "Enabled: true, Mode: GOVERNANCE, Days: 30",
		},
		{
			name: "with default retention years",
			c: Config{
				ObjectLockEnabled: "Enabled",
				Rule: &struct {
					DefaultRetention DefaultRetention `xml:"DefaultRetention"`
				}{
					DefaultRetention: DefaultRetention{
						Mode:  RetCompliance,
						Years: yearsPtr,
					},
				},
			},
			want: "Enabled: true, Mode: COMPLIANCE, Years: 2",
		},
		{
			name: "disabled case",
			c: Config{
				ObjectLockEnabled: "Disabled",
			},
			want: "Enabled: false",
		},
		{
			name: "empty case",
			c:    Config{},
			want: "Enabled: false",
		},
	}
	for _, tt := range tests {
		got := tt.c.String()
		if got != tt.want {
			t.Errorf("test: %s, got: '%v', want: '%v'", tt.name, got, tt.want)
		}
	}
}
