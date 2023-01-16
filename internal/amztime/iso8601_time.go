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

package amztime

import (
	"strings"
	"time"
)

// RFC3339 a subset of the ISO8601 timestamp format. e.g 2014-04-29T18:30:38Z
const (
	iso8601TimeFormat     = "2006-01-02T15:04:05.000Z"    // Reply date format with millisecond precision.
	iso8601TimeFormatLong = "2006-01-02T15:04:05.000000Z" // Reply date format with nanosecond precision.
)

// ISO8601Format converts time 't' into ISO8601 time format expected in AWS S3 spec.
//
// This function is needed to avoid a Go's float64 precision bug, where Go avoids
// padding the extra '0' before the timezone.
func ISO8601Format(t time.Time) string {
	value := t.Format(iso8601TimeFormat)
	if len(value) < len(iso8601TimeFormat) {
		value = t.Format(iso8601TimeFormat[:len(iso8601TimeFormat)-1])
		// Pad necessary zeroes to full-fill the iso8601TimeFormat
		return value + strings.Repeat("0", (len(iso8601TimeFormat)-1)-len(value)) + "Z"
	}
	return value
}

// ISO8601Parse parses ISO8601 date string
func ISO8601Parse(iso8601 string) (t time.Time, err error) {
	for _, layout := range []string{
		iso8601TimeFormat,
		iso8601TimeFormatLong,
		time.RFC3339,
	} {
		t, err = time.Parse(layout, iso8601)
		if err == nil {
			return t, nil
		}
	}

	return t, err
}
