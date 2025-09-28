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

package sql

import (
	"time"
)

const (
	layoutYear       = "2006T"
	layoutMonth      = "2006-01T"
	layoutDay        = "2006-01-02T"
	layoutMinute     = "2006-01-02T15:04Z07:00"
	layoutSecond     = "2006-01-02T15:04:05Z07:00"
	layoutNanosecond = "2006-01-02T15:04:05.999999999Z07:00"
)

var tformats = []string{
	layoutYear,
	layoutMonth,
	layoutDay,
	layoutMinute,
	layoutSecond,
	layoutNanosecond,
}

func parseSQLTimestamp(s string) (t time.Time, err error) {
	for _, f := range tformats {
		t, err = time.Parse(f, s)
		if err == nil {
			break
		}
	}
	return t, err
}

// FormatSQLTimestamp - returns the a string representation of the
// timestamp as used in S3 Select
func FormatSQLTimestamp(t time.Time) string {
	_, zoneOffset := t.Zone()
	hasZone := zoneOffset != 0
	hasFracSecond := t.Nanosecond() != 0
	hasSecond := t.Second() != 0
	hasTime := t.Hour() != 0 || t.Minute() != 0
	hasDay := t.Day() != 1
	hasMonth := t.Month() != 1

	switch {
	case hasFracSecond:
		return t.Format(layoutNanosecond)
	case hasSecond:
		return t.Format(layoutSecond)
	case hasTime || hasZone:
		return t.Format(layoutMinute)
	case hasDay:
		return t.Format(layoutDay)
	case hasMonth:
		return t.Format(layoutMonth)
	default:
		return t.Format(layoutYear)
	}
}

const (
	timePartYear           = "YEAR"
	timePartMonth          = "MONTH"
	timePartDay            = "DAY"
	timePartHour           = "HOUR"
	timePartMinute         = "MINUTE"
	timePartSecond         = "SECOND"
	timePartTimezoneHour   = "TIMEZONE_HOUR"
	timePartTimezoneMinute = "TIMEZONE_MINUTE"
)

func extract(what string, t time.Time) (v *Value, err error) {
	switch what {
	case timePartYear:
		return FromInt(int64(t.Year())), nil
	case timePartMonth:
		return FromInt(int64(t.Month())), nil
	case timePartDay:
		return FromInt(int64(t.Day())), nil
	case timePartHour:
		return FromInt(int64(t.Hour())), nil
	case timePartMinute:
		return FromInt(int64(t.Minute())), nil
	case timePartSecond:
		return FromInt(int64(t.Second())), nil
	case timePartTimezoneHour:
		_, zoneOffset := t.Zone()
		return FromInt(int64(zoneOffset / 3600)), nil
	case timePartTimezoneMinute:
		_, zoneOffset := t.Zone()
		return FromInt(int64((zoneOffset % 3600) / 60)), nil
	default:
		// This does not happen
		return nil, errNotImplemented
	}
}

func dateAdd(timePart string, qty float64, t time.Time) (*Value, error) {
	var duration time.Duration
	switch timePart {
	case timePartYear:
		return FromTimestamp(t.AddDate(int(qty), 0, 0)), nil
	case timePartMonth:
		return FromTimestamp(t.AddDate(0, int(qty), 0)), nil
	case timePartDay:
		return FromTimestamp(t.AddDate(0, 0, int(qty))), nil
	case timePartHour:
		duration = time.Duration(qty) * time.Hour
	case timePartMinute:
		duration = time.Duration(qty) * time.Minute
	case timePartSecond:
		duration = time.Duration(qty) * time.Second
	default:
		return nil, errNotImplemented
	}
	return FromTimestamp(t.Add(duration)), nil
}

// dateDiff computes the difference between two times in terms of the
// `timePart` which can be years, months, days, hours, minutes or
// seconds. For difference in years, months or days, the time part,
// including timezone is ignored.
func dateDiff(timePart string, ts1, ts2 time.Time) (*Value, error) {
	if ts2.Before(ts1) {
		v, err := dateDiff(timePart, ts2, ts1)
		if err == nil {
			v.negate()
		}
		return v, err
	}

	duration := ts2.Sub(ts1)
	y1, m1, d1 := ts1.Date()
	y2, m2, d2 := ts2.Date()

	switch timePart {
	case timePartYear:
		dy := int64(y2 - y1)
		if m2 > m1 || (m2 == m1 && d2 >= d1) {
			return FromInt(dy), nil
		}
		return FromInt(dy - 1), nil
	case timePartMonth:
		m1 += time.Month(12 * y1)
		m2 += time.Month(12 * y2)

		return FromInt(int64(m2 - m1)), nil
	case timePartDay:
		return FromInt(int64(duration / (24 * time.Hour))), nil
	case timePartHour:
		hours := duration / time.Hour
		return FromInt(int64(hours)), nil
	case timePartMinute:
		minutes := duration / time.Minute
		return FromInt(int64(minutes)), nil
	case timePartSecond:
		seconds := duration / time.Second
		return FromInt(int64(seconds)), nil
	default:
	}
	return nil, errNotImplemented
}
