/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

var (
	tformats = []string{
		layoutYear,
		layoutMonth,
		layoutDay,
		layoutMinute,
		layoutSecond,
		layoutNanosecond,
	}
)

func parseSQLTimestamp(s string) (t time.Time, err error) {
	for _, f := range tformats {
		t, err = time.Parse(f, s)
		if err == nil {
			break
		}
	}
	return
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

const (
	dayInNanoseconds = time.Hour * 24
)

// dateDiff computes the difference between two times in terms of the
// `timePart` which can be years, months, days, hours, minutes or
// seconds. For difference in years, months or days, the time part,
// including timezone is ignored.
func dateDiff(timePart string, ts1, ts2 time.Time) (*Value, error) {
	if ts2.Before(ts1) {
		v, err := dateDiff(timePart, ts2, ts1)
		v.negate()
		return v, err
	}

	duration := ts2.Sub(ts1)
	y1, m1, d1 := ts1.Date()
	y2, m2, d2 := ts2.Date()
	dy, dm := int64(y2-y1), int64(m2-m1)

	switch timePart {
	case timePartYear:
		if m2 > m1 || (m2 == m1 && d2 >= d1) {
			return FromInt(dy), nil
		}
		return FromInt(dy - 1), nil
	case timePartMonth:
		months := 12 * dy
		if m2 >= m1 {
			months += dm
		} else {
			months += 12 + dm
		}
		if d2 < d1 {
			months--
		}
		return FromInt(months), nil
	case timePartDay:
		// To compute the number of days between two times
		// using the time package, zero out the time portions
		// of the timestamps, compute the difference duration
		// and then divide by the length of a day.
		d1 := time.Date(y1, m1, d1, 0, 0, 0, 0, time.UTC)
		d2 := time.Date(y2, m2, d2, 0, 0, 0, 0, time.UTC)
		diff := d2.Sub(d1)
		days := diff / dayInNanoseconds
		return FromInt(int64(days)), nil
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
