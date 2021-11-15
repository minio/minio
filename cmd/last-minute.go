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

package cmd

import (
	"time"
)

const (
	sizeLessThan1KiB = iota
	sizeLessThan1MiB
	sizeLessThan10MiB
	sizeLessThan100MiB
	sizeLessThan1GiB
	sizeGreaterThan1GiB
	// Add new entries here

	sizeLastElemMarker
)

// sizeToTag converts a size to a tag.
func sizeToTag(size int64) int {
	switch {
	case size < 1024:
		return sizeLessThan1KiB
	case size < 1024*1024:
		return sizeLessThan1MiB
	case size < 10*1024*1024:
		return sizeLessThan10MiB
	case size < 100*1024*1024:
		return sizeLessThan100MiB
	case size < 1024*1024*1024:
		return sizeLessThan1GiB
	default:
		return sizeGreaterThan1GiB
	}
}

func sizeTagToString(tag int) string {
	switch tag {
	case sizeLessThan1KiB:
		return "LESS_THAN_1_KiB"
	case sizeLessThan1MiB:
		return "LESS_THAN_1_MiB"
	case sizeLessThan10MiB:
		return "LESS_THAN_10_MiB"
	case sizeLessThan100MiB:
		return "LESS_THAN_100_MiB"
	case sizeLessThan1GiB:
		return "LESS_THAN_1_GiB"
	case sizeGreaterThan1GiB:
		return "GREATER_THAN_1_GiB"
	default:
		return "unknown"
	}
}

// AccElem holds information for calculating an average value
type AccElem struct {
	Total int64
	N     int64
}

// add dur to a as a single element.
func (a *AccElem) add(dur time.Duration) {
	a.Total += int64(dur)
	a.N++
}

// merge b into a.
func (a *AccElem) merge(b AccElem) {
	a.N += b.N
	a.Total += b.Total
}

// avg converts total to average.
func (a *AccElem) avg() uint64 {
	if a.N >= 1 && a.Total > 0 {
		return uint64(a.Total / a.N)
	}
	return 0
}

// LastMinuteLatencies keeps track of last minute latencies.
type LastMinuteLatencies struct {
	Totals  [60][sizeLastElemMarker]AccElem
	LastSec int64
}

// Clone safely returns a copy for a LastMinuteLatencies structure
func (l *LastMinuteLatencies) Clone() LastMinuteLatencies {
	n := LastMinuteLatencies{}
	n.LastSec = l.LastSec
	for i := range l.Totals {
		for j := range l.Totals[i] {
			n.Totals[i][j] = AccElem{
				Total: l.Totals[i][j].Total,
				N:     l.Totals[i][j].N,
			}
		}
	}
	return n
}

// Merge safely merges two LastMinuteLatencies structures into one
func (l LastMinuteLatencies) Merge(o LastMinuteLatencies) (merged LastMinuteLatencies) {
	cl := l.Clone()
	co := o.Clone()

	if cl.LastSec > co.LastSec {
		co.forwardTo(cl.LastSec)
		merged.LastSec = cl.LastSec
	} else {
		cl.forwardTo(co.LastSec)
		merged.LastSec = co.LastSec
	}

	for i := range cl.Totals {
		for j := range cl.Totals[i] {
			merged.Totals[i][j] = AccElem{
				Total: cl.Totals[i][j].Total + co.Totals[i][j].Total,
				N:     cl.Totals[i][j].N + co.Totals[i][j].N,
			}
		}
	}
	return merged
}

// Add latency t from object with the specified size.
func (l *LastMinuteLatencies) Add(size int64, t time.Duration) {
	tag := sizeToTag(size)

	// Update...
	sec := time.Now().Unix()
	l.forwardTo(sec)

	winIdx := sec % 60
	l.Totals[winIdx][tag].add(t)

	l.LastSec = sec
}

// GetAvg will return the average for each bucket from the last time minute.
// The number of objects is also included.
func (l *LastMinuteLatencies) GetAvg() [sizeLastElemMarker]AccElem {
	var res [sizeLastElemMarker]AccElem
	sec := time.Now().Unix()
	l.forwardTo(sec)
	for _, elems := range l.Totals[:] {
		for j := range elems {
			res[j].merge(elems[j])
		}
	}
	return res
}

// forwardTo time t, clearing any entries in between.
func (l *LastMinuteLatencies) forwardTo(t int64) {
	if l.LastSec >= t {
		return
	}
	if t-l.LastSec >= 60 {
		l.Totals = [60][sizeLastElemMarker]AccElem{}
		return
	}
	for l.LastSec != t {
		// Clear next element.
		idx := (l.LastSec + 1) % 60
		l.Totals[idx] = [sizeLastElemMarker]AccElem{}
		l.LastSec++
	}
}
