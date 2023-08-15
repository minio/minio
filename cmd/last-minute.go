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

//go:generate msgp -file=$GOFILE -unexported

package cmd

import (
	"time"

	"github.com/minio/madmin-go/v3"
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
	Size  int64
	N     int64
}

// Add a duration to a single element.
func (a *AccElem) add(dur time.Duration) {
	if dur < 0 {
		dur = 0
	}
	a.Total += int64(dur)
	a.N++
}

// Merge b into a.
func (a *AccElem) merge(b AccElem) {
	a.N += b.N
	a.Total += b.Total
	a.Size += b.Size
}

// Avg returns average time spent.
func (a AccElem) avg() time.Duration {
	if a.N >= 1 && a.Total > 0 {
		return time.Duration(a.Total / a.N)
	}
	return 0
}

// asTimedAction returns the element as a madmin.TimedAction.
func (a AccElem) asTimedAction() madmin.TimedAction {
	return madmin.TimedAction{AccTime: uint64(a.Total), Count: uint64(a.N), Bytes: uint64(a.Size)}
}

// lastMinuteLatency keeps track of last minute latency.
type lastMinuteLatency struct {
	Totals  [60]AccElem
	LastSec int64
}

// Merge data of two lastMinuteLatency structure
func (l lastMinuteLatency) merge(o lastMinuteLatency) (merged lastMinuteLatency) {
	if l.LastSec > o.LastSec {
		o.forwardTo(l.LastSec)
		merged.LastSec = l.LastSec
	} else {
		l.forwardTo(o.LastSec)
		merged.LastSec = o.LastSec
	}

	for i := range merged.Totals {
		merged.Totals[i] = AccElem{
			Total: l.Totals[i].Total + o.Totals[i].Total,
			N:     l.Totals[i].N + o.Totals[i].N,
			Size:  l.Totals[i].Size + o.Totals[i].Size,
		}
	}
	return merged
}

// Add  a new duration data
func (l *lastMinuteLatency) add(t time.Duration) {
	sec := time.Now().Unix()
	l.forwardTo(sec)
	winIdx := sec % 60
	l.Totals[winIdx].add(t)
	l.LastSec = sec
}

// Add  a new duration data
func (l *lastMinuteLatency) addAll(sec int64, a AccElem) {
	l.forwardTo(sec)
	winIdx := sec % 60
	l.Totals[winIdx].merge(a)
	l.LastSec = sec
}

// Merge all recorded latencies of last minute into one
func (l *lastMinuteLatency) getTotal() AccElem {
	var res AccElem
	sec := time.Now().Unix()
	l.forwardTo(sec)
	for _, elem := range l.Totals[:] {
		res.merge(elem)
	}
	return res
}

// forwardTo time t, clearing any entries in between.
func (l *lastMinuteLatency) forwardTo(t int64) {
	if l.LastSec >= t {
		return
	}
	if t-l.LastSec >= 60 {
		l.Totals = [60]AccElem{}
		return
	}
	for l.LastSec != t {
		// Clear next element.
		idx := (l.LastSec + 1) % 60
		l.Totals[idx] = AccElem{}
		l.LastSec++
	}
}

// LastMinuteHistogram keeps track of last minute sizes added.
type LastMinuteHistogram [sizeLastElemMarker]lastMinuteLatency

// Merge safely merges two LastMinuteHistogram structures into one
func (l LastMinuteHistogram) Merge(o LastMinuteHistogram) (merged LastMinuteHistogram) {
	for i := range l {
		merged[i] = l[i].merge(o[i])
	}
	return merged
}

// Add latency t from object with the specified size.
func (l *LastMinuteHistogram) Add(size int64, t time.Duration) {
	l[sizeToTag(size)].add(t)
}

// GetAvgData will return the average for each bucket from the last time minute.
// The number of objects is also included.
func (l *LastMinuteHistogram) GetAvgData() [sizeLastElemMarker]AccElem {
	var res [sizeLastElemMarker]AccElem
	for i, elem := range l[:] {
		res[i] = elem.getTotal()
	}
	return res
}
