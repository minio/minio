package types

// timestamp.go defines the timestamp type and implements sort.Interface
// interface for slices of timestamps.

import (
	"time"
)

type (
	Timestamp      uint64
	TimestampSlice []Timestamp
)

// CurrentTimestamp returns the current time as a Timestamp.
func CurrentTimestamp() Timestamp {
	return Timestamp(time.Now().Unix())
}

// Len is part of sort.Interface
func (ts TimestampSlice) Len() int {
	return len(ts)
}

// Less is part of sort.Interface
func (ts TimestampSlice) Less(i, j int) bool {
	return ts[i] < ts[j]
}

// Swap is part of sort.Interface
func (ts TimestampSlice) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

// Clock allows clients to retrieve the current time.
type Clock interface {
	Now() Timestamp
}

// StdClock is an implementation of Clock that retrieves the current time using
// the system time.
type StdClock struct{}

// Now retrieves the current timestamp.
func (c StdClock) Now() Timestamp {
	return Timestamp(time.Now().Unix())
}
