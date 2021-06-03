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

package bandwidth

import (
	"context"
	"io"
)

// MonitoredReader monitors the bandwidth
type MonitoredReader struct {
	opts              *MonitorReaderOptions
	bucketMeasurement *bucketMeasurement // bucket measurement object
	reader            io.Reader          // Reader to wrap
	throttle          *throttle          // throttle the rate at which replication occur
	monitor           *Monitor           // Monitor reference
	lastErr           error              // last error reported, if this non-nil all reads will fail.
}

// MonitorReaderOptions provides configurable options for monitor reader implementation.
type MonitorReaderOptions struct {
	Bucket               string
	Object               string
	HeaderSize           int
	BandwidthBytesPerSec int64
	ClusterBandwidth     int64
}

// NewMonitoredReader returns a io.Reader that reports bandwidth details.
func NewMonitoredReader(ctx context.Context, monitor *Monitor, reader io.Reader, opts *MonitorReaderOptions) *MonitoredReader {
	return &MonitoredReader{
		opts:              opts,
		bucketMeasurement: monitor.track(opts.Bucket, opts.Object),
		reader:            reader,
		throttle:          monitor.throttleBandwidth(ctx, opts.Bucket, opts.BandwidthBytesPerSec, opts.ClusterBandwidth),
		monitor:           monitor,
	}
}

// Read wraps the read reader
func (m *MonitoredReader) Read(p []byte) (n int, err error) {
	if m.lastErr != nil {
		err = m.lastErr
		return
	}

	p = p[:m.throttle.GetLimitForBytes(int64(len(p)))]

	n, err = m.reader.Read(p)
	if err != nil {
		m.lastErr = err
	}

	update := n + m.opts.HeaderSize
	unused := len(p) - update

	m.bucketMeasurement.incrementBytes(uint64(update))
	m.opts.HeaderSize = 0 // Set to 0 post first read

	if unused > 0 {
		m.throttle.ReleaseUnusedBandwidth(int64(unused))
	}

	return
}
