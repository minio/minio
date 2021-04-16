/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

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
