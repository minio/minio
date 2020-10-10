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
	"time"
)

// MonitoredReader monitors the bandwidth
type MonitoredReader struct {
	bucket            string             // Token to track bucket
	bucketMeasurement *bucketMeasurement // bucket measurement object
	object            string             // Token to track object
	reader            io.Reader          // Reader to wrap
	lastStop          time.Time          // Last timestamp for a measurement
	headerSize        int                // Size of the header not captured by reader
	throttle          *throttle          // throttle the rate at which replication occur
	monitor           *Monitor           // Monitor reference
	closed            bool               // Reader is closed
}

// NewMonitoredReader returns a io.ReadCloser that reports bandwidth details
func NewMonitoredReader(ctx context.Context, monitor *Monitor, bucket string, object string, reader io.Reader, headerSize int, bandwidthBytesPerSecond int64) *MonitoredReader {
	timeNow := time.Now()
	b := monitor.track(bucket, object, timeNow)
	return &MonitoredReader{
		bucket:            bucket,
		object:            object,
		bucketMeasurement: b,
		reader:            reader,
		lastStop:          timeNow,
		headerSize:        headerSize,
		throttle:          monitor.throttleBandwidth(ctx, bucket, bandwidthBytesPerSecond),
		monitor:           monitor,
	}
}

// Read wraps the read reader
func (m *MonitoredReader) Read(p []byte) (n int, err error) {
	if m.closed {
		err = io.ErrClosedPipe
		return
	}
	p = p[:m.throttle.GetLimitForBytes(int64(len(p)))]

	n, err = m.reader.Read(p)
	stop := time.Now()
	update := uint64(n + m.headerSize)

	m.bucketMeasurement.incrementBytes(update)
	m.lastStop = stop
	unused := len(p) - (n + m.headerSize)
	m.headerSize = 0 // Set to 0 post first read

	if unused > 0 {
		m.throttle.ReleaseUnusedBandwidth(int64(unused))
	}
	return
}

// Close stops tracking the io
func (m *MonitoredReader) Close() error {
	rc, ok := m.reader.(io.ReadCloser)
	m.closed = true
	if ok {
		return rc.Close()
	}
	return nil
}
