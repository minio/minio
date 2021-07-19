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
	"reflect"
	"testing"
	"time"

	"github.com/minio/madmin-go"
)

const (
	oneMiB uint64 = 1024 * 1024
)

func TestMonitor_GetReport(t *testing.T) {
	type fields struct {
		activeBuckets map[string]*bucketMeasurement
		endTime       time.Time
		update2       uint64
		endTime2      time.Time
	}
	start := time.Now()
	m0 := newBucketMeasurement(start)
	m0.incrementBytes(0)
	m1MiBPS := newBucketMeasurement(start)
	m1MiBPS.incrementBytes(oneMiB)
	tests := []struct {
		name   string
		fields fields
		want   *madmin.BucketBandwidthReport
		want2  *madmin.BucketBandwidthReport
	}{
		{
			name: "ZeroToOne",
			fields: fields{
				activeBuckets: map[string]*bucketMeasurement{
					"bucket": m0,
				},
				endTime:  start.Add(1 * time.Second),
				update2:  oneMiB,
				endTime2: start.Add(2 * time.Second),
			},
			want: &madmin.BucketBandwidthReport{
				BucketStats: map[string]madmin.BandwidthDetails{"bucket": {LimitInBytesPerSecond: 1024 * 1024, CurrentBandwidthInBytesPerSecond: 0}},
			},
			want2: &madmin.BucketBandwidthReport{
				BucketStats: map[string]madmin.BandwidthDetails{"bucket": {LimitInBytesPerSecond: 1024 * 1024, CurrentBandwidthInBytesPerSecond: (1024 * 1024) / start.Add(2*time.Second).Sub(start.Add(1*time.Second)).Seconds()}},
			},
		},
		{
			name: "OneToTwo",
			fields: fields{
				activeBuckets: map[string]*bucketMeasurement{
					"bucket": m1MiBPS,
				},
				endTime:  start.Add(1 * time.Second),
				update2:  2 * oneMiB,
				endTime2: start.Add(2 * time.Second),
			},
			want: &madmin.BucketBandwidthReport{
				BucketStats: map[string]madmin.BandwidthDetails{"bucket": {LimitInBytesPerSecond: 1024 * 1024, CurrentBandwidthInBytesPerSecond: float64(oneMiB)}},
			},
			want2: &madmin.BucketBandwidthReport{
				BucketStats: map[string]madmin.BandwidthDetails{"bucket": {
					LimitInBytesPerSecond:            1024 * 1024,
					CurrentBandwidthInBytesPerSecond: exponentialMovingAverage(betaBucket, float64(oneMiB), 2*float64(oneMiB))}},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			thr := throttle{
				NodeBandwidthPerSec: 1024 * 1024,
			}
			m := &Monitor{
				activeBuckets:  tt.fields.activeBuckets,
				bucketThrottle: map[string]*throttle{"bucket": &thr},
				NodeCount:      1,
			}
			m.activeBuckets["bucket"].updateExponentialMovingAverage(tt.fields.endTime)
			got := m.GetReport(SelectBuckets())
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetReport() = %v, want %v", got, tt.want)
			}
			m.activeBuckets["bucket"].incrementBytes(tt.fields.update2)
			m.activeBuckets["bucket"].updateExponentialMovingAverage(tt.fields.endTime2)
			got = m.GetReport(SelectBuckets())
			if !reflect.DeepEqual(got, tt.want2) {
				t.Errorf("GetReport() = %v, want %v", got, tt.want2)
			}
		})
	}
}
