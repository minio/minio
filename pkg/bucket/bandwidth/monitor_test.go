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
 */

package bandwidth

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/minio/minio/pkg/bandwidth"
)

const (
	oneMiB uint64 = 1024 * 1024
)

func TestMonitor_GetThrottle(t *testing.T) {
	type fields struct {
		bucketThrottles map[string]*throttle
		bucket          string
		bpi             int64
	}
	t1 := newThrottle(context.Background(), 100)
	t2 := newThrottle(context.Background(), 200)
	tests := []struct {
		name   string
		fields fields
		want   *throttle
	}{
		{
			name: "Existing",
			fields: fields{
				bucketThrottles: map[string]*throttle{"bucket": t1},
				bucket:          "bucket",
				bpi:             100,
			},
			want: t1,
		},
		{
			name: "new",
			fields: fields{
				bucketThrottles: map[string]*throttle{"bucket": t1},
				bucket:          "bucket2",
				bpi:             200,
			},
			want: t2,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := &Monitor{
				bucketThrottle: tt.fields.bucketThrottles,
			}
			if got := m.throttleBandwidth(context.Background(), tt.fields.bucket, tt.fields.bpi); got.bytesPerInterval != tt.want.bytesPerInterval {
				t.Errorf("throttleBandwidth() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
		want   *bandwidth.Report
		want2  *bandwidth.Report
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
			want: &bandwidth.Report{
				BucketStats: map[string]bandwidth.Details{"bucket": {LimitInBytesPerSecond: 1024 * 1024, CurrentBandwidthInBytesPerSecond: 0}},
			},
			want2: &bandwidth.Report{
				BucketStats: map[string]bandwidth.Details{"bucket": {LimitInBytesPerSecond: 1024 * 1024, CurrentBandwidthInBytesPerSecond: (1024 * 1024) / start.Add(2*time.Second).Sub(start.Add(1*time.Second)).Seconds()}},
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
			want: &bandwidth.Report{
				BucketStats: map[string]bandwidth.Details{"bucket": {LimitInBytesPerSecond: 1024 * 1024, CurrentBandwidthInBytesPerSecond: float64(oneMiB)}},
			},
			want2: &bandwidth.Report{
				BucketStats: map[string]bandwidth.Details{"bucket": {
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
				bytesPerSecond: 1024 * 1024,
			}
			m := &Monitor{
				activeBuckets:  tt.fields.activeBuckets,
				bucketThrottle: map[string]*throttle{"bucket": &thr},
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
