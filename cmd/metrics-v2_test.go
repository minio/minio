// Copyright (c) 2015-2023 MinIO, Inc.
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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestGetHistogramMetrics(t *testing.T) {
	histBuckets := []float64{0.05, 0.1, 0.25, 0.5, 0.75}
	labels := []string{"GetObject", "PutObject", "CopyObject", "CompleteMultipartUpload"}
	ttfbHist := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3_ttfb_seconds",
			Help:    "Time taken by requests served by current MinIO server instance",
			Buckets: histBuckets,
		},
		[]string{"api"},
	)
	observations := []struct {
		val   float64
		label string
	}{
		{
			val:   0.02,
			label: labels[0],
		},
		{
			val:   0.07,
			label: labels[1],
		},
		{
			val:   0.11,
			label: labels[1],
		},
		{
			val:   0.19,
			label: labels[1],
		},
		{
			val:   0.31,
			label: labels[1],
		},
		{
			val:   0.61,
			label: labels[3],
		},
		{
			val:   0.79,
			label: labels[2],
		},
	}
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	for _, obs := range observations {
		// Send observations once every 1ms, to simulate delay between
		// observations. This is to test the channel based
		// synchronization used internally.
		select {
		case <-ticker.C:
			ttfbHist.With(prometheus.Labels{"api": obs.label}).Observe(obs.val)
		}
	}

	metrics := getHistogramMetrics(ttfbHist, getBucketTTFBDistributionMD())
	// additional labels for +Inf for all histogram metrics so check with double
	if expPoints := 2 * len(labels) * len(histBuckets); expPoints != len(metrics) {
		t.Fatalf("Expected %v data points but got %v", expPoints, len(metrics))
	}
}
