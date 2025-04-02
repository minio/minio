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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestGetHistogramMetrics_BucketCount(t *testing.T) {
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
		<-ticker.C
		ttfbHist.With(prometheus.Labels{"api": obs.label}).Observe(obs.val)
	}

	metrics := getHistogramMetrics(ttfbHist, getBucketTTFBDistributionMD(), false, false)
	// additional labels for +Inf for all histogram metrics
	if expPoints := len(labels) * (len(histBuckets) + 1); expPoints != len(metrics) {
		t.Fatalf("Expected %v data points when toLowerAPILabels=false but got %v", expPoints, len(metrics))
	}

	metrics = getHistogramMetrics(ttfbHist, getBucketTTFBDistributionMD(), true, false)
	// additional labels for +Inf for all histogram metrics
	if expPoints := len(labels) * (len(histBuckets) + 1); expPoints != len(metrics) {
		t.Fatalf("Expected %v data points when toLowerAPILabels=true but got %v", expPoints, len(metrics))
	}
}

func TestGetHistogramMetrics_Values(t *testing.T) {
	histBuckets := []float64{0.50, 5.00}
	labels := []string{"PutObject", "CopyObject"}
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
			val:   0.19,
			label: labels[1],
		},
		{
			val:   0.31,
			label: labels[1],
		},
		{
			val:   0.61,
			label: labels[0],
		},
		{
			val:   6.79,
			label: labels[1],
		},
	}
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	for _, obs := range observations {
		// Send observations once every 1ms, to simulate delay between
		// observations. This is to test the channel based
		// synchronization used internally.
		<-ticker.C
		ttfbHist.With(prometheus.Labels{"api": obs.label}).Observe(obs.val)
	}

	// Accumulate regular-cased API label metrics for 'PutObject' for deeper verification
	metrics := getHistogramMetrics(ttfbHist, getBucketTTFBDistributionMD(), false, false)
	capitalPutObjects := make([]MetricV2, 0, len(histBuckets)+1)
	for _, metric := range metrics {
		if value := metric.VariableLabels["api"]; value == "PutObject" {
			capitalPutObjects = append(capitalPutObjects, metric)
		}
	}
	if expMetricsPerAPI := len(histBuckets) + 1; expMetricsPerAPI != len(capitalPutObjects) {
		t.Fatalf("Expected %d api=PutObject metrics but got %d", expMetricsPerAPI, len(capitalPutObjects))
	}

	// Deterministic ordering
	slices.SortFunc(capitalPutObjects, func(a MetricV2, b MetricV2) int {
		le1 := a.VariableLabels["le"]
		le2 := a.VariableLabels["le"]
		return strings.Compare(le1, le2)
	})
	if le := capitalPutObjects[0].VariableLabels["le"]; le != "0.500" {
		t.Errorf("Expected le='0.050' api=PutObject metrics but got '%v'", le)
	}
	if value := capitalPutObjects[0].Value; value != 1 {
		t.Errorf("Expected le='0.050' api=PutObject value to be 1 but got '%v'", value)
	}
	if le := capitalPutObjects[1].VariableLabels["le"]; le != "5.000" {
		t.Errorf("Expected le='5.000' api=PutObject metrics but got '%v'", le)
	}
	if value := capitalPutObjects[1].Value; value != 2 {
		t.Errorf("Expected le='5.000' api=PutObject value to be 2 but got '%v'", value)
	}
	if le := capitalPutObjects[2].VariableLabels["le"]; le != "+Inf" {
		t.Errorf("Expected le='+Inf' api=PutObject metrics but got '%v'", le)
	}
	if value := capitalPutObjects[2].Value; value != 2 {
		t.Errorf("Expected le='+Inf' api=PutObject value to be 2 but got '%v'", value)
	}

	// Accumulate lower-cased API label metrics for 'copyobject' for deeper verification
	metrics = getHistogramMetrics(ttfbHist, getBucketTTFBDistributionMD(), true, false)
	lowerCopyObjects := make([]MetricV2, 0, len(histBuckets)+1)
	for _, metric := range metrics {
		if value := metric.VariableLabels["api"]; value == "copyobject" {
			lowerCopyObjects = append(lowerCopyObjects, metric)
		}
	}
	if expMetricsPerAPI := len(histBuckets) + 1; expMetricsPerAPI != len(lowerCopyObjects) {
		t.Fatalf("Expected %d api=copyobject metrics but got %d", expMetricsPerAPI, len(lowerCopyObjects))
	}

	// Deterministic ordering
	slices.SortFunc(lowerCopyObjects, func(a MetricV2, b MetricV2) int {
		le1 := a.VariableLabels["le"]
		le2 := a.VariableLabels["le"]
		return strings.Compare(le1, le2)
	})
	if le := lowerCopyObjects[0].VariableLabels["le"]; le != "0.500" {
		t.Errorf("Expected le='0.050' api=copyobject metrics but got '%v'", le)
	}
	if value := lowerCopyObjects[0].Value; value != 2 {
		t.Errorf("Expected le='0.050' api=copyobject value to be 2 but got '%v'", value)
	}
	if le := lowerCopyObjects[1].VariableLabels["le"]; le != "5.000" {
		t.Errorf("Expected le='5.000' api=copyobject metrics but got '%v'", le)
	}
	if value := lowerCopyObjects[1].Value; value != 2 {
		t.Errorf("Expected le='5.000' api=copyobject value to be 2 but got '%v'", value)
	}
	if le := lowerCopyObjects[2].VariableLabels["le"]; le != "+Inf" {
		t.Errorf("Expected le='+Inf' api=copyobject metrics but got '%v'", le)
	}
	if value := lowerCopyObjects[2].Value; value != 3 {
		t.Errorf("Expected le='+Inf' api=copyobject value to be 3 but got '%v'", value)
	}
}
