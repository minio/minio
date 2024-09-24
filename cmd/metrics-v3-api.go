// Copyright (c) 2015-2024 MinIO, Inc.
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
	"context"

	"github.com/minio/minio-go/v7/pkg/set"
)

const (
	apiRejectedAuthTotal      MetricName = "rejected_auth_total"
	apiRejectedHeaderTotal    MetricName = "rejected_header_total"
	apiRejectedTimestampTotal MetricName = "rejected_timestamp_total"
	apiRejectedInvalidTotal   MetricName = "rejected_invalid_total"

	apiRequestsWaitingTotal  MetricName = "waiting_total"
	apiRequestsIncomingTotal MetricName = "incoming_total"

	apiRequestsInFlightTotal  MetricName = "inflight_total"
	apiRequestsTotal          MetricName = "total"
	apiRequestsErrorsTotal    MetricName = "errors_total"
	apiRequests5xxErrorsTotal MetricName = "5xx_errors_total"
	apiRequests4xxErrorsTotal MetricName = "4xx_errors_total"
	apiRequestsCanceledTotal  MetricName = "canceled_total"

	apiRequestsTTFBSecondsDistribution MetricName = "ttfb_seconds_distribution"

	apiTrafficSentBytes MetricName = "traffic_sent_bytes"
	apiTrafficRecvBytes MetricName = "traffic_received_bytes"
)

var (
	apiRejectedAuthTotalMD = NewCounterMD(apiRejectedAuthTotal,
		"Total number of requests rejected for auth failure", "type")
	apiRejectedHeaderTotalMD = NewCounterMD(apiRejectedHeaderTotal,
		"Total number of requests rejected for invalid header", "type")
	apiRejectedTimestampTotalMD = NewCounterMD(apiRejectedTimestampTotal,
		"Total number of requests rejected for invalid timestamp", "type")
	apiRejectedInvalidTotalMD = NewCounterMD(apiRejectedInvalidTotal,
		"Total number of invalid requests", "type")

	apiRequestsWaitingTotalMD = NewGaugeMD(apiRequestsWaitingTotal,
		"Total number of requests in the waiting queue", "type")
	apiRequestsIncomingTotalMD = NewGaugeMD(apiRequestsIncomingTotal,
		"Total number of incoming requests", "type")

	apiRequestsInFlightTotalMD = NewGaugeMD(apiRequestsInFlightTotal,
		"Total number of requests currently in flight", "name", "type")
	apiRequestsTotalMD = NewCounterMD(apiRequestsTotal,
		"Total number of requests", "name", "type")
	apiRequestsErrorsTotalMD = NewCounterMD(apiRequestsErrorsTotal,
		"Total number of requests with (4xx and 5xx) errors", "name", "type")
	apiRequests5xxErrorsTotalMD = NewCounterMD(apiRequests5xxErrorsTotal,
		"Total number of requests with 5xx errors", "name", "type")
	apiRequests4xxErrorsTotalMD = NewCounterMD(apiRequests4xxErrorsTotal,
		"Total number of requests with 4xx errors", "name", "type")
	apiRequestsCanceledTotalMD = NewCounterMD(apiRequestsCanceledTotal,
		"Total number of requests canceled by the client", "name", "type")

	apiRequestsTTFBSecondsDistributionMD = NewCounterMD(apiRequestsTTFBSecondsDistribution,
		"Distribution of time to first byte across API calls", "name", "type", "le")

	apiTrafficSentBytesMD = NewCounterMD(apiTrafficSentBytes,
		"Total number of bytes sent", "type")
	apiTrafficRecvBytesMD = NewCounterMD(apiTrafficRecvBytes,
		"Total number of bytes received", "type")
)

// loadAPIRequestsHTTPMetrics - reads S3 HTTP metrics.
//
// This is a `MetricsLoaderFn`.
//
// This includes node level S3 HTTP metrics.
//
// This function currently ignores `opts`.
func loadAPIRequestsHTTPMetrics(ctx context.Context, m MetricValues, _ *metricsCache) error {
	// Collect node level S3 HTTP metrics.
	httpStats := globalHTTPStats.toServerHTTPStats(false)

	// Currently we only collect S3 API related stats, so we set the "type"
	// label to "s3".

	m.Set(apiRejectedAuthTotal, float64(httpStats.TotalS3RejectedAuth), "type", "s3")
	m.Set(apiRejectedTimestampTotal, float64(httpStats.TotalS3RejectedTime), "type", "s3")
	m.Set(apiRejectedHeaderTotal, float64(httpStats.TotalS3RejectedHeader), "type", "s3")
	m.Set(apiRejectedInvalidTotal, float64(httpStats.TotalS3RejectedInvalid), "type", "s3")
	m.Set(apiRequestsWaitingTotal, float64(httpStats.S3RequestsInQueue), "type", "s3")
	m.Set(apiRequestsIncomingTotal, float64(httpStats.S3RequestsIncoming), "type", "s3")

	for name, value := range httpStats.CurrentS3Requests.APIStats {
		m.Set(apiRequestsInFlightTotal, float64(value), "name", name, "type", "s3")
	}
	for name, value := range httpStats.TotalS3Requests.APIStats {
		m.Set(apiRequestsTotal, float64(value), "name", name, "type", "s3")
	}
	for name, value := range httpStats.TotalS3Errors.APIStats {
		m.Set(apiRequestsErrorsTotal, float64(value), "name", name, "type", "s3")
	}
	for name, value := range httpStats.TotalS35xxErrors.APIStats {
		m.Set(apiRequests5xxErrorsTotal, float64(value), "name", name, "type", "s3")
	}
	for name, value := range httpStats.TotalS34xxErrors.APIStats {
		m.Set(apiRequests4xxErrorsTotal, float64(value), "name", name, "type", "s3")
	}
	for name, value := range httpStats.TotalS3Canceled.APIStats {
		m.Set(apiRequestsCanceledTotal, float64(value), "name", name, "type", "s3")
	}
	return nil
}

// loadAPIRequestsTTFBMetrics - loads S3 TTFB metrics.
//
// This is a `MetricsLoaderFn`.
func loadAPIRequestsTTFBMetrics(ctx context.Context, m MetricValues, _ *metricsCache) error {
	renameLabels := map[string]string{"api": "name"}
	labelsFilter := map[string]set.StringSet{}
	m.SetHistogram(apiRequestsTTFBSecondsDistribution, httpRequestsDuration, labelsFilter, renameLabels, nil,
		"type", "s3")
	return nil
}

// loadAPIRequestsNetworkMetrics - loads S3 network metrics.
//
// This is a `MetricsLoaderFn`.
func loadAPIRequestsNetworkMetrics(ctx context.Context, m MetricValues, _ *metricsCache) error {
	connStats := globalConnStats.toServerConnStats()
	m.Set(apiTrafficSentBytes, float64(connStats.s3OutputBytes), "type", "s3")
	m.Set(apiTrafficRecvBytes, float64(connStats.s3InputBytes), "type", "s3")
	return nil
}

// Metric Descriptions for bucket level S3 metrics.
var (
	bucketAPITrafficSentBytesMD = NewCounterMD(apiTrafficSentBytes,
		"Total number of bytes received for a bucket", "bucket", "type")
	bucketAPITrafficRecvBytesMD = NewCounterMD(apiTrafficRecvBytes,
		"Total number of bytes sent for a bucket", "bucket", "type")

	bucketAPIRequestsInFlightMD = NewGaugeMD(apiRequestsInFlightTotal,
		"Total number of requests currently in flight for a bucket", "bucket", "name", "type")
	bucketAPIRequestsTotalMD = NewCounterMD(apiRequestsTotal,
		"Total number of requests for a bucket", "bucket", "name", "type")
	bucketAPIRequestsCanceledMD = NewCounterMD(apiRequestsCanceledTotal,
		"Total number of requests canceled by the client for a bucket", "bucket", "name", "type")
	bucketAPIRequests4xxErrorsMD = NewCounterMD(apiRequests4xxErrorsTotal,
		"Total number of requests with 4xx errors for a bucket", "bucket", "name", "type")
	bucketAPIRequests5xxErrorsMD = NewCounterMD(apiRequests5xxErrorsTotal,
		"Total number of requests with 5xx errors for a bucket", "bucket", "name", "type")

	bucketAPIRequestsTTFBSecondsDistributionMD = NewCounterMD(apiRequestsTTFBSecondsDistribution,
		"Distribution of time to first byte across API calls for a bucket",
		"bucket", "name", "le", "type")
)

// loadBucketAPIHTTPMetrics - loads bucket level S3 HTTP metrics.
//
// This is a `MetricsLoaderFn`.
//
// This includes bucket level S3 HTTP metrics and S3 network in/out metrics.
func loadBucketAPIHTTPMetrics(ctx context.Context, m MetricValues, _ *metricsCache, buckets []string) error {
	if len(buckets) == 0 {
		return nil
	}
	for bucket, inOut := range globalBucketConnStats.getBucketS3InOutBytes(buckets) {
		recvBytes := inOut.In
		if recvBytes > 0 {
			m.Set(apiTrafficSentBytes, float64(recvBytes), "bucket", bucket, "type", "s3")
		}
		sentBytes := inOut.Out
		if sentBytes > 0 {
			m.Set(apiTrafficRecvBytes, float64(sentBytes), "bucket", bucket, "type", "s3")
		}

		httpStats := globalBucketHTTPStats.load(bucket)
		for k, v := range httpStats.currentS3Requests.Load(false) {
			m.Set(apiRequestsInFlightTotal, float64(v), "bucket", bucket, "name", k, "type", "s3")
		}

		for k, v := range httpStats.totalS3Requests.Load(false) {
			m.Set(apiRequestsTotal, float64(v), "bucket", bucket, "name", k, "type", "s3")
		}

		for k, v := range httpStats.totalS3Canceled.Load(false) {
			m.Set(apiRequestsCanceledTotal, float64(v), "bucket", bucket, "name", k, "type", "s3")
		}

		for k, v := range httpStats.totalS34xxErrors.Load(false) {
			m.Set(apiRequests4xxErrorsTotal, float64(v), "bucket", bucket, "name", k, "type", "s3")
		}

		for k, v := range httpStats.totalS35xxErrors.Load(false) {
			m.Set(apiRequests5xxErrorsTotal, float64(v), "bucket", bucket, "name", k, "type", "s3")
		}
	}

	return nil
}

// loadBucketAPITTFBMetrics - loads bucket S3 TTFB metrics.
//
// This is a `MetricsLoaderFn`.
func loadBucketAPITTFBMetrics(ctx context.Context, m MetricValues, _ *metricsCache, buckets []string) error {
	renameLabels := map[string]string{"api": "name"}
	labelsFilter := map[string]set.StringSet{}
	m.SetHistogram(apiRequestsTTFBSecondsDistribution, bucketHTTPRequestsDuration, labelsFilter, renameLabels,
		buckets, "type", "s3")
	return nil
}
