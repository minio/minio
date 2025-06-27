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
)

const (
	bucketReplLastHrFailedBytes                    = "last_hour_failed_bytes"
	bucketReplLastHrFailedCount                    = "last_hour_failed_count"
	bucketReplLastMinFailedBytes                   = "last_minute_failed_bytes"
	bucketReplLastMinFailedCount                   = "last_minute_failed_count"
	bucketReplLatencyMs                            = "latency_ms"
	bucketReplProxiedDeleteTaggingRequestsTotal    = "proxied_delete_tagging_requests_total"
	bucketReplProxiedGetRequestsFailures           = "proxied_get_requests_failures"
	bucketReplProxiedGetRequestsTotal              = "proxied_get_requests_total"
	bucketReplProxiedGetTaggingRequestsFailures    = "proxied_get_tagging_requests_failures"
	bucketReplProxiedGetTaggingRequestsTotal       = "proxied_get_tagging_requests_total"
	bucketReplProxiedHeadRequestsFailures          = "proxied_head_requests_failures"
	bucketReplProxiedHeadRequestsTotal             = "proxied_head_requests_total"
	bucketReplProxiedPutTaggingRequestsFailures    = "proxied_put_tagging_requests_failures"
	bucketReplProxiedPutTaggingRequestsTotal       = "proxied_put_tagging_requests_total"
	bucketReplSentBytes                            = "sent_bytes"
	bucketReplSentCount                            = "sent_count"
	bucketReplTotalFailedBytes                     = "total_failed_bytes"
	bucketReplTotalFailedCount                     = "total_failed_count"
	bucketReplProxiedDeleteTaggingRequestsFailures = "proxied_delete_tagging_requests_failures"
	bucketL                                        = "bucket"
	operationL                                     = "operation"
	targetArnL                                     = "targetArn"
)

var (
	bucketReplLastHrFailedBytesMD = NewGaugeMD(bucketReplLastHrFailedBytes,
		"Total number of bytes failed at least once to replicate in the last hour on a bucket",
		bucketL, targetArnL)
	bucketReplLastHrFailedCountMD = NewGaugeMD(bucketReplLastHrFailedCount,
		"Total number of objects which failed replication in the last hour on a bucket",
		bucketL, targetArnL)
	bucketReplLastMinFailedBytesMD = NewGaugeMD(bucketReplLastMinFailedBytes,
		"Total number of bytes failed at least once to replicate in the last full minute on a bucket",
		bucketL, targetArnL)
	bucketReplLastMinFailedCountMD = NewGaugeMD(bucketReplLastMinFailedCount,
		"Total number of objects which failed replication in the last full minute on a bucket",
		bucketL, targetArnL)
	bucketReplLatencyMsMD = NewGaugeMD(bucketReplLatencyMs,
		"Replication latency on a bucket in milliseconds",
		bucketL, operationL, rangeL, targetArnL)
	bucketReplProxiedDeleteTaggingRequestsTotalMD = NewCounterMD(bucketReplProxiedDeleteTaggingRequestsTotal,
		"Number of DELETE tagging requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplProxiedGetRequestsFailuresMD = NewCounterMD(bucketReplProxiedGetRequestsFailures,
		"Number of failures in GET requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplProxiedGetRequestsTotalMD = NewCounterMD(bucketReplProxiedGetRequestsTotal,
		"Number of GET requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplProxiedGetTaggingRequestsFailuresMD = NewCounterMD(bucketReplProxiedGetTaggingRequestsFailures,
		"Number of failures in GET tagging requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplProxiedGetTaggingRequestsTotalMD = NewCounterMD(bucketReplProxiedGetTaggingRequestsTotal,
		"Number of GET tagging requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplProxiedHeadRequestsFailuresMD = NewCounterMD(bucketReplProxiedHeadRequestsFailures,
		"Number of failures in HEAD requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplProxiedHeadRequestsTotalMD = NewCounterMD(bucketReplProxiedHeadRequestsTotal,
		"Number of HEAD requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplProxiedPutTaggingRequestsFailuresMD = NewCounterMD(bucketReplProxiedPutTaggingRequestsFailures,
		"Number of failures in PUT tagging requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplProxiedPutTaggingRequestsTotalMD = NewCounterMD(bucketReplProxiedPutTaggingRequestsTotal,
		"Number of PUT tagging requests proxied to replication target",
		bucketL, targetArnL)
	bucketReplSentBytesMD = NewCounterMD(bucketReplSentBytes,
		"Total number of bytes replicated to the target",
		bucketL, targetArnL)
	bucketReplSentCountMD = NewCounterMD(bucketReplSentCount,
		"Total number of objects replicated to the target",
		bucketL, targetArnL)
	bucketReplTotalFailedBytesMD = NewCounterMD(bucketReplTotalFailedBytes,
		"Total number of bytes failed at least once to replicate since server start",
		bucketL, targetArnL)
	bucketReplTotalFailedCountMD = NewCounterMD(bucketReplTotalFailedCount,
		"Total number of objects which failed replication since server start",
		bucketL, targetArnL)
	bucketReplProxiedDeleteTaggingRequestsFailuresMD = NewCounterMD(bucketReplProxiedDeleteTaggingRequestsFailures,
		"Number of failures in DELETE tagging requests proxied to replication target",
		bucketL, targetArnL)
)

// loadBucketReplicationMetrics - `BucketMetricsLoaderFn` for bucket replication metrics
// such as latency and sent bytes.
func loadBucketReplicationMetrics(ctx context.Context, m MetricValues, c *metricsCache, buckets []string) error {
	if globalSiteReplicationSys.isEnabled() {
		return nil
	}

	dataUsageInfo, err := c.dataUsageInfo.Get()
	if err != nil {
		metricsLogIf(ctx, err)
		return nil
	}

	bucketReplStats := globalReplicationStats.Load().getAllLatest(dataUsageInfo.BucketsUsage)
	for _, bucket := range buckets {
		if s, ok := bucketReplStats[bucket]; ok {
			stats := s.ReplicationStats
			if stats.hasReplicationUsage() {
				for arn, stat := range stats.Stats {
					labels := []string{bucketL, bucket, targetArnL, arn}
					m.Set(bucketReplLastHrFailedBytes, float64(stat.Failed.LastHour.Bytes), labels...)
					m.Set(bucketReplLastHrFailedCount, float64(stat.Failed.LastHour.Count), labels...)
					m.Set(bucketReplLastMinFailedBytes, float64(stat.Failed.LastMinute.Bytes), labels...)
					m.Set(bucketReplLastMinFailedCount, float64(stat.Failed.LastMinute.Count), labels...)
					m.Set(bucketReplProxiedDeleteTaggingRequestsTotal, float64(s.ProxyStats.RmvTagTotal), labels...)
					m.Set(bucketReplProxiedGetRequestsFailures, float64(s.ProxyStats.GetFailedTotal), labels...)
					m.Set(bucketReplProxiedGetRequestsTotal, float64(s.ProxyStats.GetTotal), labels...)
					m.Set(bucketReplProxiedGetTaggingRequestsFailures, float64(s.ProxyStats.GetTagFailedTotal), labels...)
					m.Set(bucketReplProxiedGetTaggingRequestsTotal, float64(s.ProxyStats.GetTagTotal), labels...)
					m.Set(bucketReplProxiedHeadRequestsFailures, float64(s.ProxyStats.HeadFailedTotal), labels...)
					m.Set(bucketReplProxiedHeadRequestsTotal, float64(s.ProxyStats.HeadTotal), labels...)
					m.Set(bucketReplProxiedPutTaggingRequestsFailures, float64(s.ProxyStats.PutTagFailedTotal), labels...)
					m.Set(bucketReplProxiedPutTaggingRequestsTotal, float64(s.ProxyStats.PutTagTotal), labels...)
					m.Set(bucketReplSentCount, float64(stat.ReplicatedCount), labels...)
					m.Set(bucketReplTotalFailedBytes, float64(stat.Failed.Totals.Bytes), labels...)
					m.Set(bucketReplTotalFailedCount, float64(stat.Failed.Totals.Count), labels...)
					m.Set(bucketReplProxiedDeleteTaggingRequestsFailures, float64(s.ProxyStats.RmvTagFailedTotal), labels...)
					m.Set(bucketReplSentBytes, float64(stat.ReplicatedSize), labels...)

					SetHistogramValues(m, bucketReplLatencyMs, stat.Latency.getUploadLatency(), bucketL, bucket, operationL, "upload", targetArnL, arn)
				}
			}
		}
	}

	return nil
}
