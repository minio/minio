// Copyright (c) 2024 MinIO, Inc.
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
	"sync/atomic"
	"time"
)

const (
	lastSyncDurationMillis                 = "last_sync_duration_millis"
	pluginAuthnServiceFailedRequestsMinute = "plugin_authn_service_failed_requests_minute"
	pluginAuthnServiceLastFailSeconds      = "plugin_authn_service_last_fail_seconds"
	pluginAuthnServiceLastSuccSeconds      = "plugin_authn_service_last_succ_seconds"
	pluginAuthnServiceSuccAvgRttMsMinute   = "plugin_authn_service_succ_avg_rtt_ms_minute"
	pluginAuthnServiceSuccMaxRttMsMinute   = "plugin_authn_service_succ_max_rtt_ms_minute"
	pluginAuthnServiceTotalRequestsMinute  = "plugin_authn_service_total_requests_minute"
	sinceLastSyncMillis                    = "since_last_sync_millis"
	syncFailures                           = "sync_failures"
	syncSuccesses                          = "sync_successes"
)

var (
	lastSyncDurationMillisMD                 = NewCounterMD(lastSyncDurationMillis, "Last successful IAM data sync duration in milliseconds")
	pluginAuthnServiceFailedRequestsMinuteMD = NewCounterMD(pluginAuthnServiceFailedRequestsMinute, "When plugin authentication is configured, returns failed requests count in the last full minute")
	pluginAuthnServiceLastFailSecondsMD      = NewCounterMD(pluginAuthnServiceLastFailSeconds, "When plugin authentication is configured, returns time (in seconds) since the last failed request to the service")
	pluginAuthnServiceLastSuccSecondsMD      = NewCounterMD(pluginAuthnServiceLastSuccSeconds, "When plugin authentication is configured, returns time (in seconds) since the last successful request to the service")
	pluginAuthnServiceSuccAvgRttMsMinuteMD   = NewCounterMD(pluginAuthnServiceSuccAvgRttMsMinute, "When plugin authentication is configured, returns average round-trip-time of successful requests in the last full minute")
	pluginAuthnServiceSuccMaxRttMsMinuteMD   = NewCounterMD(pluginAuthnServiceSuccMaxRttMsMinute, "When plugin authentication is configured, returns maximum round-trip-time of successful requests in the last full minute")
	pluginAuthnServiceTotalRequestsMinuteMD  = NewCounterMD(pluginAuthnServiceTotalRequestsMinute, "When plugin authentication is configured, returns total requests count in the last full minute")
	sinceLastSyncMillisMD                    = NewCounterMD(sinceLastSyncMillis, "Time (in milliseconds) since last successful IAM data sync.")
	syncFailuresMD                           = NewCounterMD(syncFailures, "Number of failed IAM data syncs since server start.")
	syncSuccessesMD                          = NewCounterMD(syncSuccesses, "Number of successful IAM data syncs since server start.")
)

// loadClusterIAMMetrics - `MetricsLoaderFn` for cluster IAM metrics.
func loadClusterIAMMetrics(_ context.Context, m MetricValues, _ *metricsCache) error {
	m.Set(lastSyncDurationMillis, float64(atomic.LoadUint64(&globalIAMSys.LastRefreshDurationMilliseconds)))
	pluginAuthNMetrics := globalAuthNPlugin.Metrics()
	m.Set(pluginAuthnServiceFailedRequestsMinute, float64(pluginAuthNMetrics.FailedRequests))
	m.Set(pluginAuthnServiceLastFailSeconds, pluginAuthNMetrics.LastUnreachableSecs)
	m.Set(pluginAuthnServiceLastSuccSeconds, pluginAuthNMetrics.LastReachableSecs)
	m.Set(pluginAuthnServiceSuccAvgRttMsMinute, pluginAuthNMetrics.AvgSuccRTTMs)
	m.Set(pluginAuthnServiceSuccMaxRttMsMinute, pluginAuthNMetrics.MaxSuccRTTMs)
	m.Set(pluginAuthnServiceTotalRequestsMinute, float64(pluginAuthNMetrics.TotalRequests))
	lastSyncTime := atomic.LoadUint64(&globalIAMSys.LastRefreshTimeUnixNano)
	if lastSyncTime != 0 {
		m.Set(sinceLastSyncMillis, float64((uint64(time.Now().UnixNano())-lastSyncTime)/uint64(time.Millisecond)))
	}
	m.Set(syncFailures, float64(atomic.LoadUint64(&globalIAMSys.TotalRefreshFailures)))
	m.Set(syncSuccesses, float64(atomic.LoadUint64(&globalIAMSys.TotalRefreshSuccesses)))
	return nil
}
