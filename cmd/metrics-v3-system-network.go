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

	"github.com/minio/minio/internal/rest"
)

const (
	internodeErrorsTotal      MetricName = "errors_total"
	internodeDialErrorsTotal  MetricName = "dial_errors_total"
	internodeDialAvgTimeNanos MetricName = "dial_avg_time_nanos"
	internodeSentBytesTotal   MetricName = "sent_bytes_total"
	internodeRecvBytesTotal   MetricName = "recv_bytes_total"
)

var (
	internodeErrorsTotalMD = NewCounterMD(internodeErrorsTotal,
		"Total number of failed internode calls")
	internodeDialedErrorsTotalMD = NewCounterMD(internodeDialErrorsTotal,
		"Total number of internode TCP dial timeouts and errors")
	internodeDialAvgTimeNanosMD = NewGaugeMD(internodeDialAvgTimeNanos,
		"Average dial time of internode TCP calls in nanoseconds")
	internodeSentBytesTotalMD = NewCounterMD(internodeSentBytesTotal,
		"Total number of bytes sent to other peer nodes")
	internodeRecvBytesTotalMD = NewCounterMD(internodeRecvBytesTotal,
		"Total number of bytes received from other peer nodes")
)

// loadNetworkInternodeMetrics - reads internode network metrics.
//
// This is a `MetricsLoaderFn`.
func loadNetworkInternodeMetrics(ctx context.Context, m MetricValues, _ *metricsCache) error {
	connStats := globalConnStats.toServerConnStats()
	rpcStats := rest.GetRPCStats()
	if globalIsDistErasure {
		m.Set(internodeErrorsTotal, float64(rpcStats.Errs))
		m.Set(internodeDialErrorsTotal, float64(rpcStats.DialErrs))
		m.Set(internodeDialAvgTimeNanos, float64(rpcStats.DialAvgDuration))
		m.Set(internodeSentBytesTotal, float64(connStats.internodeOutputBytes))
		m.Set(internodeRecvBytesTotal, float64(connStats.internodeInputBytes))
	}
	return nil
}
