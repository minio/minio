// Copyright (c) 2024 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
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
	notificationCurrentSendInProgress = "current_send_in_progress"
	notificationEventsErrorsTotal     = "events_errors_total"
	notificationEventsSentTotal       = "events_sent_total"
	notificationEventsSkippedTotal    = "events_skipped_total"
)

var (
	notificationCurrentSendInProgressMD = NewCounterMD(notificationCurrentSendInProgress, "Number of concurrent async Send calls active to all targets")
	notificationEventsErrorsTotalMD     = NewCounterMD(notificationEventsErrorsTotal, "Events that were failed to be sent to the targets")
	notificationEventsSentTotalMD       = NewCounterMD(notificationEventsSentTotal, "Total number of events sent to the targets")
	notificationEventsSkippedTotalMD    = NewCounterMD(notificationEventsSkippedTotal, "Events that were skipped to be sent to the targets due to the in-memory queue being full")
)

// loadClusterNotificationMetrics - `MetricsLoaderFn` for cluster notification metrics.
func loadClusterNotificationMetrics(_ context.Context, m MetricValues, _ *metricsCache) error {
	if globalEventNotifier == nil {
		return nil
	}

	nstats := globalEventNotifier.targetList.Stats()
	m.Set(notificationCurrentSendInProgress, float64(nstats.CurrentSendCalls))
	m.Set(notificationEventsErrorsTotal, float64(nstats.EventsErrorsTotal))
	m.Set(notificationEventsSentTotal, float64(nstats.TotalEvents))
	m.Set(notificationEventsSkippedTotal, float64(nstats.EventsSkipped))

	return nil
}
