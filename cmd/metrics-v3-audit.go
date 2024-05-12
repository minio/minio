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

	"github.com/minio/minio/internal/logger"
)

const (
	auditFailedMessages    = "failed_messages"
	auditTargetQueueLength = "target_queue_length"
	auditTotalMessages     = "total_messages"
	targetID               = "target_id"
)

var (
	auditFailedMessagesMD = NewCounterMD(auditFailedMessages,
		"Total number of messages that failed to send since start",
		targetID)
	auditTargetQueueLengthMD = NewGaugeMD(auditTargetQueueLength,
		"Number of unsent messages in queue for target",
		targetID)
	auditTotalMessagesMD = NewCounterMD(auditTotalMessages,
		"Total number of messages sent since start",
		targetID)
)

// loadAuditMetrics - `MetricsLoaderFn` for audit
// such as failed messages and total messages.
func loadAuditMetrics(_ context.Context, m MetricValues, c *metricsCache) error {
	audit := logger.CurrentStats()
	for id, st := range audit {
		labels := []string{targetID, id}
		m.Set(auditFailedMessages, float64(st.FailedMessages), labels...)
		m.Set(auditTargetQueueLength, float64(st.QueueLength), labels...)
		m.Set(auditTotalMessages, float64(st.TotalMessages), labels...)
	}

	return nil
}
