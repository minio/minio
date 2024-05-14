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
	webhookQueueLength    = "queue_length"
	webhookTotalMessages  = "total_messages"
	webhookFailedMessages = "failed_messages"
	nameL                 = "name"
	endpointL             = "endpoint"
)

var (
	allWebhookLabels        = []string{nameL, endpointL}
	webhookFailedMessagesMD = NewCounterMD(webhookFailedMessages,
		"Number of messages that failed to send",
		allWebhookLabels...)
	webhookQueueLengthMD = NewGaugeMD(webhookQueueLength,
		"Webhook queue length",
		allWebhookLabels...)
	webhookTotalMessagesMD = NewCounterMD(webhookTotalMessages,
		"Total number of messages sent to this target",
		allWebhookLabels...)
)

// loadLoggerWebhookMetrics - `MetricsLoaderFn` for logger webhook
// such as failed messages and total messages.
func loadLoggerWebhookMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	tgts := append(logger.SystemTargets(), logger.AuditTargets()...)
	for _, t := range tgts {
		labels := []string{nameL, t.String(), endpointL, t.Endpoint()}
		m.Set(webhookFailedMessages, float64(t.Stats().FailedMessages), labels...)
		m.Set(webhookQueueLength, float64(t.Stats().QueueLength), labels...)
		m.Set(webhookTotalMessages, float64(t.Stats().TotalMessages), labels...)
	}

	return nil
}
