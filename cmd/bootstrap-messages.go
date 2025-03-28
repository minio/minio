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
	"context"
	"sync"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/pubsub"
)

const bootstrapTraceLimit = 4 << 10

type bootstrapTracer struct {
	mu   sync.RWMutex
	info []madmin.TraceInfo
}

var globalBootstrapTracer = &bootstrapTracer{}

func (bs *bootstrapTracer) Record(info madmin.TraceInfo) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if len(bs.info) > bootstrapTraceLimit {
		return
	}
	bs.info = append(bs.info, info)
}

func (bs *bootstrapTracer) Events() []madmin.TraceInfo {
	traceInfo := make([]madmin.TraceInfo, 0, bootstrapTraceLimit)

	bs.mu.RLock()
	traceInfo = append(traceInfo, bs.info...)
	bs.mu.RUnlock()

	return traceInfo
}

func (bs *bootstrapTracer) Publish(ctx context.Context, trace *pubsub.PubSub[madmin.TraceInfo, madmin.TraceType]) {
	for _, bsEvent := range bs.Events() {
		if bsEvent.Message != "" {
			select {
			case <-ctx.Done():
			default:
				trace.Publish(bsEvent)
			}
		}
	}
}
