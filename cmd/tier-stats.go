// Copyright (c) 2015-2021 MinIO, Inc.
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
)

//go:generate msgp -file $GOFILE -unexported

//msgp:tuple tieringStats
type tieringStats struct {
	TieredSize uint64 `json:"tieredSize"`
}

func (t tieringStats) Add(u tieringStats) tieringStats {
	var r tieringStats
	r.TieredSize = t.TieredSize + u.TieredSize
	return r
}

type TierStatsCache map[string]tieringStats

func (a TierStatsCache) merge(b TierStatsCache) {
	for tier, stat := range b {
		cur := a[tier]
		a[tier] = cur.Add(stat)
	}
}

var globalTieringStats *NodeTieringStats

//msgp:ignore NodeTieringStats
type NodeTieringStats struct {
	sync.RWMutex
	UsageCache TierStatsCache
	NodeCache  TierStatsCache
}

func (t *NodeTieringStats) GetInitialUsage(tier string) tieringStats {
	t.RLock()
	defer t.RUnlock()

	return t.UsageCache[tier]
}

func (t *NodeTieringStats) Update(tier string, u tieringStats) {
	t.Lock()
	t.NodeCache[tier] = t.NodeCache[tier].Add(u)
	t.Unlock()
}

func (t *NodeTieringStats) Get(tier string) tieringStats {
	t.RLock()
	defer t.RUnlock()

	ts := t.UsageCache[tier]
	ts = ts.Add(t.NodeCache[tier])
	return ts
}

func NewTieringStats(ctx context.Context, objAPI ObjectLayer) *NodeTieringStats {
	st := &NodeTieringStats{
		NodeCache:  make(TierStatsCache),
		UsageCache: make(TierStatsCache),
	}
	dataUsageInfo, err := loadDataUsageFromBackend(ctx, objAPI)
	if err != nil {
		return st
	}

	// data usage has not captured any data yet.
	if dataUsageInfo.LastUpdate.IsZero() {
		return st
	}

	for tier, stat := range dataUsageInfo.TierStats {
		st.UsageCache[tier] = st.UsageCache[tier].Add(tieringStats{TieredSize: stat.TieredSize})
	}

	return st
}
