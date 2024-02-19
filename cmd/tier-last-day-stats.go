// Copyright (c) 2022 MinIO, Inc.
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
	"time"

	"github.com/minio/madmin-go/v3"
)

//go:generate msgp -file=$GOFILE -unexported

type lastDayTierStats struct {
	Bins      [24]tierStats
	UpdatedAt time.Time
}

func (l *lastDayTierStats) addStats(ts tierStats) {
	now := time.Now()
	l.forwardTo(now)

	nowIdx := now.Hour()
	l.Bins[nowIdx] = l.Bins[nowIdx].add(ts)
}

// forwardTo moves time to t, clearing entries between last update and t.
func (l *lastDayTierStats) forwardTo(t time.Time) {
	if t.IsZero() {
		t = time.Now()
	}

	since := t.Sub(l.UpdatedAt).Hours()
	// within the hour since l.UpdatedAt
	if since < 1 {
		return
	}

	idx, lastIdx := t.Hour(), l.UpdatedAt.Hour()

	l.UpdatedAt = t // update to the latest time index

	if since >= 24 {
		l.Bins = [24]tierStats{}
		return
	}

	for lastIdx != idx {
		lastIdx = (lastIdx + 1) % 24
		l.Bins[lastIdx] = tierStats{}
	}
}

func (l *lastDayTierStats) clone() lastDayTierStats {
	clone := lastDayTierStats{
		UpdatedAt: l.UpdatedAt,
	}
	copy(clone.Bins[:], l.Bins[:])
	return clone
}

func (l lastDayTierStats) merge(m lastDayTierStats) (merged lastDayTierStats) {
	cl := l.clone()
	cm := m.clone()

	if cl.UpdatedAt.After(cm.UpdatedAt) {
		cm.forwardTo(cl.UpdatedAt)
		merged.UpdatedAt = cl.UpdatedAt
	} else {
		cl.forwardTo(cm.UpdatedAt)
		merged.UpdatedAt = cm.UpdatedAt
	}

	for i := range cl.Bins {
		merged.Bins[i] = cl.Bins[i].add(cm.Bins[i])
	}

	return merged
}

// DailyAllTierStats is used to aggregate last day tier stats across MinIO servers
type DailyAllTierStats map[string]lastDayTierStats

func (l DailyAllTierStats) merge(m DailyAllTierStats) {
	for tier, st := range m {
		l[tier] = l[tier].merge(st)
	}
}

func (l DailyAllTierStats) addToTierInfo(tierInfos []madmin.TierInfo) []madmin.TierInfo {
	for i := range tierInfos {
		lst, ok := l[tierInfos[i].Name]
		if !ok {
			continue
		}
		for hr, st := range lst.Bins {
			tierInfos[i].DailyStats.Bins[hr] = madmin.TierStats{
				TotalSize:   st.TotalSize,
				NumVersions: st.NumVersions,
				NumObjects:  st.NumObjects,
			}
		}
		tierInfos[i].DailyStats.UpdatedAt = lst.UpdatedAt
	}
	return tierInfos
}
