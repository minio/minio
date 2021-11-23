package cmd

import (
	"time"

	"github.com/minio/madmin-go"
)

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
	since := t.Sub(l.UpdatedAt).Hours()
	// within the hour since l.UpdatedAt
	if since < 1 {
		return
	}

	idx, lastIdx := t.Hour(), l.UpdatedAt.Hour()
	l.UpdatedAt = t

	if since >= 24 {
		l.Bins = [24]tierStats{}
		return
	}

	for ; lastIdx != idx; lastIdx++ {
		l.Bins[(lastIdx+1)%24] = tierStats{}
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

// dailyAllTierStats is used to aggregate last day tier stats across MinIO servers
type dailyAllTierStats map[string]lastDayTierStats

func (l dailyAllTierStats) merge(m dailyAllTierStats) {
	for tier, st := range m {
		l[tier] = l[tier].merge(st)
	}
}

func (l dailyAllTierStats) addToTierInfo(tierInfos []madmin.TierInfo) []madmin.TierInfo {
	for i := range tierInfos {
		var lst lastDayTierStats
		var ok bool
		if lst, ok = l[tierInfos[i].Name]; !ok {
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
