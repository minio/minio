package cmd

import "time"

type lastDayTierStats struct {
	bins      [24]tierStats
	updatedAt time.Time
}

func (l *lastDayTierStats) addStats(ts tierStats) {
	now := time.Now()
	l.forwardTo(now)

	nowIdx := now.Hour() % 24
	l.bins[nowIdx] = l.bins[nowIdx].add(ts)
}

func (l *lastDayTierStats) forwardTo(t time.Time) {
	since := t.Sub(l.updatedAt)
	if since <= 0 {
		return
	}

	idx, lastIdx := t.Hour(), l.updatedAt.Hour()
	l.updatedAt = t

	if since.Hours() >= 24 {
		l.bins = [24]tierStats{}
		return
	}

	// clear out entries between lastIdx and now
	for ; lastIdx != idx; lastIdx++ {
		l.bins[(lastIdx+1)%24] = tierStats{}
	}
}

func (l *lastDayTierStats) clone() lastDayTierStats {
	clone := lastDayTierStats{
		updatedAt: l.updatedAt,
	}
	for i := range l.bins {
		clone.bins[i] = l.bins[i]
	}
	return clone
}

func (l lastDayTierStats) merge(m lastDayTierStats) (merged lastDayTierStats) {
	cl := l.clone()
	cm := m.clone()

	if cl.updatedAt.Sub(cm.updatedAt) >= 0 {
		cm.forwardTo(cl.updatedAt)
		merged.updatedAt = cl.updatedAt
	} else {
		cl.forwardTo(cm.updatedAt)
		merged.updatedAt = cm.updatedAt
	}

	for i := range cl.bins {
		merged.bins[i] = cl.bins[i].add(cm.bins[i])
	}

	return merged
}
