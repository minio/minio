package hostdb

import (
	"math"

	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
)

// updateHostDBEntry updates a HostDBEntries's historic interactions if more
// than one block passed since the last update. This should be called every time
// before the recent interactions are updated.  if passedTime is e.g. 10, this
// means that the recent interactions were updated 10 blocks ago but never
// since. So we need to apply the decay of 1 block before we append the recent
// interactions from 10 blocks ago and then apply the decay of 9 more blocks in
// which the recent interactions have been 0
func updateHostHistoricInteractions(host *modules.HostDBEntry, bh types.BlockHeight) {
	// Check that the last historic update was not in the future.
	if host.LastHistoricUpdate >= bh {
		// The hostdb may be performing a rescan, or maybe no time has passed
		// since the last update, so there is nothing to do.
		return
	}
	passedTime := bh - host.LastHistoricUpdate

	// tmp float64 values for more accurate decay
	hsi := host.HistoricSuccessfulInteractions
	hfi := host.HistoricFailedInteractions

	// Apply the decay of a single block.
	decay := historicInteractionDecay
	hsi *= decay
	hfi *= decay

	// Apply the recent interactions of that single block. Recent interactions
	// cannot represent more than recentInteractionWeightLimit of historic
	// interactions, unless there are less than historicInteractionDecayLimit
	// total interactions, and then the recent interactions cannot count for
	// more than recentInteractionWeightLimit of the decay limit.
	rsi := float64(host.RecentSuccessfulInteractions)
	rfi := float64(host.RecentFailedInteractions)
	if hsi+hfi > historicInteractionDecayLimit {
		if rsi+rfi > recentInteractionWeightLimit*(hsi+hfi) {
			adjustment := recentInteractionWeightLimit * (hsi + hfi) / (rsi + rfi)
			rsi *= adjustment
			rfi *= adjustment
		}
	} else {
		if rsi+rfi > recentInteractionWeightLimit*historicInteractionDecayLimit {
			adjustment := recentInteractionWeightLimit * historicInteractionDecayLimit / (rsi + rfi)
			rsi *= adjustment
			rfi *= adjustment
		}
	}
	hsi += rsi
	hfi += rfi

	// Apply the decay of the rest of the blocks
	if passedTime > 1 && hsi+hfi > historicInteractionDecayLimit {
		decay := math.Pow(historicInteractionDecay, float64(passedTime-1))
		hsi *= decay
		hfi *= decay
	}

	// Set new values
	host.HistoricSuccessfulInteractions = hsi
	host.HistoricFailedInteractions = hfi
	host.RecentSuccessfulInteractions = 0
	host.RecentFailedInteractions = 0

	// Update the time of the last update
	host.LastHistoricUpdate = bh
}

// IncrementSuccessfulInteractions increments the number of successful
// interactions with a host for a given key
func (hdb *HostDB) IncrementSuccessfulInteractions(key types.SiaPublicKey) {
	hdb.mu.Lock()
	defer hdb.mu.Unlock()

	// Fetch the host.
	host, haveHost := hdb.hostTree.Select(key)
	if !haveHost {
		return
	}

	// Update historic values if necessary
	updateHostHistoricInteractions(&host, hdb.blockHeight)

	// Increment the successful interactions
	host.RecentSuccessfulInteractions++
	hdb.hostTree.Modify(host)
}

// IncrementFailedInteractions increments the number of failed interactions with
// a host for a given key
func (hdb *HostDB) IncrementFailedInteractions(key types.SiaPublicKey) {
	hdb.mu.Lock()
	defer hdb.mu.Unlock()

	// Fetch the host.
	host, haveHost := hdb.hostTree.Select(key)
	if !haveHost || !hdb.online {
		// If we are offline it probably wasn't the host's fault
		return
	}

	// Update historic values if necessary
	updateHostHistoricInteractions(&host, hdb.blockHeight)

	// Increment the failed interactions
	host.RecentFailedInteractions++
	hdb.hostTree.Modify(host)
}
