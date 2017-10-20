package hostdb

import (
	"time"
)

// online.go performs regular checks against the gateway to see if there is
// internet connectivity. The check is performed by looking for at least one
// non-local peer in the peer list. If there is no internet connectivity, scans
// must be stopped lest we penalize otherwise online hosts.

func (hdb *HostDB) threadedOnlineCheck() {
	err := hdb.tg.Add()
	if err != nil {
		return
	}
	defer hdb.tg.Done()

	for {
		// Every 30 seconds, check the online status and update the online
		// field.
		peers := hdb.gateway.Peers()
		hdb.mu.Lock()
		hdb.online = false
		for _, peer := range peers {
			if !peer.Local {
				hdb.online = true
				break
			}
		}
		hdb.mu.Unlock()
		select {
		case <-time.After(time.Second * 30):
			continue
		case <-hdb.tg.StopChan():
			return
		}
	}
}
