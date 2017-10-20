package hostdb

// scan.go contains the functions which periodically scan the list of all hosts
// to see which hosts are online or offline, and to get any updates to the
// settings of the hosts.

import (
	"net"
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/fastrand"
)

// queueScan will add a host to the queue to be scanned.
func (hdb *HostDB) queueScan(entry modules.HostDBEntry) {
	// If this entry is already in the scan pool, can return immediately.
	_, exists := hdb.scanMap[entry.PublicKey.String()]
	if exists {
		return
	}

	// Add the entry to a waitlist, then check if any thread is currently
	// emptying the waitlist. If not, spawn a thread to empty the waitlist.
	hdb.scanMap[entry.PublicKey.String()] = struct{}{}
	hdb.scanList = append(hdb.scanList, entry)
	if hdb.scanWait {
		// Another thread is emptying the scan list, nothing to worry about.
		return
	}

	// Sanity check - the scan map and the scan list should have the same
	// length.
	if build.DEBUG && len(hdb.scanMap) > len(hdb.scanList)+maxScanningThreads {
		hdb.log.Critical("The hostdb scan map has seemingly grown too large:", len(hdb.scanMap), len(hdb.scanList), maxScanningThreads)
	}

	hdb.scanWait = true
	scanPool := make(chan modules.HostDBEntry)

	go func() {
		defer close(scanPool)

		// Nobody is emptying the scan list, volunteer.
		if hdb.tg.Add() != nil {
			// Hostdb is shutting down, don't spin up another thread.  It is
			// okay to leave scanWait set to true as that will not affect
			// shutdown.
			return
		}
		defer hdb.tg.Done()

		for {
			hdb.mu.Lock()
			if len(hdb.scanList) == 0 {
				// Scan list is empty, can exit. Let the world know that nobody
				// is emptying the scan list anymore.
				hdb.scanWait = false
				hdb.mu.Unlock()
				return
			}
			// Get the next host, shrink the scan list.
			entry := hdb.scanList[0]
			hdb.scanList = hdb.scanList[1:]
			delete(hdb.scanMap, entry.PublicKey.String())
			scansRemaining := len(hdb.scanList)

			// Grab the most recent entry for this host.
			recentEntry, exists := hdb.hostTree.Select(entry.PublicKey)
			if exists {
				entry = recentEntry
			}

			// Create new worker thread
			if hdb.scanningThreads < maxScanningThreads {
				hdb.scanningThreads++
				go func() {
					hdb.threadedProbeHosts(scanPool)
					hdb.mu.Lock()
					hdb.scanningThreads--
					hdb.mu.Unlock()
				}()
			}
			hdb.mu.Unlock()

			// Block while waiting for an opening in the scan pool.
			hdb.log.Debugf("Sending host %v for scan, %v hosts remain", entry.PublicKey.String(), scansRemaining)
			select {
			case scanPool <- entry:
				// iterate again
			case <-hdb.tg.StopChan():
				// quit
				return
			}
		}
	}()
}

// updateEntry updates an entry in the hostdb after a scan has taken place.
//
// CAUTION: This function will automatically add multiple entries to a new host
// to give that host some base uptime. This makes this function co-dependent
// with the host weight functions. Adjustment of the host weight functions need
// to keep this function in mind, and vice-versa.
func (hdb *HostDB) updateEntry(entry modules.HostDBEntry, netErr error) {
	// If the scan failed because we don't have Internet access, toss out this update.
	if netErr != nil && !hdb.online {
		return
	}

	// Grab the host from the host tree.
	newEntry, exists := hdb.hostTree.Select(entry.PublicKey)
	if exists {
		newEntry.HostExternalSettings = entry.HostExternalSettings
	} else {
		newEntry = entry
	}

	// Add the datapoints for the scan.
	if len(newEntry.ScanHistory) < 2 {
		// Add two scans to the scan history. Two are needed because the scans
		// are forward looking, but we want this first scan to represent as
		// much as one week of uptime or downtime.
		earliestStartTime := time.Now().Add(time.Hour * 7 * 24 * -1)                                                   // Permit up to a week of starting uptime or downtime.
		suggestedStartTime := time.Now().Add(time.Minute * 10 * time.Duration(hdb.blockHeight-entry.FirstSeen+1) * -1) // Add one to the FirstSeen in case FirstSeen is this block, guarantees incrementing order.
		if suggestedStartTime.Before(earliestStartTime) {
			suggestedStartTime = earliestStartTime
		}
		newEntry.ScanHistory = modules.HostDBScans{
			{Timestamp: suggestedStartTime, Success: netErr == nil},
			{Timestamp: time.Now(), Success: netErr == nil},
		}
	} else {
		if newEntry.ScanHistory[len(newEntry.ScanHistory)-1].Success && netErr != nil {
			hdb.log.Debugf("Host %v is being downgraded from an online host to an offline host: %v\n", newEntry.PublicKey.String(), netErr)
		}

		// Make sure that the current time is after the timestamp of the
		// previous scan. It may not be if the system clock has changed. This
		// will prevent the sort-check sanity checks from triggering.
		newTimestamp := time.Now()
		prevTimestamp := newEntry.ScanHistory[len(newEntry.ScanHistory)-1].Timestamp
		if !newTimestamp.After(prevTimestamp) {
			newTimestamp = prevTimestamp.Add(time.Second)
		}

		// Before appending, make sure that the scan we just performed is
		// timestamped after the previous scan performed. It may not be if the
		// system clock has changed.
		newEntry.ScanHistory = append(newEntry.ScanHistory, modules.HostDBScan{Timestamp: newTimestamp, Success: netErr == nil})
	}

	// Check whether any of the recent scans demonstrate uptime. The pruning and
	// compression of the history ensure that there are only relatively recent
	// scans represented.
	var recentUptime bool
	for _, scan := range newEntry.ScanHistory {
		if scan.Success {
			recentUptime = true
		}
	}

	// If the host has been offline for too long, delete the host from the
	// hostdb. Only delete if there have been enough scans over a long enough
	// period to be confident that the host really is offline for good.
	if time.Now().Sub(newEntry.ScanHistory[0].Timestamp) > maxHostDowntime && !recentUptime && len(newEntry.ScanHistory) >= minScans {
		err := hdb.hostTree.Remove(newEntry.PublicKey)
		if err != nil {
			hdb.log.Println("ERROR: unable to remove host newEntry which has had a ton of downtime:", err)
		}

		// The function should terminate here as no more interaction is needed
		// with this host.
		return
	}

	// Compress any old scans into the historic values.
	for len(newEntry.ScanHistory) > minScans && time.Now().Sub(newEntry.ScanHistory[0].Timestamp) > maxHostDowntime {
		timePassed := newEntry.ScanHistory[1].Timestamp.Sub(newEntry.ScanHistory[0].Timestamp)
		if newEntry.ScanHistory[0].Success {
			newEntry.HistoricUptime += timePassed
		} else {
			newEntry.HistoricDowntime += timePassed
		}
		newEntry.ScanHistory = newEntry.ScanHistory[1:]
	}

	// Add the updated entry
	if !exists {
		err := hdb.hostTree.Insert(newEntry)
		if err != nil {
			hdb.log.Println("ERROR: unable to insert entry which is was thought to be new:", err)
		} else {
			hdb.log.Debugf("Adding host %v to the hostdb. Net error: %v\n", newEntry.PublicKey.String(), netErr)
		}
	} else {
		err := hdb.hostTree.Modify(newEntry)
		if err != nil {
			hdb.log.Println("ERROR: unable to modify entry which is thought to exist:", err)
		} else {
			hdb.log.Debugf("Adding host %v to the hostdb. Net error: %v\n", newEntry.PublicKey.String(), netErr)
		}
	}
}

// managedScanHost will connect to a host and grab the settings, verifying
// uptime and updating to the host's preferences.
func (hdb *HostDB) managedScanHost(entry modules.HostDBEntry) {
	// Request settings from the queued host entry.
	netAddr := entry.NetAddress
	pubKey := entry.PublicKey
	hdb.log.Debugf("Scanning host %v at %v", pubKey, netAddr)

	// Update historic interactions of entry if necessary
	hdb.mu.RLock()
	updateHostHistoricInteractions(&entry, hdb.blockHeight)
	hdb.mu.RUnlock()

	var settings modules.HostExternalSettings
	err := func() error {
		dialer := &net.Dialer{
			Cancel:  hdb.tg.StopChan(),
			Timeout: hostRequestTimeout,
		}
		conn, err := dialer.Dial("tcp", string(netAddr))
		if err != nil {
			return err
		}
		connCloseChan := make(chan struct{})
		go func() {
			select {
			case <-hdb.tg.StopChan():
			case <-connCloseChan:
			}
			conn.Close()
		}()
		defer close(connCloseChan)
		conn.SetDeadline(time.Now().Add(hostScanDeadline))

		err = encoding.WriteObject(conn, modules.RPCSettings)
		if err != nil {
			return err
		}
		var pubkey crypto.PublicKey
		copy(pubkey[:], pubKey.Key)
		return crypto.ReadSignedObject(conn, &settings, maxSettingsLen, pubkey)
	}()
	if err != nil {
		hdb.log.Debugf("Scan of host at %v failed: %v", netAddr, err)
		if hdb.online {
			// Increment failed host interactions
			entry.RecentFailedInteractions++
		}

	} else {
		hdb.log.Debugf("Scan of host at %v succeeded.", netAddr)
		entry.HostExternalSettings = settings

		// Increment successful host interactions
		entry.RecentSuccessfulInteractions++
	}

	// Update the host tree to have a new entry, including the new error. Then
	// delete the entry from the scan map as the scan has been successful.
	hdb.mu.Lock()
	hdb.updateEntry(entry, err)
	hdb.mu.Unlock()
}

// threadedProbeHosts pulls hosts from the thread pool and runs a scan on them.
func (hdb *HostDB) threadedProbeHosts(scanPool <-chan modules.HostDBEntry) {
	err := hdb.tg.Add()
	if err != nil {
		return
	}
	defer hdb.tg.Done()
	for hostEntry := range scanPool {
		// Block until hostdb has internet connectivity.
		for {
			hdb.mu.RLock()
			online := hdb.online
			hdb.mu.RUnlock()
			if online {
				break
			}
			select {
			case <-time.After(time.Second * 30):
				continue
			case <-hdb.tg.StopChan():
				return
			}
		}

		// There appears to be internet connectivity, continue with the
		// scan.
		hdb.managedScanHost(hostEntry)
	}
}

// threadedScan is an ongoing function which will query the full set of hosts
// every few hours to see who is online and available for uploading.
func (hdb *HostDB) threadedScan() {
	err := hdb.tg.Add()
	if err != nil {
		return
	}
	defer hdb.tg.Done()

	for {
		// Set up a scan for the hostCheckupQuanity most valuable hosts in the
		// hostdb. Hosts that fail their scans will be docked significantly,
		// pushing them further back in the hierarchy, ensuring that for the
		// most part only online hosts are getting scanned unless there are
		// fewer than hostCheckupQuantity of them.

		// Grab a set of hosts to scan, grab hosts that are active, inactive,
		// and offline to get high diversity.
		var onlineHosts, offlineHosts []modules.HostDBEntry
		allHosts := hdb.hostTree.All()
		for i := len(allHosts) - 1; i >= 0; i-- {
			if len(onlineHosts) >= hostCheckupQuantity && len(offlineHosts) >= hostCheckupQuantity {
				break
			}

			// Figure out if the host is online or offline.
			host := allHosts[i]
			online := len(host.ScanHistory) > 0 && host.ScanHistory[len(host.ScanHistory)-1].Success
			if online && len(onlineHosts) < hostCheckupQuantity {
				onlineHosts = append(onlineHosts, host)
			} else if !online && len(offlineHosts) < hostCheckupQuantity {
				offlineHosts = append(offlineHosts, host)
			}
		}

		// Queue the scans for each host.
		hdb.log.Println("Performing scan on", len(onlineHosts), "online hosts and", len(offlineHosts), "offline hosts.")
		hdb.mu.Lock()
		for _, host := range onlineHosts {
			hdb.queueScan(host)
		}
		for _, host := range offlineHosts {
			hdb.queueScan(host)
		}
		hdb.mu.Unlock()

		// Sleep for a random amount of time before doing another round of
		// scanning. The minimums and maximums keep the scan time reasonable,
		// while the randomness prevents the scanning from always happening at
		// the same time of day or week.
		sleepTime := defaultScanSleep
		sleepRange := int(maxScanSleep - minScanSleep)
		sleepTime = minScanSleep + time.Duration(fastrand.Intn(sleepRange))

		// Sleep until it's time for the next scan cycle.
		select {
		case <-hdb.tg.StopChan():
			return
		case <-time.After(sleepTime):
		}
	}
}
