package hostdb

import (
	"path/filepath"
	"time"

	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/persist"
	"github.com/NebulousLabs/Sia/types"
)

var (
	// persistFilename defines the name of the file that holds the hostdb's
	// persistence.
	persistFilename = "hostdb.json"

	// persistMetadata defines the metadata that tags along with the most recent
	// version of the hostdb persistence file.
	persistMetadata = persist.Metadata{
		Header:  "HostDB Persistence",
		Version: "0.5",
	}
)

// hdbPersist defines what HostDB data persists across sessions.
type hdbPersist struct {
	AllHosts    []modules.HostDBEntry
	BlockHeight types.BlockHeight
	LastChange  modules.ConsensusChangeID
}

// persistData returns the data in the hostdb that will be saved to disk.
func (hdb *HostDB) persistData() (data hdbPersist) {
	data.AllHosts = hdb.hostTree.All()
	data.BlockHeight = hdb.blockHeight
	data.LastChange = hdb.lastChange
	return data
}

// saveSync saves the hostdb persistence data to disk and then syncs to disk.
func (hdb *HostDB) saveSync() error {
	return hdb.deps.saveFileSync(persistMetadata, hdb.persistData(), filepath.Join(hdb.persistDir, persistFilename))
}

// load loads the hostdb persistence data from disk.
func (hdb *HostDB) load() error {
	// Fetch the data from the file.
	var data hdbPersist
	err := hdb.deps.loadFile(persistMetadata, &data, filepath.Join(hdb.persistDir, persistFilename))
	if err != nil {
		return err
	}

	// Set the hostdb internal values.
	hdb.blockHeight = data.BlockHeight
	hdb.lastChange = data.LastChange

	// Load each of the hosts into the host tree.
	for _, host := range data.AllHosts {
		// COMPATv1.1.0
		//
		// The host did not always track its block height correctly, meaning
		// that previously the FirstSeen values and the blockHeight values
		// could get out of sync.
		if hdb.blockHeight < host.FirstSeen {
			host.FirstSeen = hdb.blockHeight
		}

		err := hdb.hostTree.Insert(host)
		if err != nil {
			hdb.log.Debugln("ERROR: could not insert host while loading:", host.NetAddress)
		}

		// Make sure that all hosts have gone through the initial scanning.
		if len(host.ScanHistory) < 2 {
			hdb.queueScan(host)
		}
	}
	return nil
}

// threadedSaveLoop saves the hostdb to disk every 2 minutes, also saving when
// given the shutdown signal.
func (hdb *HostDB) threadedSaveLoop() {
	for {
		select {
		case <-hdb.tg.StopChan():
			return
		case <-time.After(saveFrequency):
			hdb.mu.Lock()
			err := hdb.saveSync()
			hdb.mu.Unlock()
			if err != nil {
				hdb.log.Println("Difficulties saving the hostdb:", err)
			}
		}
	}
}
