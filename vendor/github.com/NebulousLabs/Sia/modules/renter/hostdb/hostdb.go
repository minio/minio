// Package hostdb provides a HostDB object that implements the renter.hostDB
// interface. The blockchain is scanned for host announcements and hosts that
// are found get added to the host database. The database continually scans the
// set of hosts it has found and updates who is online.
package hostdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/modules/renter/hostdb/hosttree"
	"github.com/NebulousLabs/Sia/persist"
	siasync "github.com/NebulousLabs/Sia/sync"
	"github.com/NebulousLabs/Sia/types"
)

var (
	errNilCS      = errors.New("cannot create hostdb with nil consensus set")
	errNilGateway = errors.New("cannot create hostdb with nil gateway")
)

// The HostDB is a database of potential hosts. It assigns a weight to each
// host based on their hosting parameters, and then can select hosts at random
// for uploading files.
type HostDB struct {
	// dependencies
	cs         modules.ConsensusSet
	deps       dependencies
	gateway    modules.Gateway
	log        *persist.Logger
	mu         sync.RWMutex
	persistDir string
	tg         siasync.ThreadGroup

	// The hostTree is the root node of the tree that organizes hosts by
	// weight. The tree is necessary for selecting weighted hosts at
	// random.
	hostTree *hosttree.HostTree

	// the scanPool is a set of hosts that need to be scanned. There are a
	// handful of goroutines constantly waiting on the channel for hosts to
	// scan. The scan map is used to prevent duplicates from entering the scan
	// pool.
	scanList        []modules.HostDBEntry
	scanMap         map[string]struct{}
	scanWait        bool
	online          bool
	scanningThreads int

	blockHeight types.BlockHeight
	lastChange  modules.ConsensusChangeID
}

// New returns a new HostDB.
func New(g modules.Gateway, cs modules.ConsensusSet, persistDir string) (*HostDB, error) {
	// Check for nil inputs.
	if g == nil {
		return nil, errNilGateway
	}
	if cs == nil {
		return nil, errNilCS
	}
	// Create HostDB using production dependencies.
	return newHostDB(g, cs, persistDir, prodDependencies{})
}

// newHostDB creates a HostDB using the provided dependencies. It loads the old
// persistence data, spawns the HostDB's scanning threads, and subscribes it to
// the consensusSet.
func newHostDB(g modules.Gateway, cs modules.ConsensusSet, persistDir string, deps dependencies) (*HostDB, error) {
	// Create the HostDB object.
	hdb := &HostDB{
		cs:         cs,
		deps:       deps,
		gateway:    g,
		persistDir: persistDir,

		scanMap: make(map[string]struct{}),
	}

	// Create the persist directory if it does not yet exist.
	err := os.MkdirAll(persistDir, 0700)
	if err != nil {
		return nil, err
	}

	// Create the logger.
	logger, err := persist.NewFileLogger(filepath.Join(persistDir, "hostdb.log"))
	if err != nil {
		return nil, err
	}
	hdb.log = logger
	hdb.tg.AfterStop(func() {
		if err := hdb.log.Close(); err != nil {
			// Resort to println as the logger is in an uncertain state.
			fmt.Println("Failed to close the hostdb logger:", err)
		}
	})

	// The host tree is used to manage hosts and query them at random.
	hdb.hostTree = hosttree.New(hdb.calculateHostWeight)

	// Load the prior persistence structures.
	hdb.mu.Lock()
	err = hdb.load()
	hdb.mu.Unlock()
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	hdb.tg.AfterStop(func() {
		hdb.mu.Lock()
		err := hdb.saveSync()
		hdb.mu.Unlock()
		if err != nil {
			hdb.log.Println("Unable to save the hostdb:", err)
		}
	})

	// Loading is complete, establish the save loop.
	go hdb.threadedSaveLoop()

	// Don't perform the remaining startup in the presence of a quitAfterLoad
	// disruption.
	if hdb.deps.disrupt("quitAfterLoad") {
		return hdb, nil
	}

	// COMPATv1.1.0
	//
	// If the block height has loaded as zero, the most recent consensus change
	// needs to be set to perform a full rescan. This will also help the hostdb
	// to pick up any hosts that it has incorrectly dropped in the past.
	hdb.mu.Lock()
	if hdb.blockHeight == 0 {
		hdb.lastChange = modules.ConsensusChangeBeginning
	}
	hdb.mu.Unlock()

	err = cs.ConsensusSetSubscribe(hdb, hdb.lastChange, hdb.tg.StopChan())
	if err == modules.ErrInvalidConsensusChangeID {
		// Subscribe again using the new ID. This will cause a triggered scan
		// on all of the hosts, but that should be acceptable.
		hdb.mu.Lock()
		hdb.blockHeight = 0
		hdb.lastChange = modules.ConsensusChangeBeginning
		hdb.mu.Unlock()
		err = cs.ConsensusSetSubscribe(hdb, hdb.lastChange, hdb.tg.StopChan())
	}
	if err != nil {
		return nil, errors.New("hostdb subscription failed: " + err.Error())
	}
	hdb.tg.OnStop(func() {
		cs.Unsubscribe(hdb)
	})

	// Spin up the host scanning processes.
	if build.Release == "standard" {
		go hdb.threadedOnlineCheck()
	} else {
		// During testing, the hostdb is just always assumed to be online, since
		// the online check of having nonlocal peers will always fail.
		hdb.mu.Lock()
		hdb.online = true
		hdb.mu.Unlock()
	}

	// Spawn the scan loop during production, but allow it to be disrupted
	// during testing. Primary reason is so that we can fill the hostdb with
	// fake hosts and not have them marked as offline as the scanloop operates.
	if !hdb.deps.disrupt("disableScanLoop") {
		go hdb.threadedScan()
	}

	return hdb, nil
}

// ActiveHosts returns a list of hosts that are currently online, sorted by
// weight.
func (hdb *HostDB) ActiveHosts() (activeHosts []modules.HostDBEntry) {
	allHosts := hdb.hostTree.All()
	for _, entry := range allHosts {
		if len(entry.ScanHistory) == 0 {
			continue
		}
		if !entry.ScanHistory[len(entry.ScanHistory)-1].Success {
			continue
		}
		if !entry.AcceptingContracts {
			continue
		}
		activeHosts = append(activeHosts, entry)
	}
	return activeHosts
}

// AllHosts returns all of the hosts known to the hostdb, including the
// inactive ones.
func (hdb *HostDB) AllHosts() (allHosts []modules.HostDBEntry) {
	return hdb.hostTree.All()
}

// AverageContractPrice returns the average price of a host.
func (hdb *HostDB) AverageContractPrice() (totalPrice types.Currency) {
	sampleSize := 32
	hosts := hdb.hostTree.SelectRandom(sampleSize, nil)
	if len(hosts) == 0 {
		return totalPrice
	}
	for _, host := range hosts {
		totalPrice = totalPrice.Add(host.ContractPrice)
	}
	return totalPrice.Div64(uint64(len(hosts)))
}

// Close closes the hostdb, terminating its scanning threads
func (hdb *HostDB) Close() error {
	return hdb.tg.Stop()
}

// Host returns the HostSettings associated with the specified NetAddress. If
// no matching host is found, Host returns false.
func (hdb *HostDB) Host(spk types.SiaPublicKey) (modules.HostDBEntry, bool) {
	host, exists := hdb.hostTree.Select(spk)
	if !exists {
		return host, exists
	}
	hdb.mu.RLock()
	updateHostHistoricInteractions(&host, hdb.blockHeight)
	hdb.mu.RUnlock()
	return host, exists
}

// RandomHosts implements the HostDB interface's RandomHosts() method. It takes
// a number of hosts to return, and a slice of netaddresses to ignore, and
// returns a slice of entries.
func (hdb *HostDB) RandomHosts(n int, excludeKeys []types.SiaPublicKey) []modules.HostDBEntry {
	return hdb.hostTree.SelectRandom(n, excludeKeys)
}
