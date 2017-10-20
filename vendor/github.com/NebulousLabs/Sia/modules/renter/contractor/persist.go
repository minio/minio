package contractor

import (
	"path/filepath"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/persist"
	"github.com/NebulousLabs/Sia/types"
)

// contractorPersist defines what Contractor data persists across sessions.
type contractorPersist struct {
	Allowance       modules.Allowance                 `json:"allowance"`
	BlockHeight     types.BlockHeight                 `json:"blockheight"`
	CachedRevisions map[string]cachedRevision         `json:"cachedrevisions"`
	Contracts       map[string]modules.RenterContract `json:"contracts"`
	CurrentPeriod   types.BlockHeight                 `json:"currentperiod"`
	LastChange      modules.ConsensusChangeID         `json:"lastchange"`
	OldContracts    []modules.RenterContract          `json:"oldcontracts"`
	RenewedIDs      map[string]string                 `json:"renewedids"`
}

// persistData returns the data in the Contractor that will be saved to disk.
func (c *Contractor) persistData() contractorPersist {
	data := contractorPersist{
		Allowance:       c.allowance,
		BlockHeight:     c.blockHeight,
		CachedRevisions: make(map[string]cachedRevision),
		Contracts:       make(map[string]modules.RenterContract),
		CurrentPeriod:   c.currentPeriod,
		LastChange:      c.lastChange,
		RenewedIDs:      make(map[string]string),
	}
	for _, rev := range c.cachedRevisions {
		data.CachedRevisions[rev.Revision.ParentID.String()] = rev
	}
	for _, contract := range c.contracts {
		data.Contracts[contract.ID.String()] = contract
	}
	for _, contract := range c.oldContracts {
		contract.MerkleRoots = []crypto.Hash{} // prevent roots from being saved to disk twice
		data.OldContracts = append(data.OldContracts, contract)
	}
	for oldID, newID := range c.renewedIDs {
		data.RenewedIDs[oldID.String()] = newID.String()
	}
	return data
}

// load loads the Contractor persistence data from disk.
func (c *Contractor) load() error {
	var data contractorPersist
	err := c.persist.load(&data)
	if err != nil {
		return err
	}
	c.allowance = data.Allowance
	c.blockHeight = data.BlockHeight
	for _, rev := range data.CachedRevisions {
		c.cachedRevisions[rev.Revision.ParentID] = rev
	}
	c.currentPeriod = data.CurrentPeriod
	if c.currentPeriod == 0 {
		// COMPATv1.0.4-lts
		// If loading old persist, current period will be unknown. Best we can
		// do is guess based on contracts + allowance.
		var highestEnd types.BlockHeight
		for _, contract := range data.Contracts {
			if h := contract.EndHeight(); h > highestEnd {
				highestEnd = h
			}
		}
		c.currentPeriod = highestEnd - c.allowance.Period
	}

	// COMPATv1.1.0
	//
	// If loading old persist, the host's public key is unknown. We must
	// rescan the blockchain for a host announcement corresponding to the
	// contract's NetAddress.
	for _, contract := range data.Contracts {
		if len(contract.HostPublicKey.Key) == 0 {
			data.Contracts = addPubKeys(c.cs, data.Contracts, c.tg.StopChan())
			break // only need to rescan once
		}
	}

	for _, contract := range data.Contracts {
		// COMPATv1.0.4-lts
		// If loading old persist, start height of contract is unknown. Give
		// the contract a fake startheight so that it will included with the
		// other contracts in the current period.
		if contract.StartHeight == 0 {
			contract.StartHeight = c.currentPeriod + 1
		}
		// COMPATv1.1.2
		// Old versions calculated the TotalCost field incorrectly, omitting
		// the transaction fee. Recompute the TotalCost from scratch using the
		// original allocated funds and fees.
		if len(contract.FileContract.ValidProofOutputs) > 0 {
			contract.TotalCost = contract.FileContract.ValidProofOutputs[0].Value.
				Add(contract.TxnFee).Add(contract.SiafundFee).Add(contract.ContractFee)
		}
		c.contracts[contract.ID] = contract
	}

	c.lastChange = data.LastChange
	for _, contract := range data.OldContracts {
		c.oldContracts[contract.ID] = contract
	}
	for oldString, newString := range data.RenewedIDs {
		var oldHash, newHash crypto.Hash
		oldHash.LoadString(oldString)
		newHash.LoadString(newString)
		c.renewedIDs[types.FileContractID(oldHash)] = types.FileContractID(newHash)
	}

	return nil
}

// save saves the Contractor persistence data to disk.
func (c *Contractor) save() error {
	return c.persist.save(c.persistData())
}

// saveSync saves the Contractor persistence data to disk and then syncs to disk.
func (c *Contractor) saveSync() error {
	return c.persist.save(c.persistData())
}

// saveUploadRevision returns a function that saves an upload revision. It is
// used by the Editor type to prevent desynchronizing with the host.
func (c *Contractor) saveUploadRevision(id types.FileContractID) func(types.FileContractRevision, []crypto.Hash) error {
	return func(rev types.FileContractRevision, newRoots []crypto.Hash) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.cachedRevisions[id] = cachedRevision{rev, newRoots}
		return c.persist.update(updateCachedUploadRevision{
			Revision: rev,
			// only the last root is new
			SectorRoot:  newRoots[len(newRoots)-1],
			SectorIndex: len(newRoots) - 1,
		})
	}
}

// saveDownloadRevision returns a function that saves an upload revision. It
// is used by the Downloader type to prevent desynchronizing with the host.
func (c *Contractor) saveDownloadRevision(id types.FileContractID) func(types.FileContractRevision, []crypto.Hash) error {
	return func(rev types.FileContractRevision, _ []crypto.Hash) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		// roots have not changed
		cr := c.cachedRevisions[id]
		cr.Revision = rev
		c.cachedRevisions[id] = cr
		return c.persist.update(updateCachedDownloadRevision{
			Revision: rev,
		})
	}
}

// addPubKeys rescans the blockchain to fill in the HostPublicKey of
// contracts, identified by their NetAddress.
func addPubKeys(cs consensusSet, contracts map[string]modules.RenterContract, cancel <-chan struct{}) map[string]modules.RenterContract {
	pubkeys := make(pubkeyScanner)
	for _, c := range contracts {
		pubkeys[c.NetAddress] = types.SiaPublicKey{}
	}
	cs.ConsensusSetSubscribe(pubkeys, modules.ConsensusChangeBeginning, cancel)
	for id, c := range contracts {
		c.HostPublicKey = pubkeys[c.NetAddress]
		contracts[id] = c
	}
	cs.Unsubscribe(&pubkeys)
	return contracts
}

type pubkeyScanner map[modules.NetAddress]types.SiaPublicKey

func (pubkeys pubkeyScanner) ProcessConsensusChange(cc modules.ConsensusChange) {
	// find announcements
	for _, block := range cc.AppliedBlocks {
		for _, txn := range block.Transactions {
			for _, arb := range txn.ArbitraryData {
				// decode announcement
				addr, pubKey, err := modules.DecodeAnnouncement(arb)
				if err != nil {
					continue
				}

				// For each announcement, if we recognize the addr, map it
				// to the announced pubkey. Note that we will overwrite
				// the pubkey if two announcements have the same addr.
				if _, relevant := pubkeys[addr]; relevant {
					pubkeys[addr] = pubKey
				}
			}
		}
	}
}

// COMPATv1.1.0
func loadv110persist(dir string, data *contractorPersist) error {
	var oldPersist struct {
		Allowance        modules.Allowance
		BlockHeight      types.BlockHeight
		CachedRevisions  []cachedRevision
		Contracts        []modules.RenterContract
		CurrentPeriod    types.BlockHeight
		LastChange       modules.ConsensusChangeID
		OldContracts     []modules.RenterContract
		RenewedIDs       map[string]string
		FinancialMetrics struct {
			ContractSpending types.Currency
			DownloadSpending types.Currency
			StorageSpending  types.Currency
			UploadSpending   types.Currency
		}
	}
	err := persist.LoadJSON(persist.Metadata{
		Header:  "Contractor Persistence",
		Version: "0.5.2",
	}, &oldPersist, filepath.Join(dir, "contractor.json"))
	if err != nil {
		return err
	}
	cachedRevisions := make(map[string]cachedRevision)
	for _, rev := range oldPersist.CachedRevisions {
		cachedRevisions[rev.Revision.ParentID.String()] = rev
	}
	contracts := make(map[string]modules.RenterContract)
	for _, c := range oldPersist.Contracts {
		contracts[c.ID.String()] = c
	}

	// COMPATv1.0.4-lts
	//
	// If loading old persist, only aggregate metrics are known. Store these
	// in a special contract under a special identifier.
	if fm := oldPersist.FinancialMetrics; !fm.ContractSpending.Add(fm.DownloadSpending).Add(fm.StorageSpending).Add(fm.UploadSpending).IsZero() {
		oldPersist.OldContracts = append(oldPersist.OldContracts, modules.RenterContract{
			ID:               metricsContractID,
			TotalCost:        fm.ContractSpending,
			DownloadSpending: fm.DownloadSpending,
			StorageSpending:  fm.StorageSpending,
			UploadSpending:   fm.UploadSpending,
			// Give the contract a fake startheight so that it will included
			// with the other contracts in the current period. Note that in
			// update.go, the special contract is specifically deleted when a
			// new period begins.
			StartHeight: oldPersist.CurrentPeriod + 1,
			// We also need to add a ValidProofOutput so that the RenterFunds
			// method will not panic. The value should be 0, i.e. "all funds
			// were spent."
			LastRevision: types.FileContractRevision{
				NewValidProofOutputs: make([]types.SiacoinOutput, 2),
			},
		})
	}

	*data = contractorPersist{
		Allowance:       oldPersist.Allowance,
		BlockHeight:     oldPersist.BlockHeight,
		CachedRevisions: cachedRevisions,
		Contracts:       contracts,
		CurrentPeriod:   oldPersist.CurrentPeriod,
		LastChange:      oldPersist.LastChange,
		OldContracts:    oldPersist.OldContracts,
		RenewedIDs:      oldPersist.RenewedIDs,
	}

	return nil
}
