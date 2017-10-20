package contractor

import (
	"path/filepath"

	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
)

// These interfaces define the HostDB's dependencies. Using the smallest
// interface possible makes it easier to mock these dependencies in testing.
type (
	consensusSet interface {
		ConsensusSetSubscribe(modules.ConsensusSetSubscriber, modules.ConsensusChangeID, <-chan struct{}) error
		Synced() bool
		Unsubscribe(modules.ConsensusSetSubscriber)
	}
	// In order to restrict the modules.TransactionBuilder interface, we must
	// provide a shim to bridge the gap between modules.Wallet and
	// transactionBuilder.
	walletShim interface {
		NextAddress() (types.UnlockConditions, error)
		StartTransaction() modules.TransactionBuilder
	}
	wallet interface {
		NextAddress() (types.UnlockConditions, error)
		StartTransaction() transactionBuilder
	}
	transactionBuilder interface {
		AddArbitraryData([]byte) uint64
		AddFileContract(types.FileContract) uint64
		AddMinerFee(types.Currency) uint64
		AddParents([]types.Transaction)
		AddSiacoinInput(types.SiacoinInput) uint64
		AddSiacoinOutput(types.SiacoinOutput) uint64
		AddTransactionSignature(types.TransactionSignature) uint64
		Drop()
		FundSiacoins(types.Currency) error
		Sign(bool) ([]types.Transaction, error)
		View() (types.Transaction, []types.Transaction)
		ViewAdded() (parents, coins, funds, signatures []int)
	}
	transactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		FeeEstimation() (min types.Currency, max types.Currency)
	}

	hostDB interface {
		AllHosts() []modules.HostDBEntry
		ActiveHosts() []modules.HostDBEntry
		Host(types.SiaPublicKey) (modules.HostDBEntry, bool)
		IncrementSuccessfulInteractions(key types.SiaPublicKey)
		IncrementFailedInteractions(key types.SiaPublicKey)
		RandomHosts(n int, exclude []types.SiaPublicKey) []modules.HostDBEntry
		ScoreBreakdown(modules.HostDBEntry) modules.HostScoreBreakdown
	}

	persister interface {
		save(contractorPersist) error
		update(...journalUpdate) error
		load(*contractorPersist) error
		Close() error
	}
)

// Because wallet is not directly compatible with modules.Wallet (wrong
// type signature for StartTransaction), we must provide a bridge type.
type walletBridge struct {
	w walletShim
}

func (ws *walletBridge) NextAddress() (types.UnlockConditions, error) { return ws.w.NextAddress() }
func (ws *walletBridge) StartTransaction() transactionBuilder         { return ws.w.StartTransaction() }

// stdPersist implements the persister interface via the journal type. The
// filename required by these functions is internal to stdPersist.
type stdPersist struct {
	journal  *journal
	filename string
}

func (p *stdPersist) save(data contractorPersist) error {
	if p.journal == nil {
		var err error
		p.journal, err = newJournal(p.filename, data)
		return err
	}
	return p.journal.checkpoint(data)
}

func (p *stdPersist) update(us ...journalUpdate) error {
	return p.journal.update(us)
}

func (p *stdPersist) load(data *contractorPersist) error {
	var err error
	p.journal, err = openJournal(p.filename, data)
	if err != nil {
		// Try loading old persist.
		err = loadv110persist(filepath.Dir(p.filename), data)
		if err != nil {
			return err
		}
		p.journal, err = newJournal(p.filename, *data)
	}
	return err
}

func (p stdPersist) Close() error {
	return p.journal.Close()
}

func newPersist(dir string) *stdPersist {
	return &stdPersist{
		filename: filepath.Join(dir, "contractor.journal"),
	}
}
