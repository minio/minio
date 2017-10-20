package modules

import (
	"bytes"
	"errors"

	"github.com/NebulousLabs/entropy-mnemonics"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/types"
)

const (
	// WalletDir is the directory that contains the wallet persistence.
	WalletDir = "wallet"

	// SeedChecksumSize is the number of bytes that are used to checksum
	// addresses to prevent accidental spending.
	SeedChecksumSize = 6

	// PublicKeysPerSeed define the number of public keys that get pregenerated
	// for a seed at startup when searching for balances in the blockchain.
	PublicKeysPerSeed = 2500
)

var (
	// ErrBadEncryptionKey is returned if the incorrect encryption key to a
	// file is provided.
	ErrBadEncryptionKey = errors.New("provided encryption key is incorrect")

	// ErrLowBalance is returned if the wallet does not have enough funds to
	// complete the desired action.
	ErrLowBalance = errors.New("insufficient balance")

	// ErrIncompleteTransactions is returned if the wallet has incomplete
	// transactions being built that are using all of the current outputs, and
	// therefore the wallet is unable to spend money despite it not technically
	// being 'unconfirmed' yet.
	ErrIncompleteTransactions = errors.New("wallet has coins spent in incomplete transactions - not enough remaining coins")

	// ErrLockedWallet is returned when an action cannot be performed due to
	// the wallet being locked.
	ErrLockedWallet = errors.New("wallet must be unlocked before it can be used")
)

type (
	// Seed is cryptographic entropy that is used to derive spendable wallet
	// addresses.
	Seed [crypto.EntropySize]byte

	// WalletTransactionID is a unique identifier for a wallet transaction.
	WalletTransactionID crypto.Hash

	// A ProcessedInput represents funding to a transaction. The input is
	// coming from an address and going to the outputs. The fund types are
	// 'SiacoinInput', 'SiafundInput'.
	ProcessedInput struct {
		ParentID       types.OutputID   `json:"parentid"`
		FundType       types.Specifier  `json:"fundtype"`
		WalletAddress  bool             `json:"walletaddress"`
		RelatedAddress types.UnlockHash `json:"relatedaddress"`
		Value          types.Currency   `json:"value"`
	}

	// A ProcessedOutput is a siacoin output that appears in a transaction.
	// Some outputs mature immediately, some are delayed, and some may never
	// mature at all (in the event of storage proofs).
	//
	// Fund type can either be 'SiacoinOutput', 'SiafundOutput', 'ClaimOutput',
	// 'MinerPayout', or 'MinerFee'. All outputs except the miner fee create
	// outputs accessible to an address. Miner fees are not spendable, and
	// instead contribute to the block subsidy.
	//
	// MaturityHeight indicates at what block height the output becomes
	// available. SiacoinInputs and SiafundInputs become available immediately.
	// ClaimInputs and MinerPayouts become available after 144 confirmations.
	ProcessedOutput struct {
		ID             types.OutputID    `json:"id"`
		FundType       types.Specifier   `json:"fundtype"`
		MaturityHeight types.BlockHeight `json:"maturityheight"`
		WalletAddress  bool              `json:"walletaddress"`
		RelatedAddress types.UnlockHash  `json:"relatedaddress"`
		Value          types.Currency    `json:"value"`
	}

	// A ProcessedTransaction is a transaction that has been processed into
	// explicit inputs and outputs and tagged with some header data such as
	// confirmation height + timestamp.
	//
	// Because of the block subsidy, a block is considered as a transaction.
	// Since there is technically no transaction id for the block subsidy, the
	// block id is used instead.
	ProcessedTransaction struct {
		Transaction           types.Transaction   `json:"transaction"`
		TransactionID         types.TransactionID `json:"transactionid"`
		ConfirmationHeight    types.BlockHeight   `json:"confirmationheight"`
		ConfirmationTimestamp types.Timestamp     `json:"confirmationtimestamp"`

		Inputs  []ProcessedInput  `json:"inputs"`
		Outputs []ProcessedOutput `json:"outputs"`
	}

	// TransactionBuilder is used to construct custom transactions. A transaction
	// builder is initialized via 'RegisterTransaction' and then can be modified by
	// adding funds or other fields. The transaction is completed by calling
	// 'Sign', which will sign all inputs added via the 'FundSiacoins' or
	// 'FundSiafunds' call. All modifications are additive.
	//
	// Parents of the transaction are kept in the transaction builder. A parent is
	// any unconfirmed transaction that is required for the child to be valid.
	//
	// Transaction builders are not thread safe.
	TransactionBuilder interface {
		// FundSiacoins will add a siacoin input of exactly 'amount' to the
		// transaction. A parent transaction may be needed to achieve an input
		// with the correct value. The siacoin input will not be signed until
		// 'Sign' is called on the transaction builder. The expectation is that
		// the transaction will be completed and broadcast within a few hours.
		// Longer risks double-spends, as the wallet will assume that the
		// transaction failed.
		FundSiacoins(amount types.Currency) error

		// FundSiafunds will add a siafund input of exactly 'amount' to the
		// transaction. A parent transaction may be needed to achieve an input
		// with the correct value. The siafund input will not be signed until
		// 'Sign' is called on the transaction builder. Any siacoins that are
		// released by spending the siafund outputs will be sent to another
		// address owned by the wallet. The expectation is that the transaction
		// will be completed and broadcast within a few hours. Longer risks
		// double-spends, because the wallet will assume the transaction
		// failed.
		FundSiafunds(amount types.Currency) error

		// AddParents adds a set of parents to the transaction.
		AddParents([]types.Transaction)

		// AddMinerFee adds a miner fee to the transaction, returning the index
		// of the miner fee within the transaction.
		AddMinerFee(fee types.Currency) uint64

		// AddSiacoinInput adds a siacoin input to the transaction, returning
		// the index of the siacoin input within the transaction. When 'Sign'
		// gets called, this input will be left unsigned.
		AddSiacoinInput(types.SiacoinInput) uint64

		// AddSiacoinOutput adds a siacoin output to the transaction, returning
		// the index of the siacoin output within the transaction.
		AddSiacoinOutput(types.SiacoinOutput) uint64

		// AddFileContract adds a file contract to the transaction, returning
		// the index of the file contract within the transaction.
		AddFileContract(types.FileContract) uint64

		// AddFileContractRevision adds a file contract revision to the
		// transaction, returning the index of the file contract revision
		// within the transaction. When 'Sign' gets called, this revision will
		// be left unsigned.
		AddFileContractRevision(types.FileContractRevision) uint64

		// AddStorageProof adds a storage proof to the transaction, returning
		// the index of the storage proof within the transaction.
		AddStorageProof(types.StorageProof) uint64

		// AddSiafundInput adds a siafund input to the transaction, returning
		// the index of the siafund input within the transaction. When 'Sign'
		// is called, this input will be left unsigned.
		AddSiafundInput(types.SiafundInput) uint64

		// AddSiafundOutput adds a siafund output to the transaction, returning
		// the index of the siafund output within the transaction.
		AddSiafundOutput(types.SiafundOutput) uint64

		// AddArbitraryData adds arbitrary data to the transaction, returning
		// the index of the data within the transaction.
		AddArbitraryData(arb []byte) uint64

		// AddTransactionSignature adds a transaction signature to the
		// transaction, returning the index of the signature within the
		// transaction. The signature should already be valid, and shouldn't
		// sign any of the inputs that were added by calling 'FundSiacoins' or
		// 'FundSiafunds'.
		AddTransactionSignature(types.TransactionSignature) uint64

		// Sign will sign any inputs added by 'FundSiacoins' or 'FundSiafunds'
		// and return a transaction set that contains all parents prepended to
		// the transaction. If more fields need to be added, a new transaction
		// builder will need to be created.
		//
		// If the whole transaction flag is set to true, then the whole
		// transaction flag will be set in the covered fields object. If the
		// whole transaction flag is set to false, then the covered fields
		// object will cover all fields that have already been added to the
		// transaction, but will also leave room for more fields to be added.
		//
		// An error will be returned if there are multiple calls to 'Sign',
		// sometimes even if the first call to Sign has failed. Sign should
		// only ever be called once, and if the first signing fails, the
		// transaction should be dropped.
		Sign(wholeTransaction bool) ([]types.Transaction, error)

		// View returns the incomplete transaction along with all of its
		// parents.
		View() (txn types.Transaction, parents []types.Transaction)

		// ViewAdded returns all of the siacoin inputs, siafund inputs, and
		// parent transactions that have been automatically added by the
		// builder. Items are returned by index.
		ViewAdded() (newParents, siacoinInputs, siafundInputs, transactionSignatures []int)

		// Drop indicates that a transaction is no longer useful and will not be
		// broadcast, and that all of the outputs can be reclaimed. 'Drop'
		// should only be used before signatures are added.
		Drop()
	}

	// EncryptionManager can encrypt, lock, unlock, and indicate the current
	// status of the EncryptionManager.
	EncryptionManager interface {
		// Encrypt will encrypt the wallet using the input key. Upon
		// encryption, a primary seed will be created for the wallet (no seed
		// exists prior to this point). If the key is blank, then the hash of
		// the seed that is generated will be used as the key.
		//
		// Encrypt can only be called once throughout the life of the wallet
		// and will return an error on subsequent calls (even after restarting
		// the wallet). To reset the wallet, the wallet files must be moved to
		// a different directory or deleted.
		Encrypt(masterKey crypto.TwofishKey) (Seed, error)

		// Reset will reset the wallet, clearing the database and returning it to
		// the unencrypted state. Reset can only be called on a wallet that has
		// already been encrypted.
		Reset() error

		// Encrypted returns whether or not the wallet has been encrypted yet.
		// After being encrypted for the first time, the wallet can only be
		// unlocked using the encryption password.
		Encrypted() bool

		// InitFromSeed functions like Encrypt, but using a specified seed.
		// Unlike Encrypt, the blockchain will be scanned to determine the
		// seed's progress. For this reason, InitFromSeed should not be called
		// until the blockchain is fully synced.
		InitFromSeed(masterKey crypto.TwofishKey, seed Seed) error

		// Lock deletes all keys in memory and prevents the wallet from being
		// used to spend coins or extract keys until 'Unlock' is called.
		Lock() error

		// Unlock must be called before the wallet is usable. All wallets and
		// wallet seeds are encrypted by default, and the wallet will not know
		// which addresses to watch for on the blockchain until unlock has been
		// called.
		//
		// All items in the wallet are encrypted using different keys which are
		// derived from the master key.
		Unlock(masterKey crypto.TwofishKey) error

		// ChangeKey changes the wallet's materKey from masterKey to newKey,
		// re-encrypting the wallet with the provided key.
		ChangeKey(masterKey crypto.TwofishKey, newKey crypto.TwofishKey) error

		// Unlocked returns true if the wallet is currently unlocked, false
		// otherwise.
		Unlocked() bool
	}

	// KeyManager manages wallet keys, including the use of seeds, creating and
	// loading backups, and providing a layer of compatibility for older wallet
	// files.
	KeyManager interface {
		// AllAddresses returns all addresses that the wallet is able to spend
		// from, including unseeded addresses. Addresses are returned sorted in
		// byte-order.
		AllAddresses() []types.UnlockHash

		// AllSeeds returns all of the seeds that are being tracked by the
		// wallet, including the primary seed. Only the primary seed is used to
		// generate new addresses, but the wallet can spend funds sent to
		// public keys generated by any of the seeds returned.
		AllSeeds() ([]Seed, error)

		// CreateBackup will create a backup of the wallet at the provided
		// filepath. The backup will have all seeds and keys.
		CreateBackup(string) error

		// LoadBackup will load a backup of the wallet from the provided
		// address. The backup wallet will be added as an auxiliary seed, not
		// as a primary seed.
		// LoadBackup(masterKey, backupMasterKey crypto.TwofishKey, string) error

		// Load033xWallet will load a version 0.3.3.x wallet from disk and add all of
		// the keys in the wallet as unseeded keys.
		Load033xWallet(crypto.TwofishKey, string) error

		// LoadSeed will recreate a wallet file using the recovery phrase.
		// LoadSeed only needs to be called if the original seed file or
		// encryption password was lost. The master key is used to encrypt the
		// recovery seed before saving it to disk.
		LoadSeed(crypto.TwofishKey, Seed) error

		// LoadSiagKeys will take a set of filepaths that point to a siag key
		// and will have the siag keys loaded into the wallet so that they will
		// become spendable.
		LoadSiagKeys(crypto.TwofishKey, []string) error

		// NextAddress returns a new coin addresses generated from the
		// primary seed.
		NextAddress() (types.UnlockConditions, error)

		// PrimarySeed returns the unencrypted primary seed of the wallet,
		// along with a uint64 indicating how many addresses may be safely
		// generated from the seed.
		PrimarySeed() (Seed, uint64, error)

		// SweepSeed scans the blockchain for outputs generated from seed and
		// creates a transaction that transfers them to the wallet. Note that
		// this incurs a transaction fee. It returns the total value of the
		// outputs, minus the fee. If only siafunds were found, the fee is
		// deducted from the wallet.
		SweepSeed(seed Seed) (coins, funds types.Currency, err error)
	}

	// Wallet stores and manages siacoins and siafunds. The wallet file is
	// encrypted using a user-specified password. Common addresses are all
	// derived from a single address seed.
	Wallet interface {
		EncryptionManager
		KeyManager

		// Close permits clean shutdown during testing and serving.
		Close() error

		// ConfirmedBalance returns the confirmed balance of the wallet, minus
		// any outgoing transactions. ConfirmedBalance will include unconfirmed
		// refund transactions.
		ConfirmedBalance() (siacoinBalance types.Currency, siafundBalance types.Currency, siacoinClaimBalance types.Currency)

		// UnconfirmedBalance returns the unconfirmed balance of the wallet.
		// Outgoing funds and incoming funds are reported separately. Refund
		// outputs are included, meaning that sending a single coin to
		// someone could result in 'outgoing: 12, incoming: 11'. Siafunds are
		// not considered in the unconfirmed balance.
		UnconfirmedBalance() (outgoingSiacoins types.Currency, incomingSiacoins types.Currency)

		// AddressTransactions returns all of the transactions that are related
		// to a given address.
		AddressTransactions(types.UnlockHash) []ProcessedTransaction

		// AddressUnconfirmedHistory returns all of the unconfirmed
		// transactions related to a given address.
		AddressUnconfirmedTransactions(types.UnlockHash) []ProcessedTransaction

		// Transaction returns the transaction with the given id. The bool
		// indicates whether the transaction is in the wallet database. The
		// wallet only stores transactions that are related to the wallet.
		Transaction(types.TransactionID) (ProcessedTransaction, bool)

		// Transactions returns all of the transactions that were confirmed at
		// heights [startHeight, endHeight]. Unconfirmed transactions are not
		// included.
		Transactions(startHeight types.BlockHeight, endHeight types.BlockHeight) ([]ProcessedTransaction, error)

		// UnconfirmedTransactions returns all unconfirmed transactions
		// relative to the wallet.
		UnconfirmedTransactions() []ProcessedTransaction

		// RegisterTransaction takes a transaction and its parents and returns
		// a TransactionBuilder which can be used to expand the transaction.
		RegisterTransaction(t types.Transaction, parents []types.Transaction) TransactionBuilder

		// Rescanning reports whether the wallet is currently rescanning the
		// blockchain.
		Rescanning() bool

		// StartTransaction is a convenience method that calls
		// RegisterTransaction(types.Transaction{}, nil)
		StartTransaction() TransactionBuilder

		// SendSiacoins is a tool for sending siacoins from the wallet to an
		// address. Sending money usually results in multiple transactions. The
		// transactions are automatically given to the transaction pool, and
		// are also returned to the caller.
		SendSiacoins(amount types.Currency, dest types.UnlockHash) ([]types.Transaction, error)

		// SendSiacoinsMulti sends coins to multiple addresses.
		SendSiacoinsMulti(outputs []types.SiacoinOutput) ([]types.Transaction, error)

		// SendSiafunds is a tool for sending siafunds from the wallet to an
		// address. Sending money usually results in multiple transactions. The
		// transactions are automatically given to the transaction pool, and
		// are also returned to the caller.
		SendSiafunds(amount types.Currency, dest types.UnlockHash) ([]types.Transaction, error)
	}
)

// CalculateWalletTransactionID is a helper function for determining the id of
// a wallet transaction.
func CalculateWalletTransactionID(tid types.TransactionID, oid types.OutputID) WalletTransactionID {
	return WalletTransactionID(crypto.HashAll(tid, oid))
}

// SeedToString converts a wallet seed to a human friendly string.
func SeedToString(seed Seed, did mnemonics.DictionaryID) (string, error) {
	fullChecksum := crypto.HashObject(seed)
	checksumSeed := append(seed[:], fullChecksum[:SeedChecksumSize]...)
	phrase, err := mnemonics.ToPhrase(checksumSeed, did)
	if err != nil {
		return "", err
	}
	return phrase.String(), nil
}

// StringToSeed converts a string to a wallet seed.
func StringToSeed(str string, did mnemonics.DictionaryID) (Seed, error) {
	// Decode the string into the checksummed byte slice.
	checksumSeedBytes, err := mnemonics.FromString(str, did)
	if err != nil {
		return Seed{}, err
	}

	// Copy the seed from the checksummed slice.
	var seed Seed
	copy(seed[:], checksumSeedBytes)
	fullChecksum := crypto.HashObject(seed)
	if len(checksumSeedBytes) != crypto.EntropySize+SeedChecksumSize || !bytes.Equal(fullChecksum[:SeedChecksumSize], checksumSeedBytes[crypto.EntropySize:]) {
		return Seed{}, errors.New("seed failed checksum verification")
	}
	return seed, nil
}
