package types

// filecontracts.go contains the basic structs and helper functions for file
// contracts.

import (
	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
)

var (
	ProofValid  ProofStatus = true
	ProofMissed ProofStatus = false
)

type (
	// A FileContract is a public record of a storage agreement between a "host"
	// and a "renter." It mandates that a host must submit a storage proof to the
	// network, proving that they still possess the file they have agreed to
	// store.
	//
	// The party must submit the storage proof in a block that is between
	// 'WindowStart' and 'WindowEnd'. Upon submitting the proof, the outputs
	// for 'ValidProofOutputs' are created. If the party does not submit a
	// storage proof by 'WindowEnd', then the outputs for 'MissedProofOutputs'
	// are created instead. The sum of 'MissedProofOutputs' must equal
	// 'Payout', and the sum of 'ValidProofOutputs' must equal 'Payout' plus
	// the siafund fee.  This fee is sent to the siafund pool, which is a set
	// of siacoins only spendable by siafund owners.
	//
	// Under normal circumstances, the payout will be funded by both the host and
	// the renter, which gives the host incentive not to lose the file. The
	// 'ValidProofUnlockHash' will typically be spendable by host, and the
	// 'MissedProofUnlockHash' will either by spendable by the renter or by
	// nobody (the ZeroUnlockHash).
	//
	// A contract can be terminated early by submitting a FileContractTermination
	// whose UnlockConditions hash to 'TerminationHash'.
	FileContract struct {
		FileSize           uint64          `json:"filesize"`
		FileMerkleRoot     crypto.Hash     `json:"filemerkleroot"`
		WindowStart        BlockHeight     `json:"windowstart"`
		WindowEnd          BlockHeight     `json:"windowend"`
		Payout             Currency        `json:"payout"`
		ValidProofOutputs  []SiacoinOutput `json:"validproofoutputs"`
		MissedProofOutputs []SiacoinOutput `json:"missedproofoutputs"`
		UnlockHash         UnlockHash      `json:"unlockhash"`
		RevisionNumber     uint64          `json:"revisionnumber"`
	}

	// A FileContractRevision revises an existing file contract. The ParentID
	// points to the file contract that is being revised. The UnlockConditions
	// are the conditions under which the revision is valid, and must match the
	// UnlockHash of the parent file contract. The Payout of the file contract
	// cannot be changed, but all other fields are allowed to be changed. The
	// sum of the outputs must match the original payout (taking into account
	// the fee for the proof payouts.) A revision number is included. When
	// getting accepted, the revision number of the revision must be higher
	// than any previously seen revision number for that file contract.
	//
	// FileContractRevisions enable trust-free modifications to existing file
	// contracts.
	FileContractRevision struct {
		ParentID          FileContractID   `json:"parentid"`
		UnlockConditions  UnlockConditions `json:"unlockconditions"`
		NewRevisionNumber uint64           `json:"newrevisionnumber"`

		NewFileSize           uint64          `json:"newfilesize"`
		NewFileMerkleRoot     crypto.Hash     `json:"newfilemerkleroot"`
		NewWindowStart        BlockHeight     `json:"newwindowstart"`
		NewWindowEnd          BlockHeight     `json:"newwindowend"`
		NewValidProofOutputs  []SiacoinOutput `json:"newvalidproofoutputs"`
		NewMissedProofOutputs []SiacoinOutput `json:"newmissedproofoutputs"`
		NewUnlockHash         UnlockHash      `json:"newunlockhash"`
	}

	// A StorageProof fulfills a FileContract. The proof contains a specific
	// segment of the file, along with a set of hashes from the file's Merkle
	// tree. In combination, these can be used to prove that the segment came
	// from the file. To prevent abuse, the segment must be chosen randomly, so
	// the ID of block 'WindowStart' - 1 is used as a seed value; see
	// StorageProofSegment for the exact implementation.
	//
	// A transaction with a StorageProof cannot have any SiacoinOutputs,
	// SiafundOutputs, or FileContracts. This is because a mundane reorg can
	// invalidate the proof, and with it the rest of the transaction.
	StorageProof struct {
		ParentID FileContractID           `json:"parentid"`
		Segment  [crypto.SegmentSize]byte `json:"segment"`
		HashSet  []crypto.Hash            `json:"hashset"`
	}

	ProofStatus bool
)

// StorageProofOutputID returns the ID of an output created by a file
// contract, given the status of the storage proof. The ID is calculating by
// hashing the concatenation of the StorageProofOutput Specifier, the ID of
// the file contract that the proof is for, a boolean indicating whether the
// proof was valid (true) or missed (false), and the index of the output
// within the file contract.
func (fcid FileContractID) StorageProofOutputID(proofStatus ProofStatus, i uint64) SiacoinOutputID {
	return SiacoinOutputID(crypto.HashAll(
		SpecifierStorageProofOutput,
		fcid,
		proofStatus,
		i,
	))
}

// PostTax returns the amount of currency remaining in a file contract payout
// after tax.
func PostTax(height BlockHeight, payout Currency) Currency {
	return payout.Sub(Tax(height, payout))
}

// Tax returns the amount of Currency that will be taxed from fc.
func Tax(height BlockHeight, payout Currency) Currency {
	// COMPATv0.4.0 - until the first 20,000 blocks have been archived, they
	// will need to be handled in a special way.
	if (height < 21e3 && build.Release == "standard") || (height < 10 && build.Release == "testing") {
		return payout.MulFloat(0.039).RoundDown(SiafundCount)
	}
	return payout.MulTax().RoundDown(SiafundCount)
}
