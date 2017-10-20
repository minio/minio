package types

// validtransaction.go has functions for checking whether a transaction is
// valid outside of the context of a consensus set. This means checking the
// size of the transaction, the content of the signatures, and a large set of
// other rules that are inherent to how a transaction should be constructed.

import (
	"errors"
)

var (
	ErrDoubleSpend                      = errors.New("transaction uses a parent object twice")
	ErrFileContractWindowEndViolation   = errors.New("file contract window must end at least one block after it starts")
	ErrFileContractWindowStartViolation = errors.New("file contract window must start in the future")
	ErrFileContractOutputSumViolation   = errors.New("file contract has invalid output sums")
	ErrNonZeroClaimStart                = errors.New("transaction has a siafund output with a non-zero siafund claim")
	ErrNonZeroRevision                  = errors.New("new file contract has a nonzero revision number")
	ErrStorageProofWithOutputs          = errors.New("transaction has both a storage proof and other outputs")
	ErrTimelockNotSatisfied             = errors.New("timelock has not been met")
	ErrTransactionTooLarge              = errors.New("transaction is too large to fit in a block")
	ErrZeroMinerFee                     = errors.New("transaction has a zero value miner fee")
	ErrZeroOutput                       = errors.New("transaction cannot have an output or payout that has zero value")
	ErrZeroRevision                     = errors.New("transaction has a file contract revision with RevisionNumber=0")
)

// correctFileContracts checks that the file contracts adhere to the file
// contract rules.
func (t Transaction) correctFileContracts(currentHeight BlockHeight) error {
	// Check that FileContract rules are being followed.
	for _, fc := range t.FileContracts {
		// Check that start and expiration are reasonable values.
		if fc.WindowStart <= currentHeight {
			return ErrFileContractWindowStartViolation
		}
		if fc.WindowEnd <= fc.WindowStart {
			return ErrFileContractWindowEndViolation
		}

		// Check that the proof outputs sum to the payout after the
		// siafund fee has been applied.
		var validProofOutputSum, missedProofOutputSum Currency
		for _, output := range fc.ValidProofOutputs {
			/* - Future hardforking code.
			if output.Value.IsZero() {
				return ErrZeroOutput
			}
			*/
			validProofOutputSum = validProofOutputSum.Add(output.Value)
		}
		for _, output := range fc.MissedProofOutputs {
			/* - Future hardforking code.
			if output.Value.IsZero() {
				return ErrZeroOutput
			}
			*/
			missedProofOutputSum = missedProofOutputSum.Add(output.Value)
		}
		outputPortion := PostTax(currentHeight, fc.Payout)
		if validProofOutputSum.Cmp(outputPortion) != 0 {
			return ErrFileContractOutputSumViolation
		}
		if missedProofOutputSum.Cmp(outputPortion) != 0 {
			return ErrFileContractOutputSumViolation
		}
	}
	return nil
}

// correctFileContractRevisions checks that any file contract revisions adhere
// to the revision rules.
func (t Transaction) correctFileContractRevisions(currentHeight BlockHeight) error {
	for _, fcr := range t.FileContractRevisions {
		// Check that start and expiration are reasonable values.
		if fcr.NewWindowStart <= currentHeight {
			return ErrFileContractWindowStartViolation
		}
		if fcr.NewWindowEnd <= fcr.NewWindowStart {
			return ErrFileContractWindowEndViolation
		}

		// Check that the valid outputs and missed outputs sum to the same
		// value.
		var validProofOutputSum, missedProofOutputSum Currency
		for _, output := range fcr.NewValidProofOutputs {
			/* - Future hardforking code.
			if output.Value.IsZero() {
				return ErrZeroOutput
			}
			*/
			validProofOutputSum = validProofOutputSum.Add(output.Value)
		}
		for _, output := range fcr.NewMissedProofOutputs {
			/* - Future hardforking code.
			if output.Value.IsZero() {
				return ErrZeroOutput
			}
			*/
			missedProofOutputSum = missedProofOutputSum.Add(output.Value)
		}
		if validProofOutputSum.Cmp(missedProofOutputSum) != 0 {
			return ErrFileContractOutputSumViolation
		}
	}
	return nil
}

// fitsInABlock checks if the transaction is likely to fit in a block. After
// OakHardforkHeight, transactions must be smaller than 64 KiB.
func (t Transaction) fitsInABlock(currentHeight BlockHeight) error {
	// Check that the transaction will fit inside of a block, leaving 5kb for
	// overhead.
	size := uint64(t.MarshalSiaSize())
	if size > BlockSizeLimit-5e3 {
		return ErrTransactionTooLarge
	}
	if currentHeight >= OakHardforkBlock {
		if size > OakHardforkTxnSizeLimit {
			return ErrTransactionTooLarge
		}
	}
	return nil
}

// followsMinimumValues checks that all outputs adhere to the rules for the
// minimum allowed value (generally 1).
func (t Transaction) followsMinimumValues() error {
	for _, sco := range t.SiacoinOutputs {
		if sco.Value.IsZero() {
			return ErrZeroOutput
		}
	}
	for _, fc := range t.FileContracts {
		if fc.Payout.IsZero() {
			return ErrZeroOutput
		}
	}
	for _, sfo := range t.SiafundOutputs {
		// SiafundOutputs are special in that they have a reserved field, the
		// ClaimStart, which gets sent over the wire but must always be set to
		// 0. The Value must always be greater than 0.
		if !sfo.ClaimStart.IsZero() {
			return ErrNonZeroClaimStart
		}
		if sfo.Value.IsZero() {
			return ErrZeroOutput
		}
	}
	for _, fee := range t.MinerFees {
		if fee.IsZero() {
			return ErrZeroMinerFee
		}
	}
	return nil
}

// FollowsStorageProofRules checks that a transaction follows the limitations
// placed on transactions that have storage proofs.
func (t Transaction) followsStorageProofRules() error {
	// No storage proofs, no problems.
	if len(t.StorageProofs) == 0 {
		return nil
	}

	// If there are storage proofs, there can be no siacoin outputs, siafund
	// outputs, new file contracts, or file contract terminations. These
	// restrictions are in place because a storage proof can be invalidated by
	// a simple reorg, which will also invalidate the rest of the transaction.
	// These restrictions minimize blockchain turbulence. These other types
	// cannot be invalidated by a simple reorg, and must instead by replaced by
	// a conflicting transaction.
	if len(t.SiacoinOutputs) != 0 {
		return ErrStorageProofWithOutputs
	}
	if len(t.FileContracts) != 0 {
		return ErrStorageProofWithOutputs
	}
	if len(t.FileContractRevisions) != 0 {
		return ErrStorageProofWithOutputs
	}
	if len(t.SiafundOutputs) != 0 {
		return ErrStorageProofWithOutputs
	}

	return nil
}

// noRepeats checks that a transaction does not spend multiple outputs twice,
// submit two valid storage proofs for the same file contract, etc. We
// frivolously check that a file contract termination and storage proof don't
// act on the same file contract. There is very little overhead for doing so,
// and the check is only frivolous because of the current rule that file
// contract terminations are not valid after the proof window opens.
func (t Transaction) noRepeats() error {
	// Check that there are no repeat instances of siacoin outputs, storage
	// proofs, contract terminations, or siafund outputs.
	siacoinInputs := make(map[SiacoinOutputID]struct{})
	for _, sci := range t.SiacoinInputs {
		_, exists := siacoinInputs[sci.ParentID]
		if exists {
			return ErrDoubleSpend
		}
		siacoinInputs[sci.ParentID] = struct{}{}
	}
	doneFileContracts := make(map[FileContractID]struct{})
	for _, sp := range t.StorageProofs {
		_, exists := doneFileContracts[sp.ParentID]
		if exists {
			return ErrDoubleSpend
		}
		doneFileContracts[sp.ParentID] = struct{}{}
	}
	for _, fcr := range t.FileContractRevisions {
		_, exists := doneFileContracts[fcr.ParentID]
		if exists {
			return ErrDoubleSpend
		}
		doneFileContracts[fcr.ParentID] = struct{}{}
	}
	siafundInputs := make(map[SiafundOutputID]struct{})
	for _, sfi := range t.SiafundInputs {
		_, exists := siafundInputs[sfi.ParentID]
		if exists {
			return ErrDoubleSpend
		}
		siafundInputs[sfi.ParentID] = struct{}{}
	}
	return nil
}

// validUnlockConditions checks that the conditions of uc have been met. The
// height is taken as input so that modules who might be at a different height
// can do the verification without needing to use their own function.
// Additionally, it means that the function does not need to be a method of the
// consensus set.
func validUnlockConditions(uc UnlockConditions, currentHeight BlockHeight) (err error) {
	if uc.Timelock > currentHeight {
		return ErrTimelockNotSatisfied
	}
	return
}

// validUnlockConditions checks that all of the unlock conditions in the
// transaction are valid.
func (t Transaction) validUnlockConditions(currentHeight BlockHeight) (err error) {
	for _, sci := range t.SiacoinInputs {
		err = validUnlockConditions(sci.UnlockConditions, currentHeight)
		if err != nil {
			return
		}
	}
	for _, fcr := range t.FileContractRevisions {
		err = validUnlockConditions(fcr.UnlockConditions, currentHeight)
		if err != nil {
			return
		}
	}
	for _, sfi := range t.SiafundInputs {
		err = validUnlockConditions(sfi.UnlockConditions, currentHeight)
		if err != nil {
			return
		}
	}
	return
}

// StandaloneValid returns an error if a transaction is not valid in any
// context, for example if the same output is spent twice in the same
// transaction. StandaloneValid will not check that all outputs being spent are
// legal outputs, as it has no confirmed or unconfirmed set to look at.
func (t Transaction) StandaloneValid(currentHeight BlockHeight) (err error) {
	err = t.fitsInABlock(currentHeight)
	if err != nil {
		return
	}
	err = t.followsStorageProofRules()
	if err != nil {
		return
	}
	err = t.noRepeats()
	if err != nil {
		return
	}
	err = t.followsMinimumValues()
	if err != nil {
		return
	}
	err = t.correctFileContracts(currentHeight)
	if err != nil {
		return
	}
	err = t.correctFileContractRevisions(currentHeight)
	if err != nil {
		return
	}
	err = t.validUnlockConditions(currentHeight)
	if err != nil {
		return
	}
	err = t.validSignatures(currentHeight)
	if err != nil {
		return
	}
	return
}
