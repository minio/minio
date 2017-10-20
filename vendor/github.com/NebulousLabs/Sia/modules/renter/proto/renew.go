package proto

import (
	"errors"
	"net"

	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
)

// Renew negotiates a new contract for data already stored with a host, and
// submits the new contract transaction to tpool.
func Renew(contract modules.RenterContract, params ContractParams, txnBuilder transactionBuilder, tpool transactionPool, hdb hostDB, cancel <-chan struct{}) (modules.RenterContract, error) {
	// extract vars from params, for convenience
	host, filesize, startHeight, endHeight, refundAddress := params.Host, params.Filesize, params.StartHeight, params.EndHeight, params.RefundAddress
	ourSK := contract.SecretKey

	// calculate cost to renter and cost to host
	storageAllocation := host.StoragePrice.Mul64(filesize).Mul64(uint64(endHeight - startHeight))
	hostCollateral := host.Collateral.Mul64(filesize).Mul64(uint64(endHeight - startHeight))
	if hostCollateral.Cmp(host.MaxCollateral) > 0 {
		// TODO: if we have to cap the collateral, it probably means we shouldn't be using this host
		// (ok within a factor of 2)
		hostCollateral = host.MaxCollateral
	}

	// Calculate additional basePrice and baseCollateral. If the contract
	// height did not increase, basePrice and baseCollateral are zero.
	var basePrice, baseCollateral types.Currency
	if endHeight+host.WindowSize > contract.LastRevision.NewWindowEnd {
		timeExtension := uint64((endHeight + host.WindowSize) - contract.LastRevision.NewWindowEnd)
		basePrice = host.StoragePrice.Mul64(contract.LastRevision.NewFileSize).Mul64(timeExtension)    // cost of data already covered by contract, i.e. lastrevision.Filesize
		baseCollateral = host.Collateral.Mul64(contract.LastRevision.NewFileSize).Mul64(timeExtension) // same but collateral
	}

	hostPayout := hostCollateral.Add(host.ContractPrice).Add(basePrice)
	payout := storageAllocation.Add(hostCollateral.Add(host.ContractPrice)).Mul64(10406).Div64(10000) // renter covers siafund fee

	// check for negative currency
	if types.PostTax(startHeight, payout).Cmp(hostPayout) < 0 {
		return modules.RenterContract{}, errors.New("payout smaller than host payout")
	} else if hostCollateral.Cmp(baseCollateral) < 0 {
		return modules.RenterContract{}, errors.New("new collateral smaller than old collateral")
	}

	// create file contract
	fc := types.FileContract{
		FileSize:       contract.LastRevision.NewFileSize,
		FileMerkleRoot: contract.LastRevision.NewFileMerkleRoot,
		WindowStart:    endHeight,
		WindowEnd:      endHeight + host.WindowSize,
		Payout:         payout,
		UnlockHash:     contract.LastRevision.NewUnlockHash,
		RevisionNumber: 0,
		ValidProofOutputs: []types.SiacoinOutput{
			// renter
			{Value: types.PostTax(startHeight, payout).Sub(hostPayout), UnlockHash: refundAddress},
			// host
			{Value: hostPayout, UnlockHash: host.UnlockHash},
		},
		MissedProofOutputs: []types.SiacoinOutput{
			// renter
			{Value: types.PostTax(startHeight, payout).Sub(hostPayout), UnlockHash: refundAddress},
			// host gets its unused collateral back, plus the contract price
			{Value: hostCollateral.Sub(baseCollateral).Add(host.ContractPrice), UnlockHash: host.UnlockHash},
			// void gets the spent storage fees, plus the collateral being risked
			{Value: basePrice.Add(baseCollateral), UnlockHash: types.UnlockHash{}},
		},
	}

	// calculate transaction fee
	_, maxFee := tpool.FeeEstimation()
	txnFee := maxFee.Mul64(estTxnSize)

	// build transaction containing fc
	renterCost := payout.Sub(hostCollateral).Add(txnFee)
	err := txnBuilder.FundSiacoins(renterCost)
	if err != nil {
		return modules.RenterContract{}, err
	}
	txnBuilder.AddFileContract(fc)

	// add miner fee
	txnBuilder.AddMinerFee(txnFee)

	// create initial transaction set
	txn, parentTxns := txnBuilder.View()
	txnSet := append(parentTxns, txn)

	// Increase Successful/Failed interactions accordingly
	defer func() {
		// A revision mismatch might not be the host's fault.
		if err != nil && !IsRevisionMismatch(err) {
			hdb.IncrementFailedInteractions(contract.HostPublicKey)
		} else if err == nil {
			hdb.IncrementSuccessfulInteractions(contract.HostPublicKey)
		}
	}()

	// initiate connection
	dialer := &net.Dialer{
		Cancel:  cancel,
		Timeout: connTimeout,
	}
	conn, err := dialer.Dial("tcp", string(host.NetAddress))
	if err != nil {
		return modules.RenterContract{}, err
	}
	defer func() { _ = conn.Close() }()

	// allot time for sending RPC ID, verifyRecentRevision, and verifySettings
	extendDeadline(conn, modules.NegotiateRecentRevisionTime+modules.NegotiateSettingsTime)
	if err = encoding.WriteObject(conn, modules.RPCRenewContract); err != nil {
		return modules.RenterContract{}, errors.New("couldn't initiate RPC: " + err.Error())
	}
	// verify that both parties are renewing the same contract
	if err = verifyRecentRevision(conn, contract, host.Version); err != nil {
		// don't add context; want to preserve the original error type so that
		// callers can check using IsRevisionMismatch
		return modules.RenterContract{}, err
	}
	// verify the host's settings and confirm its identity
	host, err = verifySettings(conn, host)
	if err != nil {
		return modules.RenterContract{}, errors.New("settings exchange failed: " + err.Error())
	}
	if !host.AcceptingContracts {
		return modules.RenterContract{}, errors.New("host is not accepting contracts")
	}

	// allot time for negotiation
	extendDeadline(conn, modules.NegotiateRenewContractTime)

	// send acceptance, txn signed by us, and pubkey
	if err = modules.WriteNegotiationAcceptance(conn); err != nil {
		return modules.RenterContract{}, errors.New("couldn't send initial acceptance: " + err.Error())
	}
	if err = encoding.WriteObject(conn, txnSet); err != nil {
		return modules.RenterContract{}, errors.New("couldn't send the contract signed by us: " + err.Error())
	}
	if err = encoding.WriteObject(conn, ourSK.PublicKey()); err != nil {
		return modules.RenterContract{}, errors.New("couldn't send our public key: " + err.Error())
	}

	// read acceptance and txn signed by host
	if err = modules.ReadNegotiationAcceptance(conn); err != nil {
		return modules.RenterContract{}, errors.New("host did not accept our proposed contract: " + err.Error())
	}
	// host now sends any new parent transactions, inputs and outputs that
	// were added to the transaction
	var newParents []types.Transaction
	var newInputs []types.SiacoinInput
	var newOutputs []types.SiacoinOutput
	if err = encoding.ReadObject(conn, &newParents, types.BlockSizeLimit); err != nil {
		return modules.RenterContract{}, errors.New("couldn't read the host's added parents: " + err.Error())
	}
	if err = encoding.ReadObject(conn, &newInputs, types.BlockSizeLimit); err != nil {
		return modules.RenterContract{}, errors.New("couldn't read the host's added inputs: " + err.Error())
	}
	if err = encoding.ReadObject(conn, &newOutputs, types.BlockSizeLimit); err != nil {
		return modules.RenterContract{}, errors.New("couldn't read the host's added outputs: " + err.Error())
	}

	// merge txnAdditions with txnSet
	txnBuilder.AddParents(newParents)
	for _, input := range newInputs {
		txnBuilder.AddSiacoinInput(input)
	}
	for _, output := range newOutputs {
		txnBuilder.AddSiacoinOutput(output)
	}

	// sign the txn
	signedTxnSet, err := txnBuilder.Sign(true)
	if err != nil {
		return modules.RenterContract{}, modules.WriteNegotiationRejection(conn, errors.New("failed to sign transaction: "+err.Error()))
	}

	// calculate signatures added by the transaction builder
	var addedSignatures []types.TransactionSignature
	_, _, _, addedSignatureIndices := txnBuilder.ViewAdded()
	for _, i := range addedSignatureIndices {
		addedSignatures = append(addedSignatures, signedTxnSet[len(signedTxnSet)-1].TransactionSignatures[i])
	}

	// create initial (no-op) revision, transaction, and signature
	initRevision := types.FileContractRevision{
		ParentID:          signedTxnSet[len(signedTxnSet)-1].FileContractID(0),
		UnlockConditions:  contract.LastRevision.UnlockConditions,
		NewRevisionNumber: 1,

		NewFileSize:           fc.FileSize,
		NewFileMerkleRoot:     fc.FileMerkleRoot,
		NewWindowStart:        fc.WindowStart,
		NewWindowEnd:          fc.WindowEnd,
		NewValidProofOutputs:  fc.ValidProofOutputs,
		NewMissedProofOutputs: fc.MissedProofOutputs,
		NewUnlockHash:         fc.UnlockHash,
	}
	renterRevisionSig := types.TransactionSignature{
		ParentID:       crypto.Hash(initRevision.ParentID),
		PublicKeyIndex: 0,
		CoveredFields: types.CoveredFields{
			FileContractRevisions: []uint64{0},
		},
	}
	revisionTxn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{initRevision},
		TransactionSignatures: []types.TransactionSignature{renterRevisionSig},
	}
	encodedSig := crypto.SignHash(revisionTxn.SigHash(0), ourSK)
	revisionTxn.TransactionSignatures[0].Signature = encodedSig[:]

	// Send acceptance and signatures
	if err = modules.WriteNegotiationAcceptance(conn); err != nil {
		return modules.RenterContract{}, errors.New("couldn't send transaction acceptance: " + err.Error())
	}
	if err = encoding.WriteObject(conn, addedSignatures); err != nil {
		return modules.RenterContract{}, errors.New("couldn't send added signatures: " + err.Error())
	}
	if err = encoding.WriteObject(conn, revisionTxn.TransactionSignatures[0]); err != nil {
		return modules.RenterContract{}, errors.New("couldn't send revision signature: " + err.Error())
	}

	// Read the host acceptance and signatures.
	err = modules.ReadNegotiationAcceptance(conn)
	if err != nil {
		return modules.RenterContract{}, errors.New("host did not accept our signatures: " + err.Error())
	}
	var hostSigs []types.TransactionSignature
	if err = encoding.ReadObject(conn, &hostSigs, 2e3); err != nil {
		return modules.RenterContract{}, errors.New("couldn't read the host's signatures: " + err.Error())
	}
	for _, sig := range hostSigs {
		txnBuilder.AddTransactionSignature(sig)
	}
	var hostRevisionSig types.TransactionSignature
	if err = encoding.ReadObject(conn, &hostRevisionSig, 2e3); err != nil {
		return modules.RenterContract{}, errors.New("couldn't read the host's revision signature: " + err.Error())
	}
	revisionTxn.TransactionSignatures = append(revisionTxn.TransactionSignatures, hostRevisionSig)

	// Construct the final transaction.
	txn, parentTxns = txnBuilder.View()
	txnSet = append(parentTxns, txn)

	// Submit to blockchain.
	err = tpool.AcceptTransactionSet(txnSet)
	if err == modules.ErrDuplicateTransactionSet {
		// as long as it made it into the transaction pool, we're good
		err = nil
	}
	if err != nil {
		return modules.RenterContract{}, err
	}

	// calculate contract ID
	fcid := txn.FileContractID(0)

	return modules.RenterContract{
		FileContract:    fc,
		HostPublicKey:   host.PublicKey,
		ID:              fcid,
		LastRevision:    initRevision,
		LastRevisionTxn: revisionTxn,
		MerkleRoots:     contract.MerkleRoots,
		NetAddress:      host.NetAddress,
		SecretKey:       ourSK,
		StartHeight:     startHeight,

		TotalCost:   renterCost,
		ContractFee: host.ContractPrice,
		TxnFee:      txnFee,
		SiafundFee:  types.Tax(startHeight, fc.Payout),
	}, nil
}
