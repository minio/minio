package proto

import (
	"errors"
	"net"
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"
)

// extendDeadline is a helper function for extending the connection timeout.
func extendDeadline(conn net.Conn, d time.Duration) { _ = conn.SetDeadline(time.Now().Add(d)) }

// startRevision is run at the beginning of each revision iteration. It reads
// the host's settings confirms that the values are acceptable, and writes an acceptance.
func startRevision(conn net.Conn, host modules.HostDBEntry) error {
	// verify the host's settings and confirm its identity
	_, err := verifySettings(conn, host)
	if err != nil {
		return err
	}
	return modules.WriteNegotiationAcceptance(conn)
}

// startDownload is run at the beginning of each download iteration. It reads
// the host's settings confirms that the values are acceptable, and writes an acceptance.
func startDownload(conn net.Conn, host modules.HostDBEntry) error {
	// verify the host's settings and confirm its identity
	_, err := verifySettings(conn, host)
	if err != nil {
		return err
	}
	return modules.WriteNegotiationAcceptance(conn)
}

// verifySettings reads a signed HostSettings object from conn, validates the
// signature, and checks for discrepancies between the known settings and the
// received settings. If there is a discrepancy, the hostDB is notified. The
// received settings are returned.
func verifySettings(conn net.Conn, host modules.HostDBEntry) (modules.HostDBEntry, error) {
	// convert host key (types.SiaPublicKey) to a crypto.PublicKey
	if host.PublicKey.Algorithm != types.SignatureEd25519 || len(host.PublicKey.Key) != crypto.PublicKeySize {
		build.Critical("hostdb did not filter out host with wrong signature algorithm:", host.PublicKey.Algorithm)
		return modules.HostDBEntry{}, errors.New("host used unsupported signature algorithm")
	}
	var pk crypto.PublicKey
	copy(pk[:], host.PublicKey.Key)

	// read signed host settings
	var recvSettings modules.HostExternalSettings
	if err := crypto.ReadSignedObject(conn, &recvSettings, modules.NegotiateMaxHostExternalSettingsLen, pk); err != nil {
		return modules.HostDBEntry{}, errors.New("couldn't read host's settings: " + err.Error())
	}
	// TODO: check recvSettings against host.HostExternalSettings. If there is
	// a discrepancy, write the error to conn.
	if recvSettings.NetAddress != host.NetAddress {
		// for now, just overwrite the NetAddress, since we know that
		// host.NetAddress works (it was the one we dialed to get conn)
		recvSettings.NetAddress = host.NetAddress
	}
	host.HostExternalSettings = recvSettings
	return host, nil
}

// verifyRecentRevision confirms that the host and contractor agree upon the current
// state of the contract being revised.
func verifyRecentRevision(conn net.Conn, contract modules.RenterContract, hostVersion string) error {
	// send contract ID
	if err := encoding.WriteObject(conn, contract.ID); err != nil {
		return errors.New("couldn't send contract ID: " + err.Error())
	}
	// read challenge
	var challenge crypto.Hash
	if err := encoding.ReadObject(conn, &challenge, 32); err != nil {
		return errors.New("couldn't read challenge: " + err.Error())
	}
	if build.VersionCmp(hostVersion, "1.3.0") >= 0 {
		crypto.SecureWipe(challenge[:16])
	}
	// sign and return
	sig := crypto.SignHash(challenge, contract.SecretKey)
	if err := encoding.WriteObject(conn, sig); err != nil {
		return errors.New("couldn't send challenge response: " + err.Error())
	}
	// read acceptance
	if err := modules.ReadNegotiationAcceptance(conn); err != nil {
		return errors.New("host did not accept revision request: " + err.Error())
	}
	// read last revision and signatures
	var lastRevision types.FileContractRevision
	var hostSignatures []types.TransactionSignature
	if err := encoding.ReadObject(conn, &lastRevision, 2048); err != nil {
		return errors.New("couldn't read last revision: " + err.Error())
	}
	if err := encoding.ReadObject(conn, &hostSignatures, 2048); err != nil {
		return errors.New("couldn't read host signatures: " + err.Error())
	}
	// Check that the unlock hashes match; if they do not, something is
	// seriously wrong. Otherwise, check that the revision numbers match.
	if lastRevision.UnlockConditions.UnlockHash() != contract.LastRevision.UnlockConditions.UnlockHash() {
		return errors.New("unlock conditions do not match")
	} else if lastRevision.NewRevisionNumber != contract.LastRevision.NewRevisionNumber {
		return &recentRevisionError{contract.LastRevision.NewRevisionNumber, lastRevision.NewRevisionNumber}
	}
	// NOTE: we can fake the blockheight here because it doesn't affect
	// verification; it just needs to be above the fork height and below the
	// contract expiration (which was checked earlier).
	return modules.VerifyFileContractRevisionTransactionSignatures(lastRevision, hostSignatures, contract.FileContract.WindowStart-1)
}

// negotiateRevision sends a revision and actions to the host for approval,
// completing one iteration of the revision loop.
func negotiateRevision(conn net.Conn, rev types.FileContractRevision, secretKey crypto.SecretKey) (types.Transaction, error) {
	// create transaction containing the revision
	signedTxn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev},
		TransactionSignatures: []types.TransactionSignature{{
			ParentID:       crypto.Hash(rev.ParentID),
			CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
			PublicKeyIndex: 0, // renter key is always first -- see formContract
		}},
	}
	// sign the transaction
	encodedSig := crypto.SignHash(signedTxn.SigHash(0), secretKey)
	signedTxn.TransactionSignatures[0].Signature = encodedSig[:]

	// send the revision
	if err := encoding.WriteObject(conn, rev); err != nil {
		return types.Transaction{}, errors.New("couldn't send revision: " + err.Error())
	}
	// read acceptance
	if err := modules.ReadNegotiationAcceptance(conn); err != nil {
		return types.Transaction{}, errors.New("host did not accept revision: " + err.Error())
	}

	// send the new transaction signature
	if err := encoding.WriteObject(conn, signedTxn.TransactionSignatures[0]); err != nil {
		return types.Transaction{}, errors.New("couldn't send transaction signature: " + err.Error())
	}
	// read the host's acceptance and transaction signature
	// NOTE: if the host sends ErrStopResponse, we should continue processing
	// the revision, but return the error anyway.
	responseErr := modules.ReadNegotiationAcceptance(conn)
	if responseErr != nil && responseErr != modules.ErrStopResponse {
		return types.Transaction{}, errors.New("host did not accept transaction signature: " + responseErr.Error())
	}
	var hostSig types.TransactionSignature
	if err := encoding.ReadObject(conn, &hostSig, 16e3); err != nil {
		return types.Transaction{}, errors.New("couldn't read host's signature: " + err.Error())
	}

	// add the signature to the transaction and verify it
	// NOTE: we can fake the blockheight here because it doesn't affect
	// verification; it just needs to be above the fork height and below the
	// contract expiration (which was checked earlier).
	verificationHeight := rev.NewWindowStart - 1
	signedTxn.TransactionSignatures = append(signedTxn.TransactionSignatures, hostSig)
	if err := signedTxn.StandaloneValid(verificationHeight); err != nil {
		return types.Transaction{}, err
	}

	// if the host sent ErrStopResponse, return it
	return signedTxn, responseErr
}

// newRevision creates a copy of current with its revision number incremented,
// and with cost transferred from the renter to the host.
func newRevision(current types.FileContractRevision, cost types.Currency) types.FileContractRevision {
	rev := current

	// need to manually copy slice memory
	rev.NewValidProofOutputs = make([]types.SiacoinOutput, 2)
	rev.NewMissedProofOutputs = make([]types.SiacoinOutput, 3)
	copy(rev.NewValidProofOutputs, current.NewValidProofOutputs)
	copy(rev.NewMissedProofOutputs, current.NewMissedProofOutputs)

	// move valid payout from renter to host
	rev.NewValidProofOutputs[0].Value = current.NewValidProofOutputs[0].Value.Sub(cost)
	rev.NewValidProofOutputs[1].Value = current.NewValidProofOutputs[1].Value.Add(cost)

	// move missed payout from renter to void
	rev.NewMissedProofOutputs[0].Value = current.NewMissedProofOutputs[0].Value.Sub(cost)
	rev.NewMissedProofOutputs[2].Value = current.NewMissedProofOutputs[2].Value.Add(cost)

	// increment revision number
	rev.NewRevisionNumber++

	return rev
}

// newDownloadRevision revises the current revision to cover the cost of
// downloading data.
func newDownloadRevision(current types.FileContractRevision, downloadCost types.Currency) types.FileContractRevision {
	return newRevision(current, downloadCost)
}

// newUploadRevision revises the current revision to cover the cost of
// uploading a sector.
func newUploadRevision(current types.FileContractRevision, merkleRoot crypto.Hash, price, collateral types.Currency) types.FileContractRevision {
	rev := newRevision(current, price)

	// move collateral from host to void
	rev.NewMissedProofOutputs[1].Value = rev.NewMissedProofOutputs[1].Value.Sub(collateral)
	rev.NewMissedProofOutputs[2].Value = rev.NewMissedProofOutputs[2].Value.Add(collateral)

	// set new filesize and Merkle root
	rev.NewFileSize += modules.SectorSize
	rev.NewFileMerkleRoot = merkleRoot
	return rev
}

// newDeleteRevision revises the current revision to cover the cost of
// deleting a sector.
func newDeleteRevision(current types.FileContractRevision, merkleRoot crypto.Hash) types.FileContractRevision {
	rev := newRevision(current, types.ZeroCurrency)
	rev.NewFileSize -= modules.SectorSize
	rev.NewFileMerkleRoot = merkleRoot
	return rev
}

// newModifyRevision revises the current revision to cover the cost of
// modifying a sector.
func newModifyRevision(current types.FileContractRevision, merkleRoot crypto.Hash, uploadCost types.Currency) types.FileContractRevision {
	rev := newRevision(current, uploadCost)
	rev.NewFileMerkleRoot = merkleRoot
	return rev
}
