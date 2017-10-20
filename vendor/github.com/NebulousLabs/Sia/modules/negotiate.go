package modules

import (
	"bytes"
	"errors"
	"io"
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/crypto"
	"github.com/NebulousLabs/Sia/encoding"
	"github.com/NebulousLabs/Sia/types"
)

const (
	// AcceptResponse is the response given to an RPC call to indicate
	// acceptance, i.e. that the sender wishes to continue communication.
	AcceptResponse = "accept"

	// StopResponse is the response given to an RPC call to indicate graceful
	// termination, i.e. that the sender wishes to cease communication, but
	// not due to an error.
	StopResponse = "stop"

	// NegotiateDownloadTime defines the amount of time that the renter and
	// host have to negotiate a download request batch. The time is set high
	// enough that two nodes behind Tor have a reasonable chance of completing
	// the negotiation.
	NegotiateDownloadTime = 600 * time.Second

	// NegotiateFileContractTime defines the amount of time that the renter and
	// host have to negotiate a file contract. The time is set high enough that
	// a node behind Tor has a reasonable chance at making the multiple
	// required round trips to complete the negotiation.
	NegotiateFileContractTime = 360 * time.Second

	// NegotiateFileContractRevisionTime defines the minimum amount of time
	// that the renter and host have to negotiate a file contract revision. The
	// time is set high enough that a full 4MB can be piped through a
	// connection that is running over Tor.
	NegotiateFileContractRevisionTime = 600 * time.Second

	// NegotiateRecentRevisionTime establishes the minimum amount of time that
	// the connection deadline is expected to be set to when a recent file
	// contract revision is being requested from the host. The deadline is long
	// enough that the connection should be successful even if both parties are
	// running Tor.
	NegotiateRecentRevisionTime = 120 * time.Second

	// NegotiateRenewContractTime defines the minimum amount of time that the
	// renter and host have to negotiate a final contract renewal. The time is
	// high enough that the negotiation can occur over a Tor connection, and
	// that both the host and the renter can have time to process large Merkle
	// tree calculations that may be involved with renewing a file contract.
	NegotiateRenewContractTime = 600 * time.Second

	// NegotiateSettingsTime establishes the minimum amount of time that the
	// connection deadline is expected to be set to when settings are being
	// requested from the host. The deadline is long enough that the connection
	// should be successful even if both parties are on Tor.
	NegotiateSettingsTime = 120 * time.Second

	// NegotiateMaxDownloadActionRequestSize defines the maximum size that a
	// download request can be. Note, this is not a max size for the data that
	// can be requested, but instead is a max size for the definition of the
	// data being requested.
	NegotiateMaxDownloadActionRequestSize = 50e3

	// NegotiateMaxErrorSize indicates the maximum number of bytes that can be
	// used to encode an error being sent during negotiation.
	NegotiateMaxErrorSize = 256

	// NegotiateMaxFileContractRevisionSize specifies the maximum size that a
	// file contract revision is allowed to have when being sent over the wire
	// during negotiation.
	NegotiateMaxFileContractRevisionSize = 3e3

	// NegotiateMaxFileContractSetLen determines the maximum allowed size of a
	// transaction set that can be sent when trying to negotiate a file
	// contract. The transaction set will contain all of the unconfirmed
	// dependencies of the file contract, meaning that it can be quite large.
	// The transaction pool's size limit for transaction sets has been chosen
	// as a reasonable guideline for determining what is too large.
	NegotiateMaxFileContractSetLen = TransactionSetSizeLimit - 1e3

	// NegotiateMaxHostExternalSettingsLen is the maximum allowed size of an
	// encoded HostExternalSettings.
	NegotiateMaxHostExternalSettingsLen = 16000

	// NegotiateMaxSiaPubkeySize defines the maximum size that a SiaPubkey is
	// allowed to be when being sent over the wire during negotiation.
	NegotiateMaxSiaPubkeySize = 1e3

	// NegotiateMaxTransactionSignatureSize defines the maximum size that a
	// transaction signature is allowed to be when being sent over the wire
	// during negotiation.
	NegotiateMaxTransactionSignatureSize = 2e3

	// NegotiateMaxTransactionSignaturesSize defines the maximum size that a
	// transaction signature slice is allowed to be when being sent over the
	// wire during negotiation.
	NegotiateMaxTransactionSignaturesSize = 5e3
)

var (
	// ActionDelete is the specifier for a RevisionAction that deletes a
	// sector.
	ActionDelete = types.Specifier{'D', 'e', 'l', 'e', 't', 'e'}

	// ActionInsert is the specifier for a RevisionAction that inserts a
	// sector.
	ActionInsert = types.Specifier{'I', 'n', 's', 'e', 'r', 't'}

	// ActionModify is the specifier for a RevisionAction that modifies sector
	// data.
	ActionModify = types.Specifier{'M', 'o', 'd', 'i', 'f', 'y'}

	// ErrAnnNotAnnouncement indicates that the provided host announcement does
	// not use a recognized specifier, indicating that it's either not a host
	// announcement or it's not a recognized version of a host announcement.
	ErrAnnNotAnnouncement = errors.New("provided data does not form a recognized host announcement")

	// ErrAnnUnrecognizedSignature is returned when the signature in a host
	// announcement is not a type of signature that is recognized.
	ErrAnnUnrecognizedSignature = errors.New("the signature provided in the host announcement is not recognized")

	// ErrRevisionCoveredFields is returned if there is a covered fields object
	// in a transaction signature which has the 'WholeTransaction' field set to
	// true, meaning that miner fees cannot be added to the transaction without
	// invalidating the signature.
	ErrRevisionCoveredFields = errors.New("file contract revision transaction signature does not allow miner fees to be added")

	// ErrRevisionSigCount is returned when a file contract revision has the
	// wrong number of transaction signatures.
	ErrRevisionSigCount = errors.New("file contract revision has the wrong number of transaction signatures")

	// ErrStopResponse is the error returned by ReadNegotiationAcceptance when
	// it reads the StopResponse string.
	ErrStopResponse = errors.New("sender wishes to stop communicating")

	// PrefixHostAnnouncement is used to indicate that a transaction's
	// Arbitrary Data field contains a host announcement. The encoded
	// announcement will follow this prefix.
	PrefixHostAnnouncement = types.Specifier{'H', 'o', 's', 't', 'A', 'n', 'n', 'o', 'u', 'n', 'c', 'e', 'm', 'e', 'n', 't'}

	// RPCDownload is the specifier for downloading a file from a host.
	RPCDownload = types.Specifier{'D', 'o', 'w', 'n', 'l', 'o', 'a', 'd', 2}

	// RPCFormContract is the specifier for forming a contract with a host.
	RPCFormContract = types.Specifier{'F', 'o', 'r', 'm', 'C', 'o', 'n', 't', 'r', 'a', 'c', 't', 2}

	// RPCRenewContract is the specifier to renewing an existing contract.
	RPCRenewContract = types.Specifier{'R', 'e', 'n', 'e', 'w', 'C', 'o', 'n', 't', 'r', 'a', 'c', 't', 2}

	// RPCReviseContract is the specifier for revising an existing file
	// contract.
	RPCReviseContract = types.Specifier{'R', 'e', 'v', 'i', 's', 'e', 'C', 'o', 'n', 't', 'r', 'a', 'c', 't', 2}

	// RPCRecentRevision is the specifier for getting the most recent file
	// contract revision for a given file contract.
	RPCRecentRevision = types.Specifier{'R', 'e', 'c', 'e', 'n', 't', 'R', 'e', 'v', 'i', 's', 'i', 'o', 'n', 2}

	// RPCSettings is the specifier for requesting settings from the host.
	RPCSettings = types.Specifier{'S', 'e', 't', 't', 'i', 'n', 'g', 's', 2}

	// SectorSize defines how large a sector should be in bytes. The sector
	// size needs to be a power of two to be compatible with package
	// merkletree. 4MB has been chosen for the live network because large
	// sectors significantly reduce the tracking overhead experienced by the
	// renter and the host.
	SectorSize = build.Select(build.Var{
		Dev:      uint64(1 << 18), // 256 KiB
		Standard: uint64(1 << 22), // 4 MiB
		Testing:  uint64(1 << 12), // 4 KiB
	}).(uint64)
)

type (
	// A DownloadAction is a description of a download that the renter would
	// like to make. The MerkleRoot indicates the root of the sector, the
	// offset indicates what portion of the sector is being downloaded, and the
	// length indicates how many bytes should be grabbed starting from the
	// offset.
	DownloadAction struct {
		MerkleRoot crypto.Hash
		Offset     uint64
		Length     uint64
	}

	// HostAnnouncement is an announcement by the host that appears in the
	// blockchain. 'Specifier' is always 'PrefixHostAnnouncement'. The
	// announcement is always followed by a signature from the public key of
	// the whole announcement.
	HostAnnouncement struct {
		Specifier  types.Specifier
		NetAddress NetAddress
		PublicKey  types.SiaPublicKey
	}

	// HostExternalSettings are the parameters advertised by the host. These
	// are the values that the renter will request from the host in order to
	// build its database.
	HostExternalSettings struct {
		// MaxBatchSize indicates the maximum size in bytes that a batch is
		// allowed to be. A batch is an array of revision actions; each
		// revision action can have a different number of bytes, depending on
		// the action, so the number of revision actions allowed depends on the
		// sizes of each.
		AcceptingContracts   bool              `json:"acceptingcontracts"`
		MaxDownloadBatchSize uint64            `json:"maxdownloadbatchsize"`
		MaxDuration          types.BlockHeight `json:"maxduration"`
		MaxReviseBatchSize   uint64            `json:"maxrevisebatchsize"`
		NetAddress           NetAddress        `json:"netaddress"`
		RemainingStorage     uint64            `json:"remainingstorage"`
		SectorSize           uint64            `json:"sectorsize"`
		TotalStorage         uint64            `json:"totalstorage"`
		UnlockHash           types.UnlockHash  `json:"unlockhash"`
		WindowSize           types.BlockHeight `json:"windowsize"`

		// Collateral is the amount of collateral that the host will put up for
		// storage in 'bytes per block', as an assurance to the renter that the
		// host really is committed to keeping the file. But, because the file
		// contract is created with no data available, this does leave the host
		// exposed to an attack by a wealthy renter whereby the renter causes
		// the host to lockup in-advance a bunch of funds that the renter then
		// never uses, meaning the host will not have collateral for other
		// clients.
		//
		// MaxCollateral indicates the maximum number of coins that a host is
		// willing to put into a file contract.
		Collateral    types.Currency `json:"collateral"`
		MaxCollateral types.Currency `json:"maxcollateral"`

		// ContractPrice is the number of coins that the renter needs to pay to
		// the host just to open a file contract with them. Generally, the
		// price is only to cover the siacoin fees that the host will suffer
		// when submitting the file contract revision and storage proof to the
		// blockchain.
		//
		// The storage price is the cost per-byte-per-block in hastings of
		// storing data on the host.
		//
		// 'Download' bandwidth price is the cost per byte of downloading data
		// from the host.
		//
		// 'Upload' bandwidth price is the cost per byte of uploading data to
		// the host.
		ContractPrice          types.Currency `json:"contractprice"`
		DownloadBandwidthPrice types.Currency `json:"downloadbandwidthprice"`
		StoragePrice           types.Currency `json:"storageprice"`
		UploadBandwidthPrice   types.Currency `json:"uploadbandwidthprice"`

		// Because the host has a public key, and settings are signed, and
		// because settings may be MITM'd, settings need a revision number so
		// that a renter can compare multiple sets of settings and determine
		// which is the most recent.
		RevisionNumber uint64 `json:"revisionnumber"`
		Version        string `json:"version"`
	}

	// A RevisionAction is a description of an edit to be performed on a file
	// contract. Three types are allowed, 'ActionDelete', 'ActionInsert', and
	// 'ActionModify'. ActionDelete just takes a sector index, indicating which
	// sector is going to be deleted. ActionInsert takes a sector index, and a
	// full sector of data, indicating that a sector at the index should be
	// inserted with the provided data. 'Modify' revises the sector at the
	// given index, rewriting it with the provided data starting from the
	// 'offset' within the sector.
	//
	// Modify could be simulated with an insert and a delete, however an insert
	// requires a full sector to be uploaded, and a modify can be just a few
	// kb, which can be significantly faster.
	RevisionAction struct {
		Type        types.Specifier
		SectorIndex uint64
		Offset      uint64
		Data        []byte
	}
)

// ReadNegotiationAcceptance reads an accept/reject response from r (usually a
// net.Conn). If the response is not AcceptResponse, ReadNegotiationAcceptance
// returns the response as an error. If the response is StopResponse,
// ErrStopResponse is returned, allowing for direct error comparison.
//
// Note that since errors returned by ReadNegotiationAcceptance are newly
// allocated, they cannot be compared to other errors in the traditional
// fashion.
func ReadNegotiationAcceptance(r io.Reader) error {
	var resp string
	err := encoding.ReadObject(r, &resp, NegotiateMaxErrorSize)
	if err != nil {
		return err
	}
	switch resp {
	case AcceptResponse:
		return nil
	case StopResponse:
		return ErrStopResponse
	default:
		return errors.New(resp)
	}
}

// WriteNegotiationAcceptance writes the 'accept' response to w (usually a
// net.Conn).
func WriteNegotiationAcceptance(w io.Writer) error {
	return encoding.WriteObject(w, AcceptResponse)
}

// WriteNegotiationRejection will write a rejection response to w (usually a
// net.Conn) and return the input error. If the write fails, the write error
// is joined with the input error.
func WriteNegotiationRejection(w io.Writer, err error) error {
	writeErr := encoding.WriteObject(w, err.Error())
	if writeErr != nil {
		return build.JoinErrors([]error{err, writeErr}, "; ")
	}
	return err
}

// WriteNegotiationStop writes the 'stop' response to w (usually a
// net.Conn).
func WriteNegotiationStop(w io.Writer) error {
	return encoding.WriteObject(w, StopResponse)
}

// CreateAnnouncement will take a host announcement and encode it, returning
// the exact []byte that should be added to the arbitrary data of a
// transaction.
func CreateAnnouncement(addr NetAddress, pk types.SiaPublicKey, sk crypto.SecretKey) (signedAnnouncement []byte, err error) {
	if err := addr.IsValid(); err != nil {
		return nil, err
	}

	// Create the HostAnnouncement and marshal it.
	annBytes := encoding.Marshal(HostAnnouncement{
		Specifier:  PrefixHostAnnouncement,
		NetAddress: addr,
		PublicKey:  pk,
	})

	// Create a signature for the announcement.
	annHash := crypto.HashBytes(annBytes)
	sig := crypto.SignHash(annHash, sk)
	// Return the signed announcement.
	return append(annBytes, sig[:]...), nil
}

// DecodeAnnouncement decodes announcement bytes into a host announcement,
// verifying the prefix and the signature.
func DecodeAnnouncement(fullAnnouncement []byte) (na NetAddress, spk types.SiaPublicKey, err error) {
	// Read the first part of the announcement to get the intended host
	// announcement.
	var ha HostAnnouncement
	dec := encoding.NewDecoder(bytes.NewReader(fullAnnouncement))
	err = dec.Decode(&ha)
	if err != nil {
		return "", types.SiaPublicKey{}, err
	}

	// Check that the announcement was registered as a host announcement.
	if ha.Specifier != PrefixHostAnnouncement {
		return "", types.SiaPublicKey{}, ErrAnnNotAnnouncement
	}
	// Check that the public key is a recognized type of public key.
	if ha.PublicKey.Algorithm != types.SignatureEd25519 {
		return "", types.SiaPublicKey{}, ErrAnnUnrecognizedSignature
	}

	// Read the signature out of the reader.
	var sig crypto.Signature
	err = dec.Decode(&sig)
	if err != nil {
		return "", types.SiaPublicKey{}, err
	}
	// Verify the signature.
	var pk crypto.PublicKey
	copy(pk[:], ha.PublicKey.Key)
	annHash := crypto.HashObject(ha)
	err = crypto.VerifyHash(annHash, pk, sig)
	if err != nil {
		return "", types.SiaPublicKey{}, err
	}
	return ha.NetAddress, ha.PublicKey, nil
}

// VerifyFileContractRevisionTransactionSignatures checks that the signatures
// on a file contract revision are valid and cover the right fields.
func VerifyFileContractRevisionTransactionSignatures(fcr types.FileContractRevision, tsigs []types.TransactionSignature, height types.BlockHeight) error {
	if len(tsigs) != 2 {
		return ErrRevisionSigCount
	}
	for _, tsig := range tsigs {
		// The transaction needs to be malleable so that miner fees can be
		// added. If the whole transaction is covered, it is doomed to have no
		// fees.
		if tsig.CoveredFields.WholeTransaction {
			return ErrRevisionCoveredFields
		}
	}
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{fcr},
		TransactionSignatures: tsigs,
	}
	// Check that the signatures verify. This will also check that the covered
	// fields object is not over-aggressive, because if the object is pointing
	// to elements that haven't been added to the transaction, verification
	// will fail.
	return txn.StandaloneValid(height)
}
