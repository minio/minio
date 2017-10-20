package modules

import (
	"github.com/NebulousLabs/Sia/types"
)

const (
	// HostDir names the directory that contains the host persistence.
	HostDir = "host"
)

var (
	// BytesPerTerabyte is the conversion rate between bytes and terabytes.
	BytesPerTerabyte = types.NewCurrency64(1e12)

	// BlockBytesPerMonthTerabyte is the conversion rate between block-bytes and month-TB.
	BlockBytesPerMonthTerabyte = BytesPerTerabyte.Mul64(4320)

	// HostWorkingStatusChecking is returned from WorkingStatus() if the host is
	// still determining if it is working, that is, if settings calls are
	// incrementing.
	HostWorkingStatusChecking = HostWorkingStatus("checking")

	// HostWorkingStatusNotWorking is returned from WorkingStatus() if the host
	// has not received any settings calls over the duration of
	// workingStatusFrequency.
	HostWorkingStatusNotWorking = HostWorkingStatus("not working")

	// HostWorkingStatusWorking is returned from WorkingStatus() if the host has
	// received more than workingThreshold settings calls over the duration of
	// workingStatusFrequency.
	HostWorkingStatusWorking = HostWorkingStatus("working")

	// HostConnectabilityStatusChecking is returned from ConnectabilityStatus()
	// if the host is still determining if it is connectable.
	HostConnectabilityStatusChecking = HostConnectabilityStatus("checking")

	// HostConnectabilityStatusConnectable is returned from
	// ConnectabilityStatus() if the host is connectable at its configured
	// netaddress.
	HostConnectabilityStatusConnectable = HostConnectabilityStatus("connectable")

	// HostConnectabilityStatusNotConnectable is returned from
	// ConnectabilityStatus() if the host is not connectable at its configured
	// netaddress.
	HostConnectabilityStatusNotConnectable = HostConnectabilityStatus("not connectable")
)

type (
	// HostFinancialMetrics provides financial statistics for the host,
	// including money that is locked in contracts. Though verbose, these
	// statistics should provide a clear picture of where the host's money is
	// currently being used. The front end can consolidate stats where desired.
	// Potential revenue refers to revenue that is available in a file
	// contract for which the file contract window has not yet closed.
	HostFinancialMetrics struct {
		// Every time a renter forms a contract with a host, a contract fee is
		// paid by the renter. These stats track the total contract fees.
		ContractCount                 uint64         `json:"contractcount"`
		ContractCompensation          types.Currency `json:"contractcompensation"`
		PotentialContractCompensation types.Currency `json:"potentialcontractcompensation"`

		// Metrics related to storage proofs, collateral, and submitting
		// transactions to the blockchain.
		LockedStorageCollateral types.Currency `json:"lockedstoragecollateral"`
		LostRevenue             types.Currency `json:"lostrevenue"`
		LostStorageCollateral   types.Currency `json:"loststoragecollateral"`
		PotentialStorageRevenue types.Currency `json:"potentialstoragerevenue"`
		RiskedStorageCollateral types.Currency `json:"riskedstoragecollateral"`
		StorageRevenue          types.Currency `json:"storagerevenue"`
		TransactionFeeExpenses  types.Currency `json:"transactionfeeexpenses"`

		// Bandwidth financial metrics.
		DownloadBandwidthRevenue          types.Currency `json:"downloadbandwidthrevenue"`
		PotentialDownloadBandwidthRevenue types.Currency `json:"potentialdownloadbandwidthrevenue"`
		PotentialUploadBandwidthRevenue   types.Currency `json:"potentialuploadbandwidthrevenue"`
		UploadBandwidthRevenue            types.Currency `json:"uploadbandwidthrevenue"`
	}

	// HostInternalSettings contains a list of settings that can be changed.
	HostInternalSettings struct {
		AcceptingContracts   bool              `json:"acceptingcontracts"`
		MaxDownloadBatchSize uint64            `json:"maxdownloadbatchsize"`
		MaxDuration          types.BlockHeight `json:"maxduration"`
		MaxReviseBatchSize   uint64            `json:"maxrevisebatchsize"`
		NetAddress           NetAddress        `json:"netaddress"`
		WindowSize           types.BlockHeight `json:"windowsize"`

		Collateral       types.Currency `json:"collateral"`
		CollateralBudget types.Currency `json:"collateralbudget"`
		MaxCollateral    types.Currency `json:"maxcollateral"`

		MinContractPrice          types.Currency `json:"mincontractprice"`
		MinDownloadBandwidthPrice types.Currency `json:"mindownloadbandwidthprice"`
		MinStoragePrice           types.Currency `json:"minstorageprice"`
		MinUploadBandwidthPrice   types.Currency `json:"minuploadbandwidthprice"`
	}

	// HostNetworkMetrics reports the quantity of each type of RPC call that
	// has been made to the host.
	HostNetworkMetrics struct {
		DownloadCalls     uint64 `json:"downloadcalls"`
		ErrorCalls        uint64 `json:"errorcalls"`
		FormContractCalls uint64 `json:"formcontractcalls"`
		RenewCalls        uint64 `json:"renewcalls"`
		ReviseCalls       uint64 `json:"revisecalls"`
		SettingsCalls     uint64 `json:"settingscalls"`
		UnrecognizedCalls uint64 `json:"unrecognizedcalls"`
	}

	// StorageObligation contains information about a storage obligation that
	// the host has accepted.
	StorageObligation struct {
		NegotiationHeight types.BlockHeight `json:"negotiationheight"`

		OriginConfirmed     bool   `json:"originconfirmed"`
		RevisionConstructed bool   `json:"revisionconstructed"`
		RevisionConfirmed   bool   `json:"revisionconfirmed"`
		ProofConstructed    bool   `json:"proofconstructed"`
		ProofConfirmed      bool   `json:"proofconfirmed"`
		ObligationStatus    uint64 `json:"obligationstatus"`
	}

	// HostWorkingStatus reports the working state of a host. Can be one of
	// "checking", "working", or "not working.
	HostWorkingStatus string

	// HostConnectabilityStatus reports the connectability state of a host. Can be
	// one of "checking", "connectable", or "not connectable"
	HostConnectabilityStatus string

	// A Host can take storage from disk and offer it to the network, managing
	// things such as announcements, settings, and implementing all of the RPCs
	// of the host protocol.
	Host interface {
		// Announce submits a host announcement to the blockchain.
		Announce() error

		// AnnounceAddress submits an announcement using the given address.
		AnnounceAddress(NetAddress) error

		// ExternalSettings returns the settings of the host as seen by an
		// untrusted node querying the host for settings.
		ExternalSettings() HostExternalSettings

		// FinancialMetrics returns the financial statistics of the host.
		FinancialMetrics() HostFinancialMetrics

		// InternalSettings returns the host's internal settings, including
		// potentially private or sensitive information.
		InternalSettings() HostInternalSettings

		// NetworkMetrics returns information on the types of RPC calls that
		// have been made to the host.
		NetworkMetrics() HostNetworkMetrics

		// PublicKey returns the public key of the host.
		PublicKey() types.SiaPublicKey

		// SetInternalSettings sets the hosting parameters of the host.
		SetInternalSettings(HostInternalSettings) error

		// StorageObligations returns the set of storage obligations held by
		// the host.
		StorageObligations() []StorageObligation

		// ConnectabilityStatus returns the connectability status of the host, that
		// is, if it can connect to itself on the configured NetAddress.
		ConnectabilityStatus() HostConnectabilityStatus

		// WorkingStatus returns the working state of the host, determined by if
		// settings calls are increasing.
		WorkingStatus() HostWorkingStatus

		// The storage manager provides an interface for adding and removing
		// storage folders and data sectors to the host.
		StorageManager
	}
)
