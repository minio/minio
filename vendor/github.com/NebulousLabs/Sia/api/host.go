package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/types"

	"github.com/julienschmidt/httprouter"
)

var (
	// errNoPath is returned when a call fails to provide a nonempty string
	// for the path parameter.
	errNoPath = Error{"path parameter is required"}

	// errStorageFolderNotFound is returned if a call is made looking for a
	// storage folder which does not appear to exist within the storage
	// manager.
	errStorageFolderNotFound = errors.New("storage folder with the provided path could not be found")
)

type (
	// HostGET contains the information that is returned after a GET request to
	// /host - a bunch of information about the status of the host.
	HostGET struct {
		ExternalSettings     modules.HostExternalSettings     `json:"externalsettings"`
		FinancialMetrics     modules.HostFinancialMetrics     `json:"financialmetrics"`
		InternalSettings     modules.HostInternalSettings     `json:"internalsettings"`
		NetworkMetrics       modules.HostNetworkMetrics       `json:"networkmetrics"`
		ConnectabilityStatus modules.HostConnectabilityStatus `json:"connectabilitystatus"`
		WorkingStatus        modules.HostWorkingStatus        `json:"workingstatus"`
	}

	// HostEstimateScoreGET contains the information that is returned from a
	// /host/estimatescore call.
	HostEstimateScoreGET struct {
		EstimatedScore types.Currency `json:"estimatedscore"`
		ConversionRate float64        `json:"conversionrate"`
	}

	// StorageGET contains the information that is returned after a GET request
	// to /host/storage - a bunch of information about the status of storage
	// management on the host.
	StorageGET struct {
		Folders []modules.StorageFolderMetadata `json:"folders"`
	}
)

// folderIndex determines the index of the storage folder with the provided
// path.
func folderIndex(folderPath string, storageFolders []modules.StorageFolderMetadata) (int, error) {
	for _, sf := range storageFolders {
		if sf.Path == folderPath {
			return int(sf.Index), nil
		}
	}
	return -1, errStorageFolderNotFound
}

// hostHandlerGET handles GET requests to the /host API endpoint, returning key
// information about the host.
func (api *API) hostHandlerGET(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	es := api.host.ExternalSettings()
	fm := api.host.FinancialMetrics()
	is := api.host.InternalSettings()
	nm := api.host.NetworkMetrics()
	cs := api.host.ConnectabilityStatus()
	ws := api.host.WorkingStatus()
	hg := HostGET{
		ExternalSettings:     es,
		FinancialMetrics:     fm,
		InternalSettings:     is,
		NetworkMetrics:       nm,
		ConnectabilityStatus: cs,
		WorkingStatus:        ws,
	}
	WriteJSON(w, hg)
}

// parseHostSettings a request's query strings and returns a
// modules.HostInternalSettings configured with the request's query string
// parameters.
func (api *API) parseHostSettings(req *http.Request) (modules.HostInternalSettings, error) {
	settings := api.host.InternalSettings()

	if req.FormValue("acceptingcontracts") != "" {
		var x bool
		_, err := fmt.Sscan(req.FormValue("acceptingcontracts"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.AcceptingContracts = x
	}
	if req.FormValue("maxdownloadbatchsize") != "" {
		var x uint64
		_, err := fmt.Sscan(req.FormValue("maxdownloadbatchsize"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.MaxDownloadBatchSize = x
	}
	if req.FormValue("maxduration") != "" {
		var x types.BlockHeight
		_, err := fmt.Sscan(req.FormValue("maxduration"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.MaxDuration = x
	}
	if req.FormValue("maxrevisebatchsize") != "" {
		var x uint64
		_, err := fmt.Sscan(req.FormValue("maxrevisebatchsize"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.MaxReviseBatchSize = x
	}
	if req.FormValue("netaddress") != "" {
		var x modules.NetAddress
		_, err := fmt.Sscan(req.FormValue("netaddress"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.NetAddress = x
	}
	if req.FormValue("windowsize") != "" {
		var x types.BlockHeight
		_, err := fmt.Sscan(req.FormValue("windowsize"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.WindowSize = x
	}

	if req.FormValue("collateral") != "" {
		var x types.Currency
		_, err := fmt.Sscan(req.FormValue("collateral"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.Collateral = x
	}
	if req.FormValue("collateralbudget") != "" {
		var x types.Currency
		_, err := fmt.Sscan(req.FormValue("collateralbudget"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.CollateralBudget = x
	}
	if req.FormValue("maxcollateral") != "" {
		var x types.Currency
		_, err := fmt.Sscan(req.FormValue("maxcollateral"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.MaxCollateral = x
	}

	if req.FormValue("mincontractprice") != "" {
		var x types.Currency
		_, err := fmt.Sscan(req.FormValue("mincontractprice"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.MinContractPrice = x
	}
	if req.FormValue("mindownloadbandwidthprice") != "" {
		var x types.Currency
		_, err := fmt.Sscan(req.FormValue("mindownloadbandwidthprice"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.MinDownloadBandwidthPrice = x
	}
	if req.FormValue("minstorageprice") != "" {
		var x types.Currency
		_, err := fmt.Sscan(req.FormValue("minstorageprice"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.MinStoragePrice = x
	}
	if req.FormValue("minuploadbandwidthprice") != "" {
		var x types.Currency
		_, err := fmt.Sscan(req.FormValue("minuploadbandwidthprice"), &x)
		if err != nil {
			return modules.HostInternalSettings{}, nil
		}
		settings.MinUploadBandwidthPrice = x
	}

	return settings, nil
}

// hostEstimateScoreGET handles the POST request to /host/estimatescore and
// computes an estimated HostDB score for the provided settings.
func (api *API) hostEstimateScoreGET(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// This call requires a renter, check that it is present.
	if api.renter == nil {
		WriteError(w, Error{"cannot call /host/estimatescore without the renter module"}, http.StatusBadRequest)
		return
	}

	settings, err := api.parseHostSettings(req)
	if err != nil {
		WriteError(w, Error{"error parsing host settings: " + err.Error()}, http.StatusBadRequest)
		return
	}
	var totalStorage, remainingStorage uint64
	for _, sf := range api.host.StorageFolders() {
		totalStorage += sf.Capacity
		remainingStorage += sf.CapacityRemaining
	}
	mergedSettings := modules.HostExternalSettings{
		AcceptingContracts:   settings.AcceptingContracts,
		MaxDownloadBatchSize: settings.MaxDownloadBatchSize,
		MaxDuration:          settings.MaxDuration,
		MaxReviseBatchSize:   settings.MaxReviseBatchSize,
		RemainingStorage:     remainingStorage,
		SectorSize:           modules.SectorSize,
		TotalStorage:         totalStorage,
		WindowSize:           settings.WindowSize,

		Collateral:    settings.Collateral,
		MaxCollateral: settings.MaxCollateral,

		ContractPrice:          settings.MinContractPrice,
		DownloadBandwidthPrice: settings.MinDownloadBandwidthPrice,
		StoragePrice:           settings.MinStoragePrice,
		UploadBandwidthPrice:   settings.MinUploadBandwidthPrice,

		Version: build.Version,
	}
	entry := modules.HostDBEntry{}
	entry.PublicKey = api.host.PublicKey()
	entry.HostExternalSettings = mergedSettings
	estimatedScoreBreakdown := api.renter.EstimateHostScore(entry)
	e := HostEstimateScoreGET{
		EstimatedScore: estimatedScoreBreakdown.Score,
		ConversionRate: estimatedScoreBreakdown.ConversionRate,
	}
	WriteJSON(w, e)
}

// hostHandlerPOST handles POST request to the /host API endpoint, which sets
// the internal settings of the host.
func (api *API) hostHandlerPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	settings, err := api.parseHostSettings(req)
	if err != nil {
		WriteError(w, Error{"error parsing host settings: " + err.Error()}, http.StatusBadRequest)
		return
	}

	err = api.host.SetInternalSettings(settings)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	WriteSuccess(w)
}

// hostAnnounceHandler handles the API call to get the host to announce itself
// to the network.
func (api *API) hostAnnounceHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var err error
	if addr := req.FormValue("netaddress"); addr != "" {
		err = api.host.AnnounceAddress(modules.NetAddress(addr))
	} else {
		err = api.host.Announce()
	}
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	WriteSuccess(w)
}

// storageHandler returns a bunch of information about storage management on
// the host.
func (api *API) storageHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	WriteJSON(w, StorageGET{
		Folders: api.host.StorageFolders(),
	})
}

// storageFoldersAddHandler adds a storage folder to the storage manager.
func (api *API) storageFoldersAddHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	folderPath := req.FormValue("path")
	var folderSize uint64
	_, err := fmt.Sscan(req.FormValue("size"), &folderSize)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	err = api.host.AddStorageFolder(folderPath, folderSize)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	WriteSuccess(w)
}

// storageFoldersResizeHandler resizes a storage folder in the storage manager.
func (api *API) storageFoldersResizeHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	folderPath := req.FormValue("path")
	if folderPath == "" {
		WriteError(w, Error{"path parameter is required"}, http.StatusBadRequest)
		return
	}

	storageFolders := api.host.StorageFolders()
	folderIndex, err := folderIndex(folderPath, storageFolders)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}

	var newSize uint64
	_, err = fmt.Sscan(req.FormValue("newsize"), &newSize)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	err = api.host.ResizeStorageFolder(uint16(folderIndex), newSize, false)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	WriteSuccess(w)
}

// storageFoldersRemoveHandler removes a storage folder from the storage
// manager.
func (api *API) storageFoldersRemoveHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	folderPath := req.FormValue("path")
	if folderPath == "" {
		WriteError(w, Error{"path parameter is required"}, http.StatusBadRequest)
		return
	}

	storageFolders := api.host.StorageFolders()
	folderIndex, err := folderIndex(folderPath, storageFolders)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}

	force := req.FormValue("force") == "true"
	err = api.host.RemoveStorageFolder(uint16(folderIndex), force)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	WriteSuccess(w)
}

// storageSectorsDeleteHandler handles the call to delete a sector from the
// storage manager.
func (api *API) storageSectorsDeleteHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	sectorRoot, err := scanHash(ps.ByName("merkleroot"))
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	err = api.host.DeleteSector(sectorRoot)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}
	WriteSuccess(w)
}
