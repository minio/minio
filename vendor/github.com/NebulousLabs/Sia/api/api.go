package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/Sia/modules"

	"github.com/julienschmidt/httprouter"
)

// Error is a type that is encoded as JSON and returned in an API response in
// the event of an error. Only the Message field is required. More fields may
// be added to this struct in the future for better error reporting.
type Error struct {
	// Message describes the error in English. Typically it is set to
	// `err.Error()`. This field is required.
	Message string `json:"message"`

	// TODO: add a Param field with the (omitempty option in the json tag)
	// to indicate that the error was caused by an invalid, missing, or
	// incorrect parameter. This is not trivial as the API does not
	// currently do parameter validation itself. For example, the
	// /gateway/connect endpoint relies on the gateway.Connect method to
	// validate the netaddress. However, this prevents the API from knowing
	// whether an error returned by gateway.Connect is because of a
	// connection error or an invalid netaddress parameter. Validating
	// parameters in the API is not sufficient, as a parameter's value may
	// be valid or invalid depending on the current state of a module.
}

// Error implements the error interface for the Error type. It returns only the
// Message field.
func (err Error) Error() string {
	return err.Message
}

// HttpGET is a utility function for making http get requests to sia with a
// whitelisted user-agent. A non-2xx response does not return an error.
func HttpGET(url string) (resp *http.Response, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	return http.DefaultClient.Do(req)
}

// HttpGETAuthenticated is a utility function for making authenticated http get
// requests to sia with a whitelisted user-agent and the supplied password. A
// non-2xx response does not return an error.
func HttpGETAuthenticated(url string, password string) (resp *http.Response, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	req.SetBasicAuth("", password)
	return http.DefaultClient.Do(req)
}

// HttpPOST is a utility function for making post requests to sia with a
// whitelisted user-agent. A non-2xx response does not return an error.
func HttpPOST(url string, data string) (resp *http.Response, err error) {
	req, err := http.NewRequest("POST", url, strings.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return http.DefaultClient.Do(req)
}

// HttpPOSTAuthenticated is a utility function for making authenticated http
// post requests to sia with a whitelisted user-agent and the supplied
// password. A non-2xx response does not return an error.
func HttpPOSTAuthenticated(url string, data string, password string) (resp *http.Response, err error) {
	req, err := http.NewRequest("POST", url, strings.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth("", password)
	return http.DefaultClient.Do(req)
}

// RequireUserAgent is middleware that requires all requests to set a
// UserAgent that contains the specified string.
func RequireUserAgent(h http.Handler, ua string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !strings.Contains(req.UserAgent(), ua) {
			WriteError(w, Error{"Browser access disabled due to security vulnerability. Use Sia-UI or siac."}, http.StatusBadRequest)
			return
		}
		h.ServeHTTP(w, req)
	})
}

// RequirePassword is middleware that requires a request to authenticate with a
// password using HTTP basic auth. Usernames are ignored. Empty passwords
// indicate no authentication is required.
func RequirePassword(h httprouter.Handle, password string) httprouter.Handle {
	// An empty password is equivalent to no password.
	if password == "" {
		return h
	}
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		_, pass, ok := req.BasicAuth()
		if !ok || pass != password {
			w.Header().Set("WWW-Authenticate", "Basic realm=\"SiaAPI\"")
			WriteError(w, Error{"API authentication failed."}, http.StatusUnauthorized)
			return
		}
		h(w, req, ps)
	}
}

// cleanCloseHandler wraps the entire API, ensuring that underlying conns are
// not leaked if the rmeote end closes the connection before the underlying
// handler finishes.
func cleanCloseHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Close this file handle either when the function completes or when the
		// connection is done.
		done := make(chan struct{})
		go func(w http.ResponseWriter, r *http.Request) {
			defer close(done)
			next.ServeHTTP(w, r)
		}(w, r)
		select {
		case <-done:
		case <-r.Context().Done():
		}

		// Sanity check - thread should not take more than an hour to return. This
		// must be done in a goroutine, otherwise the server will not close the
		// underlying socket for this API call.
		go func() {
			select {
			case <-done:
			case <-time.After(time.Minute * 60):
				build.Severe("api call is taking more than 60 minutes to return:", r.URL.Path)
			}
		}()
	})
}

// API encapsulates a collection of modules and implements a http.Handler
// to access their methods.
type API struct {
	cs       modules.ConsensusSet
	explorer modules.Explorer
	gateway  modules.Gateway
	host     modules.Host
	miner    modules.Miner
	renter   modules.Renter
	tpool    modules.TransactionPool
	wallet   modules.Wallet

	router http.Handler
}

// api.ServeHTTP implements the http.Handler interface.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.router.ServeHTTP(w, r)
}

// New creates a new Sia API from the provided modules.  The API will require
// authentication using HTTP basic auth for certain endpoints of the supplied
// password is not the empty string.  Usernames are ignored for authentication.
func New(requiredUserAgent string, requiredPassword string, cs modules.ConsensusSet, e modules.Explorer, g modules.Gateway, h modules.Host, m modules.Miner, r modules.Renter, tp modules.TransactionPool, w modules.Wallet) *API {
	api := &API{
		cs:       cs,
		explorer: e,
		gateway:  g,
		host:     h,
		miner:    m,
		renter:   r,
		tpool:    tp,
		wallet:   w,
	}

	// Register API handlers
	router := httprouter.New()
	router.NotFound = http.HandlerFunc(UnrecognizedCallHandler)
	router.RedirectTrailingSlash = false

	// Consensus API Calls
	if api.cs != nil {
		router.GET("/consensus", api.consensusHandler)
		router.POST("/consensus/validate/transactionset", api.consensusValidateTransactionsetHandler)
	}

	// Explorer API Calls
	if api.explorer != nil {
		router.GET("/explorer", api.explorerHandler)
		router.GET("/explorer/blocks/:height", api.explorerBlocksHandler)
		router.GET("/explorer/hashes/:hash", api.explorerHashHandler)
	}

	// Gateway API Calls
	if api.gateway != nil {
		router.GET("/gateway", api.gatewayHandler)
		router.POST("/gateway/connect/:netaddress", RequirePassword(api.gatewayConnectHandler, requiredPassword))
		router.POST("/gateway/disconnect/:netaddress", RequirePassword(api.gatewayDisconnectHandler, requiredPassword))
	}

	// Host API Calls
	if api.host != nil {
		// Calls directly pertaining to the host.
		router.GET("/host", api.hostHandlerGET)                                                   // Get the host status.
		router.POST("/host", RequirePassword(api.hostHandlerPOST, requiredPassword))              // Change the settings of the host.
		router.POST("/host/announce", RequirePassword(api.hostAnnounceHandler, requiredPassword)) // Announce the host to the network.
		router.GET("/host/estimatescore", api.hostEstimateScoreGET)

		// Calls pertaining to the storage manager that the host uses.
		router.GET("/host/storage", api.storageHandler)
		router.POST("/host/storage/folders/add", RequirePassword(api.storageFoldersAddHandler, requiredPassword))
		router.POST("/host/storage/folders/remove", RequirePassword(api.storageFoldersRemoveHandler, requiredPassword))
		router.POST("/host/storage/folders/resize", RequirePassword(api.storageFoldersResizeHandler, requiredPassword))
		router.POST("/host/storage/sectors/delete/:merkleroot", RequirePassword(api.storageSectorsDeleteHandler, requiredPassword))
	}

	// Miner API Calls
	if api.miner != nil {
		router.GET("/miner", api.minerHandler)
		router.GET("/miner/header", RequirePassword(api.minerHeaderHandlerGET, requiredPassword))
		router.POST("/miner/header", RequirePassword(api.minerHeaderHandlerPOST, requiredPassword))
		router.GET("/miner/start", RequirePassword(api.minerStartHandler, requiredPassword))
		router.GET("/miner/stop", RequirePassword(api.minerStopHandler, requiredPassword))
	}

	// Renter API Calls
	if api.renter != nil {
		router.GET("/renter", api.renterHandlerGET)
		router.POST("/renter", RequirePassword(api.renterHandlerPOST, requiredPassword))
		router.GET("/renter/contracts", api.renterContractsHandler)
		router.GET("/renter/downloads", api.renterDownloadsHandler)
		router.GET("/renter/files", api.renterFilesHandler)
		router.GET("/renter/prices", api.renterPricesHandler)

		// TODO: re-enable these routes once the new .sia format has been
		// standardized and implemented.
		// router.POST("/renter/load", RequirePassword(api.renterLoadHandler, requiredPassword))
		// router.POST("/renter/loadascii", RequirePassword(api.renterLoadAsciiHandler, requiredPassword))
		// router.GET("/renter/share", RequirePassword(api.renterShareHandler, requiredPassword))
		// router.GET("/renter/shareascii", RequirePassword(api.renterShareAsciiHandler, requiredPassword))

		router.POST("/renter/delete/*siapath", RequirePassword(api.renterDeleteHandler, requiredPassword))
		router.GET("/renter/download/*siapath", RequirePassword(api.renterDownloadHandler, requiredPassword))
		router.GET("/renter/downloadasync/*siapath", RequirePassword(api.renterDownloadAsyncHandler, requiredPassword))
		router.POST("/renter/rename/*siapath", RequirePassword(api.renterRenameHandler, requiredPassword))
		router.POST("/renter/upload/*siapath", RequirePassword(api.renterUploadHandler, requiredPassword))

		// HostDB endpoints.
		router.GET("/hostdb/active", api.hostdbActiveHandler)
		router.GET("/hostdb/all", api.hostdbAllHandler)
		router.GET("/hostdb/hosts/:pubkey", api.hostdbHostsHandler)
	}

	// Transaction pool API Calls
	if api.tpool != nil {
		router.GET("/tpool/fee", api.tpoolFeeHandlerGET)
		router.GET("/tpool/raw/:id", api.tpoolRawHandlerGET)
		router.POST("/tpool/raw", api.tpoolRawHandlerPOST)

		// TODO: re-enable this route once the transaction pool API has been finalized
		//router.GET("/transactionpool/transactions", api.transactionpoolTransactionsHandler)
	}

	// Wallet API Calls
	if api.wallet != nil {
		router.GET("/wallet", api.walletHandler)
		router.POST("/wallet/033x", RequirePassword(api.wallet033xHandler, requiredPassword))
		router.GET("/wallet/address", RequirePassword(api.walletAddressHandler, requiredPassword))
		router.GET("/wallet/addresses", api.walletAddressesHandler)
		router.GET("/wallet/backup", RequirePassword(api.walletBackupHandler, requiredPassword))
		router.POST("/wallet/init", RequirePassword(api.walletInitHandler, requiredPassword))
		router.POST("/wallet/init/seed", RequirePassword(api.walletInitSeedHandler, requiredPassword))
		router.POST("/wallet/lock", RequirePassword(api.walletLockHandler, requiredPassword))
		router.POST("/wallet/seed", RequirePassword(api.walletSeedHandler, requiredPassword))
		router.GET("/wallet/seeds", RequirePassword(api.walletSeedsHandler, requiredPassword))
		router.POST("/wallet/siacoins", RequirePassword(api.walletSiacoinsHandler, requiredPassword))
		router.POST("/wallet/siafunds", RequirePassword(api.walletSiafundsHandler, requiredPassword))
		router.POST("/wallet/siagkey", RequirePassword(api.walletSiagkeyHandler, requiredPassword))
		router.POST("/wallet/sweep/seed", RequirePassword(api.walletSweepSeedHandler, requiredPassword))
		router.GET("/wallet/transaction/:id", api.walletTransactionHandler)
		router.GET("/wallet/transactions", api.walletTransactionsHandler)
		router.GET("/wallet/transactions/:addr", api.walletTransactionsAddrHandler)
		router.GET("/wallet/verify/address/:addr", api.walletVerifyAddressHandler)
		router.POST("/wallet/unlock", RequirePassword(api.walletUnlockHandler, requiredPassword))
		router.POST("/wallet/changepassword", RequirePassword(api.walletChangePasswordHandler, requiredPassword))
	}

	// Apply UserAgent middleware and return the API
	api.router = cleanCloseHandler(RequireUserAgent(router, requiredUserAgent))
	return api
}

// UnrecognizedCallHandler handles calls to unknown pages (404).
func UnrecognizedCallHandler(w http.ResponseWriter, req *http.Request) {
	WriteError(w, Error{"404 - Refer to API.md"}, http.StatusNotFound)
}

// WriteError an error to the API caller.
func WriteError(w http.ResponseWriter, err Error, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	encodingErr := json.NewEncoder(w).Encode(err)
	if _, isJsonErr := encodingErr.(*json.SyntaxError); isJsonErr {
		// Marshalling should only fail in the event of a developer error.
		// Specifically, only non-marshallable types should cause an error here.
		build.Critical("failed to encode API error response:", encodingErr)
	}
}

// WriteJSON writes the object to the ResponseWriter. If the encoding fails, an
// error is written instead. The Content-Type of the response header is set
// accordingly.
func WriteJSON(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err := json.NewEncoder(w).Encode(obj)
	if _, isJsonErr := err.(*json.SyntaxError); isJsonErr {
		// Marshalling should only fail in the event of a developer error.
		// Specifically, only non-marshallable types should cause an error here.
		build.Critical("failed to encode API response:", err)
	}
}

// WriteSuccess writes the HTTP header with status 204 No Content to the
// ResponseWriter. WriteSuccess should only be used to indicate that the
// requested action succeeded AND there is no data to return.
func WriteSuccess(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}
