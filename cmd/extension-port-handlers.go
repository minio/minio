package cmd

import (
	"net/http"
)

const (
	extPortReservedMetrics     = "metrics"
	extPortReservedMetricsPath = "/" + extPortReservedMetrics
)

//
// Metrics Routing
//
func isMetricsOnExtensionPort(path string, host string) bool {
	_, requestPort := mustSplitHostPort(host)
	requestPort = ":" + requestPort
	return path == extPortReservedMetricsPath && globalExtensionsPort != "" && requestPort == globalExtensionsPort
}

func isSupportedPath(req *http.Request) bool {
	if req == nil {
		return false
	}
	aType := getRequestAuthType(req)
	return aType == authTypeAnonymous &&
		isMetricsOnExtensionPort(req.URL.Path, req.Host)
}

type extPortReservedPathHandler struct {
	handler http.Handler
}

func setExtPortReservedPathHandler(h http.Handler) http.Handler {
	return extPortReservedPathHandler{h}
}

func (h extPortReservedPathHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isSupportedPath(r) {
		h.handler.ServeHTTP(w, r)
	} else {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
}

//
// Cache Control
//
type extPortCacheControlHandler struct {
	handler http.Handler
}

func setExtPortBrowserCacheControlHandler(h http.Handler) http.Handler {
	return extPortCacheControlHandler{h}
}

func (h extPortCacheControlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && guessIsBrowserReq(r) && globalIsBrowserEnabled {
		if hasPrefix(r.URL.Path, extPortReservedMetricsPath) {
			w.Header().Set("Cache-Control", "no-store")
		}
	}
	h.handler.ServeHTTP(w, r)
}
