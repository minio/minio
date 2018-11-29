package cmd

import (
	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/certs"
)

func initLogger() {
	logger.Init(GOPATH, GOROOT)
	logger.RegisterUIError(fmtError)
}

var extPortHandlers = []HandlerFunc{
	setExtPortReservedPathHandler,
	setExtPortBrowserCacheControlHandler,
	// Add new extension port specific handlers here.
}

func startExtensionsPortServer(getCert certs.GetCertificateFunc) {
	initLogger()
	err := checkPortAvailability(globalExtensionsPort)
	if err != nil {
		logger.Fatal(err, "Unable to start the extensions port server")
	}

	router := mux.NewRouter().SkipClean(true)

	// register all extensions port applicable routers
	registerMetricsRouter(router, extPortReservedMetricsPath)
	handler := registerHandlers(registerHandlers(router, globalHandlers...), extPortHandlers...)

	extensionsPortServer := xhttp.NewServer([]string{globalMinioHost + globalExtensionsPort}, criticalErrorHandler{handler}, getCert)
	extensionsPortServer.UpdateBytesReadFunc = globalConnStats.incInputBytes
	extensionsPortServer.UpdateBytesWrittenFunc = globalConnStats.incOutputBytes
	go func() {
		globalHTTPServerErrorCh <- extensionsPortServer.Start()
	}()
}
