// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/minio/minio/internal/logger"
)

func handleSignals() {
	// Custom exit function
	exit := func(success bool) {
		// If global profiler is set stop before we exit.
		globalProfilerMu.Lock()
		defer globalProfilerMu.Unlock()
		for _, p := range globalProfiler {
			p.Stop()
		}

		if success {
			os.Exit(0)
		}

		os.Exit(1)
	}

	stopProcess := func() bool {
		var err, oerr error

		// send signal to various go-routines that they need to quit.
		cancelGlobalContext()

		if globalNotificationSys != nil {
			globalNotificationSys.RemoveAllRemoteTargets()
		}

		if httpServer := newHTTPServerFn(); httpServer != nil {
			err = httpServer.Shutdown()
			if !errors.Is(err, http.ErrServerClosed) {
				logger.LogIf(context.Background(), err)
			}
		}

		if objAPI := newObjectLayerFn(); objAPI != nil {
			oerr = objAPI.Shutdown(context.Background())
			logger.LogIf(context.Background(), oerr)
		}

		return (err == nil && oerr == nil)
	}

	for {
		select {
		case <-globalHTTPServerErrorCh:
			exit(stopProcess())
		case osSignal := <-globalOSSignalCh:
			logger.Info("Exiting on signal: %s", strings.ToUpper(osSignal.String()))
			exit(stopProcess())
		case signal := <-globalServiceSignalCh:
			switch signal {
			case serviceRestart:
				logger.Info("Restarting on service signal")
				stop := stopProcess()
				rerr := restartProcess()
				logger.LogIf(context.Background(), rerr)
				exit(stop && rerr == nil)
			case serviceStop:
				logger.Info("Stopping on service signal")
				exit(stopProcess())
			}
		}
	}
}
