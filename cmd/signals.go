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
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/minio/minio/internal/logger"
)

func shutdownHealMRFWithTimeout() {
	const shutdownTimeout = time.Minute

	finished := make(chan struct{})
	go func() {
		globalMRFState.shutdown()
		close(finished)
	}()
	select {
	case <-time.After(shutdownTimeout):
	case <-finished:
	}
}

func handleSignals() {
	// Custom exit function
	exit := func(success bool) {
		if globalLoggerOutput != nil {
			globalLoggerOutput.Close()
		}

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
		shutdownHealMRFWithTimeout() // this can take time sometimes, it needs to be executed
		// before stopping s3 operations

		// send signal to various go-routines that they need to quit.
		cancelGlobalContext()

		if httpServer := newHTTPServerFn(); httpServer != nil {
			if err := httpServer.Shutdown(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				shutdownLogIf(context.Background(), err)
			}
		}

		if objAPI := newObjectLayerFn(); objAPI != nil {
			shutdownLogIf(context.Background(), objAPI.Shutdown(context.Background()))
		}

		if globalBrowserEnabled {
			if srv := newConsoleServerFn(); srv != nil {
				shutdownLogIf(context.Background(), srv.Shutdown())
			}
		}

		if globalEventNotifier != nil {
			globalEventNotifier.RemoveAllBucketTargets()
		}

		return true
	}

	for {
		select {
		case err := <-globalHTTPServerErrorCh:
			shutdownLogIf(context.Background(), err)
			exit(stopProcess())
		case osSignal := <-globalOSSignalCh:
			logger.Info("Exiting on signal: %s", strings.ToUpper(osSignal.String()))
			daemon.SdNotify(false, daemon.SdNotifyStopping)
			exit(stopProcess())
		case signal := <-globalServiceSignalCh:
			switch signal {
			case serviceRestart:
				logger.Info("Restarting on service signal")
				daemon.SdNotify(false, daemon.SdNotifyReloading)
				stop := stopProcess()
				rerr := restartProcess()
				if rerr == nil {
					daemon.SdNotify(false, daemon.SdNotifyReady)
				}
				shutdownLogIf(context.Background(), rerr)
				exit(stop && rerr == nil)
			case serviceStop:
				logger.Info("Stopping on service signal")
				daemon.SdNotify(false, daemon.SdNotifyStopping)
				exit(stopProcess())
			}
		}
	}
}
