/*
 * MinIO Cloud Storage, (C) 2015-2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"os"
	"strings"

	"github.com/minio/minio/cmd/logger"
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

		if globalNotificationSys != nil {
			globalNotificationSys.RemoveAllRemoteTargets()
		}

		// Stop watching for any certificate changes.
		globalTLSCerts.Stop()

		if httpServer := newHTTPServerFn(); httpServer != nil {
			err = httpServer.Shutdown()
			logger.LogIf(context.Background(), err)
		}

		// send signal to various go-routines that they need to quit.
		cancelGlobalContext()

		if objAPI := newObjectLayerWithoutSafeModeFn(); objAPI != nil {
			oerr = objAPI.Shutdown(context.Background())
			logger.LogIf(context.Background(), oerr)
		}

		return (err == nil && oerr == nil)
	}

	for {
		select {
		case err := <-globalHTTPServerErrorCh:
			if objAPI := newObjectLayerWithoutSafeModeFn(); objAPI != nil {
				objAPI.Shutdown(context.Background())
			}
			if err != nil {
				logger.Fatal(err, "Unable to start MinIO server")
			}
			exit(true)
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
