/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
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

	"github.com/minio/minio/cmd/logger"
)

func handleSignals() {
	// Custom exit function
	exit := func(state bool) {
		// If global profiler is set stop before we exit.
		if globalProfiler != nil {
			globalProfiler.Stop()
		}

		if state {
			os.Exit(0)
		}

		os.Exit(1)
	}

	stopProcess := func() bool {
		var err, oerr error

		if globalNotificationSys != nil {
			globalNotificationSys.RemoveAllRemoteTargets()
		}

		err = globalHTTPServer.Shutdown()
		logger.LogIf(context.Background(), err)

		if objAPI := newObjectLayerFn(); objAPI != nil {
			oerr = objAPI.Shutdown(context.Background())
			logger.LogIf(context.Background(), oerr)
		}

		return (err == nil && oerr == nil)
	}

	for {
		select {
		case err := <-globalHTTPServerErrorCh:
			logger.LogIf(context.Background(), err)
			var oerr error
			if objAPI := newObjectLayerFn(); objAPI != nil {
				oerr = objAPI.Shutdown(context.Background())
			}

			exit(err == nil && oerr == nil)
		case osSignal := <-globalOSSignalCh:
			stopHTTPTrace()
			logger.Info("Exiting on signal %v", osSignal)
			exit(stopProcess())
		case signal := <-globalServiceSignalCh:
			switch signal {
			case serviceStatus:
				// Ignore this at the moment.
			case serviceRestart:
				logger.Info("Restarting on service signal")
				err := globalHTTPServer.Shutdown()
				logger.LogIf(context.Background(), err)
				stopHTTPTrace()
				rerr := restartProcess()
				logger.LogIf(context.Background(), rerr)

				exit(err == nil && rerr == nil)
			case serviceStop:
				logger.Info("Stopping on service signal")
				stopHTTPTrace()
				exit(stopProcess())
			}
		}
	}
}
