/*
 * Minio Client, (C) 2015, 2016, 2017 Minio, Inc.
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
	"os"
)

func handleSignals() {
	// Custom exit function
	exit := func(err error) {
		// If global profiler is set stop before we exit.
		if globalProfiler != nil {
			globalProfiler.Stop()
		}

		rv := 0
		if err != nil {
			rv = 1
		}

		os.Exit(rv)
	}

	stopProcess := func() error {
		err := globalHTTPServer.Shutdown()
		errorIf(err, "Unable to shutdown http server")
		if objAPI := newObjectLayerFn(); objAPI != nil {
			oerr := objAPI.Shutdown()
			errorIf(oerr, "Unable to shutdown object layer")
			if err == nil {
				err = oerr
			}
		}

		return err
	}

	for {
		select {
		case err := <-globalHTTPServerErrorCh:
			errorIf(err, "http server exited abnormally")
			if objAPI := newObjectLayerFn(); objAPI != nil {
				oerr := objAPI.Shutdown()
				errorIf(oerr, "Unable to shutdown object layer")
				if err == nil {
					err = oerr
				}
			}

			exit(err)
		case osSignal := <-globalOSSignalCh:
			log.Printf("Exiting on signal %v\n", osSignal)
			exit(stopProcess())
		case signal := <-globalServiceSignalCh:
			switch signal {
			case serviceStatus:
				// Ignore this at the moment.
			case serviceRestart:
				log.Println("Restarting on service signal")
				err := globalHTTPServer.Shutdown()
				errorIf(err, "Unable to shutdown http server")

				rerr := restartProcess()
				errorIf(rerr, "Unable to restart the server.")

				if err == nil {
					err = rerr
				}

				exit(err)
			case serviceStop:
				log.Println("Stopping on service signal")
				exit(stopProcess())
			}
		}
	}
}
