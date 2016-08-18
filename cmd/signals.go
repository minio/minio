/*
 * Minio Client, (C) 2015 Minio, Inc.
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
	"os/signal"
)

// signalTrap traps the registered signals and notifies the caller.
func signalTrap(sig ...os.Signal) <-chan bool {
	// channel to notify the caller.
	trapCh := make(chan bool, 1)

	go func(chan<- bool) {
		// channel to receive signals.
		sigCh := make(chan os.Signal, 1)
		defer close(sigCh)

		// `signal.Notify` registers the given channel to
		// receive notifications of the specified signals.
		signal.Notify(sigCh, sig...)

		// Wait for the signal.
		<-sigCh

		// Once signal has been received stop signal Notify handler.
		signal.Stop(sigCh)

		// Notify the caller.
		trapCh <- true
	}(trapCh)

	return trapCh
}
