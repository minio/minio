/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"os/exec"
)

// Type of service signals currently supported.
type serviceSignal int

const (
	serviceStatus  = iota // Gets status about the service.
	serviceRestart        // Restarts the service.
	serviceStop           // Stops the server.
	// Add new service requests here.
)

// Global service signal channel.
var globalServiceSignalCh chan serviceSignal

// Global service done channel.
var globalServiceDoneCh chan struct{}

// Initialize service mutex once.
func init() {
	globalServiceDoneCh = make(chan struct{}, 1)
	globalServiceSignalCh = make(chan serviceSignal)
}

// restartProcess starts a new process passing it the active fd's. It
// doesn't fork, but starts a new process using the same environment and
// arguments as when it was originally started. This allows for a newly
// deployed binary to be started. It returns the pid of the newly started
// process when successful.
func restartProcess() error {
	// Use the original binary location. This works with symlinks such that if
	// the file it points to has been changed we will use the updated symlink.
	argv0, err := exec.LookPath(os.Args[0])
	if err != nil {
		return err
	}

	// Pass on the environment and replace the old count key with the new one.
	cmd := exec.Command(argv0, os.Args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Start()
}
