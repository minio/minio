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
	"os"
	"os/exec"
	"runtime"
	"syscall"

	xioutil "github.com/minio/minio/internal/ioutil"
)

// Type of service signals currently supported.
type serviceSignal int

const (
	serviceRestart       serviceSignal = iota // Restarts the server.
	serviceStop                               // Stops the server.
	serviceReloadDynamic                      // Reload dynamic config values.
	serviceFreeze                             // Freeze all S3 API calls.
	serviceUnFreeze                           // Un-Freeze previously frozen S3 API calls.
	// Add new service requests here.
)

// Global service signal channel.
var globalServiceSignalCh = make(chan serviceSignal)

// GlobalContext context that is canceled when server is requested to shut down.
// cancelGlobalContext can be used to indicate server shutdown.
var GlobalContext, cancelGlobalContext = context.WithCancel(context.Background())

// restartProcess starts a new process passing it the active fd's. It
// doesn't fork, but starts a new process using the same environment and
// arguments as when it was originally started. This allows for a newly
// deployed binary to be started. It returns the pid of the newly started
// process when successful.
func restartProcess() error {
	if runtime.GOOS == globalWindowsOSName {
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin
		cmd.Env = os.Environ()
		err := cmd.Run()
		if err == nil {
			os.Exit(0)
		}
		return err
	}

	// Use the original binary location. This works with symlinks such that if
	// the file it points to has been changed we will use the updated symlink.
	argv0, err := exec.LookPath(os.Args[0])
	if err != nil {
		return err
	}

	// Invokes the execve system call.
	// Re-uses the same pid. This preserves the pid over multiple server-respawns.
	return syscall.Exec(argv0, os.Args, os.Environ())
}

// freezeServices will freeze all incoming S3 API calls.
// For each call, unfreezeServices must be called once.
func freezeServices() {
	// Use atomics for globalServiceFreeze, so we can read without locking.
	// We need a lock since we are need the 2 atomic values to remain in sync.
	globalServiceFreezeMu.Lock()
	// If multiple calls, first one creates channel.
	globalServiceFreezeCnt++
	if globalServiceFreezeCnt == 1 {
		globalServiceFreeze.Store(make(chan struct{}))
	}
	globalServiceFreezeMu.Unlock()
}

// unfreezeServices will unfreeze all incoming S3 API calls.
// For each call, unfreezeServices must be called once.
func unfreezeServices() {
	// We need a lock since we need the 2 atomic values to remain in sync.
	globalServiceFreezeMu.Lock()
	// Close when we reach 0
	globalServiceFreezeCnt--
	if globalServiceFreezeCnt <= 0 {
		// Set to a nil channel.
		var _ch chan struct{}
		if val := globalServiceFreeze.Swap(_ch); val != nil {
			if ch, ok := val.(chan struct{}); ok && ch != nil {
				// Close previous non-nil channel.
				xioutil.SafeClose(ch)
			}
		}
		globalServiceFreezeCnt = 0 // Don't risk going negative.
	}
	globalServiceFreezeMu.Unlock()
}
