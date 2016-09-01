/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/pkg/profile"
)

// xmlDecoder provide decoded value in xml.
func xmlDecoder(body io.Reader, v interface{}, size int64) error {
	var lbody io.Reader
	if size > 0 {
		lbody = io.LimitReader(body, size)
	} else {
		lbody = body
	}
	d := xml.NewDecoder(lbody)
	return d.Decode(v)
}

// checkValidMD5 - verify if valid md5, returns md5 in bytes.
func checkValidMD5(md5 string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimSpace(md5))
}

/// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
const (
	// maximum object size per PUT request is 5GiB
	maxObjectSize = 1024 * 1024 * 1024 * 5
	// minimum Part size for multipart upload is 5MB
	minPartSize = 1024 * 1024 * 5
	// maximum Part ID for multipart upload is 10000 (Acceptable values range from 1 to 10000 inclusive)
	maxPartID = 10000
)

// isMaxObjectSize - verify if max object size
func isMaxObjectSize(size int64) bool {
	return size > maxObjectSize
}

// Check if part size is more than or equal to minimum allowed size.
func isMinAllowedPartSize(size int64) bool {
	return size >= minPartSize
}

// isMaxPartNumber - Check if part ID is greater than the maximum allowed ID.
func isMaxPartID(partID int) bool {
	return partID > maxPartID
}

func contains(stringList []string, element string) bool {
	for _, e := range stringList {
		if e == element {
			return true
		}
	}
	return false
}

// Represents a type of an exit func which will be invoked upon shutdown signal.
type onExitFunc func(code int)

// Represents a type for all the the callback functions invoked upon shutdown signal.
type cleanupOnExitFunc func() errCode

// Represents a collection of various callbacks executed upon exit signals.
type shutdownCallbacks struct {
	// Protect callbacks list from a concurrent access
	*sync.RWMutex
	// genericCallbacks - is the list of function callbacks executed one by one
	// when a shutdown starts. A callback returns 0 for success and 1 for failure.
	// Failure is considered an emergency error that needs an immediate exit
	genericCallbacks []cleanupOnExitFunc
	// objectLayerCallbacks - contains the list of function callbacks that
	// need to be invoked when a shutdown starts. These callbacks will be called before
	// the general callback shutdowns
	objectLayerCallbacks []cleanupOnExitFunc
}

// globalShutdownCBs stores regular and object storages callbacks
var globalShutdownCBs *shutdownCallbacks

func (s shutdownCallbacks) GetObjectLayerCBs() []cleanupOnExitFunc {
	s.RLock()
	defer s.RUnlock()
	return s.objectLayerCallbacks
}

func (s shutdownCallbacks) GetGenericCBs() []cleanupOnExitFunc {
	s.RLock()
	defer s.RUnlock()
	return s.genericCallbacks
}

func (s *shutdownCallbacks) AddObjectLayerCB(callback cleanupOnExitFunc) error {
	s.Lock()
	defer s.Unlock()
	if callback == nil {
		return errInvalidArgument
	}
	s.objectLayerCallbacks = append(s.objectLayerCallbacks, callback)
	return nil
}

func (s *shutdownCallbacks) AddGenericCB(callback cleanupOnExitFunc) error {
	s.Lock()
	defer s.Unlock()
	if callback == nil {
		return errInvalidArgument
	}
	s.genericCallbacks = append(s.genericCallbacks, callback)
	return nil
}

// Initialize graceful shutdown mechanism.
func initGracefulShutdown(onExitFn onExitFunc) error {
	// Validate exit func.
	if onExitFn == nil {
		return errInvalidArgument
	}
	globalShutdownCBs = &shutdownCallbacks{
		RWMutex: &sync.RWMutex{},
	}
	// Return start monitor shutdown signal.
	return startMonitorShutdownSignal(onExitFn)
}

type shutdownSignal int

const (
	shutdownHalt = iota
	shutdownRestart
)

// Starts a profiler returns nil if profiler is not enabled, caller needs to handle this.
func startProfiler(profiler string) interface {
	Stop()
} {
	// Set ``MINIO_PROFILE_DIR`` to the directory where profiling information should be persisted
	profileDir := os.Getenv("MINIO_PROFILE_DIR")
	// Enable profiler if ``MINIO_PROFILER`` is set. Supported options are [cpu, mem, block].
	switch profiler {
	case "cpu":
		return profile.Start(profile.CPUProfile, profile.NoShutdownHook, profile.ProfilePath(profileDir))
	case "mem":
		return profile.Start(profile.MemProfile, profile.NoShutdownHook, profile.ProfilePath(profileDir))
	case "block":
		return profile.Start(profile.BlockProfile, profile.NoShutdownHook, profile.ProfilePath(profileDir))
	default:
		return nil
	}
}

// Global shutdown signal channel.
var globalShutdownSignalCh = make(chan shutdownSignal, 1)

// Global profiler to be used by shutdown go-routine.
var globalProfiler interface {
	Stop()
}

// Start to monitor shutdownSignal to execute shutdown callbacks
func startMonitorShutdownSignal(onExitFn onExitFunc) error {
	// Validate exit func.
	if onExitFn == nil {
		return errInvalidArgument
	}

	// Start listening on shutdown signal.
	go func() {
		defer close(globalShutdownSignalCh)

		// Monitor signals.
		trapCh := signalTrap(os.Interrupt, syscall.SIGTERM)
		for {
			select {
			case <-trapCh:
				// Initiate graceful shutdown.
				globalShutdownSignalCh <- shutdownHalt
			case signal := <-globalShutdownSignalCh:
				// Call all object storage shutdown callbacks and exit for emergency
				for _, callback := range globalShutdownCBs.GetObjectLayerCBs() {
					exitCode := callback()
					if exitCode != exitSuccess {
						// If global profiler is set stop before we exit.
						if globalProfiler != nil {
							globalProfiler.Stop()
						}
						onExitFn(int(exitCode))
					}

				}
				// Call all callbacks and exit for emergency
				for _, callback := range globalShutdownCBs.GetGenericCBs() {
					exitCode := callback()
					if exitCode != exitSuccess {
						// If global profiler is set stop before we exit.
						if globalProfiler != nil {
							globalProfiler.Stop()
						}
						onExitFn(int(exitCode))
					}
				}
				// All shutdown callbacks ensure that the server is safely terminated
				// and any concurrent process could be started again
				if signal == shutdownRestart {
					path := os.Args[0]
					cmdArgs := os.Args[1:]
					cmd := exec.Command(path, cmdArgs...)
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr

					err := cmd.Start()
					if err != nil {
						errorIf(errors.New("Unable to reboot."), err.Error())
					}

					// If global profiler is set stop before we exit.
					if globalProfiler != nil {
						globalProfiler.Stop()
					}

					// Successfully forked.
					onExitFn(int(exitSuccess))
				}

				// If global profiler is set stop before we exit.
				if globalProfiler != nil {
					globalProfiler.Stop()
				}

				// Exit as success if no errors.
				onExitFn(int(exitSuccess))
			}
		}
	}()
	// Successfully started routine.
	return nil
}
