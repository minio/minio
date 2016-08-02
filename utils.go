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

package main

import (
	"encoding/base64"
	"encoding/xml"
	"io"
	"os"
	"strings"
	"syscall"
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

// shutdownSignal - is the channel that receives any boolean when
// we want broadcast the start of shutdown
var shutdownSignal chan bool

// shutdownCallbacks - is the list of function callbacks executed one by one
// when a shutdown starts. A callback returns 0 for success and 1 for failure.
// Failure is considered an emergency error that needs an immediate exit
var shutdownCallbacks []func() errCode

// shutdownObjectStorageCallbacks - contains the list of function callbacks that
// need to be invoked when a shutdown starts. These callbacks will be called before
// the general callback shutdowns
var shutdownObjectStorageCallbacks []func() errCode

// Register callback functions that need to be called when process terminates.
func registerShutdown(callback func() errCode) {
	shutdownCallbacks = append(shutdownCallbacks, callback)
}

// Register object storagecallback functions that need to be called when process terminates.
func registerObjectStorageShutdown(callback func() errCode) {
	shutdownObjectStorageCallbacks = append(shutdownObjectStorageCallbacks, callback)
}

// Start to monitor shutdownSignal to execute shutdown callbacks
func monitorShutdownSignal() {
	go func() {
		// Monitor processus signal
		trapCh := signalTrap(os.Interrupt, syscall.SIGTERM)
		for {
			select {
			case <-trapCh:
				// Start a graceful shutdown call
				shutdownSignal <- true
			case <-shutdownSignal:
				// Call all callbacks and exit for emergency
				for _, callback := range shutdownCallbacks {
					exitCode := callback()
					if exitCode != exitSuccess {
						os.Exit(int(exitCode))
					}

				}
				// Call all object storage shutdown callbacks and exit for emergency
				for _, callback := range shutdownObjectStorageCallbacks {
					exitCode := callback()
					if exitCode != exitSuccess {
						os.Exit(int(exitCode))
					}

				}
				os.Exit(int(exitSuccess))
			}
		}
	}()
}
