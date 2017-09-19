/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"time"

	"github.com/minio/dsync"
)

// Allow any RPC call request time should be no more/less than 3 seconds.
// 3 seconds is chosen arbitrarily.
const rpcSkewTimeAllowed = 3 * time.Second

func isRequestTimeAllowed(requestTime time.Time) bool {
	// Check whether request time is within acceptable skew time.
	utcNow := UTCNow()
	return !(requestTime.Sub(utcNow) > rpcSkewTimeAllowed ||
		utcNow.Sub(requestTime) > rpcSkewTimeAllowed)
}

// AuthRPCArgs represents minimum required arguments to make any authenticated RPC call.
type AuthRPCArgs struct {
	// Authentication token to be verified by the server for every RPC call.
	AuthToken string
}

// SetAuthToken - sets the token to the supplied value.
func (args *AuthRPCArgs) SetAuthToken(authToken string) {
	args.AuthToken = authToken
}

// IsAuthenticated - validated whether this auth RPC args are already authenticated or not.
func (args AuthRPCArgs) IsAuthenticated() error {
	// Check whether the token is valid
	if !isAuthTokenValid(args.AuthToken) {
		return errInvalidToken
	}

	// Good to go.
	return nil
}

// AuthRPCReply represents minimum required reply for any authenticated RPC call.
type AuthRPCReply struct{}

// LoginRPCArgs - login username and password for RPC.
type LoginRPCArgs struct {
	AuthToken   string
	Version     string
	RequestTime time.Time
}

// IsValid - validates whether this LoginRPCArgs are valid for authentication.
func (args LoginRPCArgs) IsValid() error {
	// Check if version matches.
	if args.Version != Version {
		return errServerVersionMismatch
	}

	if !isRequestTimeAllowed(args.RequestTime) {
		return errServerTimeMismatch
	}

	return nil
}

// LoginRPCReply - login reply is a dummy struct perhaps for future use.
type LoginRPCReply struct{}

// LockArgs represents arguments for any authenticated lock RPC call.
type LockArgs struct {
	AuthRPCArgs
	LockArgs dsync.LockArgs
}

func newLockArgs(args dsync.LockArgs) LockArgs {
	return LockArgs{LockArgs: args}
}
