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

// Base login method name.  It should be used along with service name.
const loginMethodName = ".Login"

// AuthRPCServer RPC server authenticates using JWT.
type AuthRPCServer struct{}

// Login - Handles JWT based RPC login.
func (b AuthRPCServer) Login(args *LoginRPCArgs, reply *LoginRPCReply) error {
	// Validate LoginRPCArgs
	if err := args.IsValid(); err != nil {
		return err
	}

	// Return an error if token is not valid.
	if !isAuthTokenValid(args.AuthToken) {
		return errAuthentication
	}

	return nil
}
