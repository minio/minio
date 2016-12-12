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

import "time"

type serviceCmd struct {
}

/// Auth operations

// Login - logging into the corresponding RPC service using JWT.
func (s serviceCmd) LoginHandler(args *RPCLoginArgs, reply *RPCLoginReply) error {
	jwt, err := newJWT(defaultInterNodeJWTExpiry, serverConfig.GetCredential())
	if err != nil {
		return err
	}
	if err = jwt.Authenticate(args.Username, args.Password); err != nil {
		return err
	}
	token, err := jwt.GenerateToken(args.Username)
	if err != nil {
		return err
	}
	reply.Token = token
	reply.Timestamp = time.Now().UTC()
	reply.ServerVersion = Version
	return nil

}

// Shutdown - Shutdown this instance of minio server.
func (s serviceCmd) Shutdown(args *GenericArgs, reply *GenericReply) error {
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}
	globalServiceSignalCh <- serviceStop
	return nil
}

// Restart - Restart this instance of minio server.
func (s serviceCmd) Restart(args *GenericArgs, reply *GenericReply) error {
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}
	globalServiceSignalCh <- serviceRestart
	return nil
}
