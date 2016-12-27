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

type loginServer struct {
}

// LoginHandler - Handles JWT based RPC logic.
func (b loginServer) LoginHandler(args *RPCLoginArgs, reply *RPCLoginReply) error {
	token, err := authenticateNode(args.Username, args.Password)
	if err != nil {
		return err
	}

	reply.Token = token
	reply.Timestamp = time.Now().UTC()
	reply.ServerVersion = Version
	return nil
}
