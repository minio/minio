/*
 * Minio Cloud Storage, (C) 2014-2016 Minio, Inc.
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

func (n *notifyAPIHandlers) LoginHandler(args *RPCLoginArgs, reply *RPCLoginReply) error {
	jwt, err := newJWT(defaultTokenExpiry)
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
	reply.ServerVersion = Version
	return nil
}

type ReloadArgs struct {
	// For Auth
	GenericArgs

	// Bucket to reload configuration for
	Bucket string
}

// Tells receiving server to reload notification configuration for a
// particular bucket.
func (n *notifyAPIHandlers) ReloadBucketConfig(args *ReloadArgs, reply *GenericReply) error {
	// check auth
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}

	// check if object layer is available.
	objAPI := n.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}

	// load notification config for bucket
	ncfg, err := loadNotificationConfig(args.Bucket, objAPI)
	if err != nil {
		errorIf(err, "Failed to load notification config.")
		return err
	}

	// update into global state.
	globalEventNotifier.UpdateNotificationConfig(args.Bucket, ncfg)
	return nil
}

// Submit an event to the receiving server.
func (n *notifyAPIHandlers) Event(args *GenericArgs, reply *GenericReply) error {
	// TODO: stub
	return nil
}
