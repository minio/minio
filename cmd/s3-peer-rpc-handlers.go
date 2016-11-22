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

import "time"

func (s3 *s3PeerAPIHandlers) LoginHandler(args *RPCLoginArgs, reply *RPCLoginReply) error {
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
	reply.ServerVersion = Version
	reply.Timestamp = time.Now().UTC()
	return nil
}

// SetBucketNotificationPeerArgs - Arguments collection to SetBucketNotificationPeer RPC
// call
type SetBucketNotificationPeerArgs struct {
	// For Auth
	GenericArgs

	Bucket string

	// Notification config for the given bucket.
	NCfg *notificationConfig
}

func (s3 *s3PeerAPIHandlers) SetBucketNotificationPeer(args *SetBucketNotificationPeerArgs, reply *GenericReply) error {
	// check auth
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}

	return s3.bms.UpdateBucketNotification(args)
}

// SetBucketListenerPeerArgs - Arguments collection to SetBucketListenerPeer RPC call
type SetBucketListenerPeerArgs struct {
	// For Auth
	GenericArgs

	Bucket string

	// Listener config for a given bucket.
	LCfg []listenerConfig
}

func (s3 *s3PeerAPIHandlers) SetBucketListenerPeer(args *SetBucketListenerPeerArgs, reply *GenericReply) error {
	// check auth
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}

	return s3.bms.UpdateBucketListener(args)
}

// EventArgs - Arguments collection for Event RPC call
type EventArgs struct {
	// For Auth
	GenericArgs

	// event being sent
	Event []NotificationEvent

	// client that it is meant for
	Arn string
}

// submit an event to the receiving server.
func (s3 *s3PeerAPIHandlers) Event(args *EventArgs, reply *GenericReply) error {
	// check auth
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}

	return s3.bms.SendEvent(args)
}

// SetBucketPolicyPeerArgs - Arguments collection for SetBucketPolicyPeer RPC call
type SetBucketPolicyPeerArgs struct {
	// For Auth
	GenericArgs

	Bucket string

	// Policy change (serialized to JSON)
	PChBytes []byte
}

// tell receiving server to update a bucket policy
func (s3 *s3PeerAPIHandlers) SetBucketPolicyPeer(args *SetBucketPolicyPeerArgs, reply *GenericReply) error {
	// check auth
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}

	return s3.bms.UpdateBucketPolicy(args)
}
