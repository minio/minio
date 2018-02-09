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

// BucketMetaState - Interface to update bucket metadata in-memory
// state.
type BucketMetaState interface {
	// Updates bucket notification
	UpdateBucketNotification(args *SetBucketNotificationPeerArgs) error

	// Updates bucket listener
	UpdateBucketListener(args *SetBucketListenerPeerArgs) error

	// Updates bucket policy
	UpdateBucketPolicy(args *SetBucketPolicyPeerArgs) error

	// Sends event
	SendEvent(args *EventArgs) error
}

// BucketUpdater - Interface implementer calls one of BucketMetaState's methods.
type BucketUpdater interface {
	BucketUpdate(client BucketMetaState) error
}

// Type that implements BucketMetaState for local node.
type localBucketMetaState struct {
	ObjectAPI func() ObjectLayer
}

// localBucketMetaState.UpdateBucketNotification - updates in-memory global bucket
// notification info.
func (lc *localBucketMetaState) UpdateBucketNotification(args *SetBucketNotificationPeerArgs) error {
	// check if object layer is available.
	objAPI := lc.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}

	globalEventNotifier.SetBucketNotificationConfig(args.Bucket, args.NCfg)

	return nil
}

// localBucketMetaState.UpdateBucketListener - updates in-memory global bucket
// listeners info.
func (lc *localBucketMetaState) UpdateBucketListener(args *SetBucketListenerPeerArgs) error {
	// check if object layer is available.
	objAPI := lc.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}

	// Update in-memory notification config.
	return globalEventNotifier.SetBucketListenerConfig(args.Bucket, args.LCfg)
}

// localBucketMetaState.UpdateBucketPolicy - updates in-memory global bucket
// policy info.
func (lc *localBucketMetaState) UpdateBucketPolicy(args *SetBucketPolicyPeerArgs) error {
	// check if object layer is available.
	objAPI := lc.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}
	return objAPI.RefreshBucketPolicy(args.Bucket)
}

// localBucketMetaState.SendEvent - sends event to local event notifier via
// `globalEventNotifier`
func (lc *localBucketMetaState) SendEvent(args *EventArgs) error {
	// check if object layer is available.
	objAPI := lc.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}

	return globalEventNotifier.SendListenerEvent(args.Arn, args.Event)
}

// Type that implements BucketMetaState for remote node.
type remoteBucketMetaState struct {
	*AuthRPCClient
}

// remoteBucketMetaState.UpdateBucketNotification - sends bucket notification
// change to remote peer via RPC call.
func (rc *remoteBucketMetaState) UpdateBucketNotification(args *SetBucketNotificationPeerArgs) error {
	reply := AuthRPCReply{}
	return rc.Call("S3.SetBucketNotificationPeer", args, &reply)
}

// remoteBucketMetaState.UpdateBucketListener - sends bucket listener change to
// remote peer via RPC call.
func (rc *remoteBucketMetaState) UpdateBucketListener(args *SetBucketListenerPeerArgs) error {
	reply := AuthRPCReply{}
	return rc.Call("S3.SetBucketListenerPeer", args, &reply)
}

// remoteBucketMetaState.UpdateBucketPolicy - sends bucket policy change to remote
// peer via RPC call.
func (rc *remoteBucketMetaState) UpdateBucketPolicy(args *SetBucketPolicyPeerArgs) error {
	reply := AuthRPCReply{}
	return rc.Call("S3.SetBucketPolicyPeer", args, &reply)
}

// remoteBucketMetaState.SendEvent - sends event for bucket listener to remote
// peer via RPC call.
func (rc *remoteBucketMetaState) SendEvent(args *EventArgs) error {
	reply := AuthRPCReply{}
	return rc.Call("S3.Event", args, &reply)
}
