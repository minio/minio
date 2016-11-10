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

import "encoding/json"

// BucketMetaState - Interface to update bucket metadata in-memory
// state.
type BucketMetaState interface {
	// Updates bucket notification
	UpdateBucketNotification(args *SetBNPArgs) error

	// Updates bucket listener
	UpdateBucketListener(args *SetBLPArgs) error

	// Updates bucket policy
	UpdateBucketPolicy(args *SetBPPArgs) error

	// Sends event
	SendEvent(args *EventArgs) error
}

// Type that implements BucketMetaState for local node.
type localBMS struct {
	ObjectAPI func() ObjectLayer
}

// localBMS.UpdateBucketNotification - updates in-memory global bucket
// notification info.
func (lc *localBMS) UpdateBucketNotification(args *SetBNPArgs) error {
	// check if object layer is available.
	objAPI := lc.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}

	globalEventNotifier.SetBucketNotificationConfig(args.Bucket, args.NCfg)

	return nil
}

// localBMS.UpdateBucketListener - updates in-memory global bucket
// listeners info.
func (lc *localBMS) UpdateBucketListener(args *SetBLPArgs) error {
	// check if object layer is available.
	objAPI := lc.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}

	// Update in-memory notification config.
	return globalEventNotifier.SetBucketListenerConfig(args.Bucket, args.LCfg)
}

// localBMS.UpdateBucketPolicy - updates in-memory global bucket
// policy info.
func (lc *localBMS) UpdateBucketPolicy(args *SetBPPArgs) error {
	// check if object layer is available.
	objAPI := lc.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}

	var pCh policyChange
	if err := json.Unmarshal(args.PChBytes, &pCh); err != nil {
		return err
	}

	return globalBucketPolicies.SetBucketPolicy(args.Bucket, pCh)
}

// localBMS.SendEvent - sends event to local event notifier via
// `globalEventNotifier`
func (lc *localBMS) SendEvent(args *EventArgs) error {
	// check if object layer is available.
	objAPI := lc.ObjectAPI()
	if objAPI == nil {
		return errServerNotInitialized
	}

	return globalEventNotifier.SendListenerEvent(args.Arn, args.Event)
}

// Type that implements BucketMetaState for remote node.
type remoteBMS struct {
	*AuthRPCClient
}

// remoteBMS.UpdateBucketNotification - sends bucket notification
// change to remote peer via RPC call.
func (rc *remoteBMS) UpdateBucketNotification(args *SetBNPArgs) error {
	reply := GenericReply{}
	return rc.Call("S3.SetBucketNotificationPeer", args, &reply)
}

// remoteBMS.UpdateBucketListener - sends bucket listener change to
// remote peer via RPC call.
func (rc *remoteBMS) UpdateBucketListener(args *SetBLPArgs) error {
	reply := GenericReply{}
	return rc.Call("S3.SetBucketListenerPeer", args, &reply)
}

// remoteBMS.UpdateBucketPolicy - sends bucket policy change to remote
// peer via RPC call.
func (rc *remoteBMS) UpdateBucketPolicy(args *SetBPPArgs) error {
	reply := GenericReply{}
	return rc.Call("S3.SetBucketPolicyPeer", args, &reply)
}

// remoteBMS.SendEvent - sends event for bucket listener to remote
// peer via RPC call.
func (rc *remoteBMS) SendEvent(args *EventArgs) error {
	reply := GenericReply{}
	return rc.Call("S3.Event", args, &reply)
}
