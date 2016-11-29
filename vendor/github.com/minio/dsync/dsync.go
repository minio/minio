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

package dsync

import "errors"

const RpcPath = "/dsync"
const DebugPath = "/debug"

const DefaultPath = "/rpc/dsync"

// Number of nodes participating in the distributed locking.
var dnodeCount int

// List of rpc client objects, one per lock server.
var clnts []RPC

// Index into rpc client array for server running on localhost
var ownNode int

// Simple majority based quorum, set to dNodeCount/2+1
var dquorum int

// Simple quorum for read operations, set to dNodeCount/2
var dquorumReads int

// SetNodesWithPath - initializes package-level global state variables such as clnts.
// N B - This function should be called only once inside any program that uses
// dsync.
func SetNodesWithClients(rpcClnts []RPC, rpcOwnNode int) (err error) {

	// Validate if number of nodes is within allowable range.
	if dnodeCount != 0 {
		return errors.New("Cannot reinitialize dsync package")
	} else if len(rpcClnts) < 4 {
		return errors.New("Dsync not designed for less than 4 nodes")
	} else if len(rpcClnts) > 16 {
		return errors.New("Dsync not designed for more than 16 nodes")
	} else if len(rpcClnts)&1 == 1 {
		return errors.New("Dsync not designed for an uneven number of nodes")
	}

	if rpcOwnNode > len(rpcClnts) {
		return errors.New("Index for own node is too large")
	}

	dnodeCount = len(rpcClnts)
	dquorum = dnodeCount/2 + 1
	dquorumReads = dnodeCount / 2
	// Initialize node name and rpc path for each RPCClient object.
	clnts = make([]RPC, dnodeCount)
	copy(clnts, rpcClnts)

	ownNode = rpcOwnNode
	return nil
}
