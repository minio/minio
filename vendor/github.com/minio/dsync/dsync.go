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

// List of nodes participating.
var nodes []string

// List of rpc paths, one per lock server.
var rpcPaths []string

// List of rpc client objects, one per lock server.
var clnts []*RPCClient

// Simple majority based quorum, set to dNodeCount/2+1
var dquorum int

// SetNodesWithPath - initializes package-level global state variables such as
// nodes, rpcPaths, clnts.
// N B - This function should be called only once inside any program that uses
// dsync.
func SetNodesWithPath(nodeList []string, paths []string) (err error) {

	// Validate if number of nodes is within allowable range.
	if dnodeCount != 0 {
		return errors.New("Cannot reinitialize dsync package")
	} else if len(nodeList) < 4 {
		return errors.New("Dsync not designed for less than 4 nodes")
	} else if len(nodeList) > 16 {
		return errors.New("Dsync not designed for more than 16 nodes")
	}

	nodes = make([]string, len(nodeList))
	copy(nodes, nodeList[:])
	rpcPaths = make([]string, len(paths))
	copy(rpcPaths, paths[:])
	dnodeCount = len(nodes)
	dquorum = dnodeCount/2 + 1
	clnts = make([]*RPCClient, dnodeCount)
	// Initialize node name and rpc path for each RPCClient object.
	for i := range clnts {
		clnts[i] = newClient(nodes[i], rpcPaths[i])
	}
	return nil
}
