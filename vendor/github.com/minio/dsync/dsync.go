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

import (
	"errors"
	"net/rpc"
)

const RpcPath = "/dsync"
const DebugPath = "/debug"

const DefaultPath = "/rpc/dsync"

var n int
var nodes []string
var rpcPaths []string
var clnts []*rpc.Client

func closeClients(clients []*rpc.Client) {
	for _, clnt := range clients {
		clnt.Close()
	}
}

// Same as SetNodes, but takes a slice of rpc paths as argument different from the package-level default.
func SetNodesWithPath(nodeList []string, paths []string) (err error) {

	// Validate if number of nodes is within allowable range.
	if n != 0 {
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
	n = len(nodes)
	return nil
}
