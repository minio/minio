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

import (
	"net/rpc"

	router "github.com/gorilla/mux"
)

// Set up an RPC endpoint that receives browser related calls. The
// original motivation is for propagating credentials change
// throughout Minio cluster, initiated from a Minio browser session.

const (
	browserPath = "/browser/setauth"
)

// The Type exporting methods exposed for RPC calls.
type browserAPIHandlers struct {
}

// Register RPC router
func registerBrowserRPCRouter(mux *router.Router) error {
	browserHandlers := &browserAPIHandlers{}

	browserRPCServer := rpc.NewServer()
	err := browserRPCServer.RegisterName("Browser", browserHandlers)
	if err != nil {
		return traceError(err)
	}

	browserRouter := mux.NewRoute().PathPrefix(reservedBucket).Subrouter()
	browserRouter.Path(browserPath).Handler(browserRPCServer)
	return nil
}
