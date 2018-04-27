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
	"context"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
)

// Set up an RPC endpoint that receives browser related calls. The
// original motivation is for propagating credentials change
// throughout Minio cluster, initiated from a Minio browser session.

const (
	browserPeerPath = "/browser/setauth"
)

// The Type exporting methods exposed for RPC calls.
type browserPeerAPIHandlers struct {
	AuthRPCServer
}

// Register RPC router
func registerBrowserPeerRPCRouter(router *mux.Router) error {
	bpHandlers := &browserPeerAPIHandlers{AuthRPCServer{}}

	bpRPCServer := newRPCServer()
	err := bpRPCServer.RegisterName("BrowserPeer", bpHandlers)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}

	bpRouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
	bpRouter.Path(browserPeerPath).Handler(bpRPCServer)
	return nil
}
