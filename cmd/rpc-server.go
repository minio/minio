/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"io"
	"net/http"
	"net/rpc"

	miniohttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
)

// ServeHTTP implements an http.Handler that answers RPC requests,
// hijacks the underlying connection and clears all deadlines if any.
func (server *rpcServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodConnect {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("remoteaddr", req.RemoteAddr)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return
	}

	// Overrides Read/Write deadlines if any.
	bufConn, ok := conn.(*miniohttp.BufConn)
	if ok {
		bufConn.RemoveTimeout()
		conn = bufConn
	}

	// Can connect to RPC service using HTTP CONNECT to rpcPath.
	io.WriteString(conn, "HTTP/1.0 200 Connected to Go RPC\n\n")
	server.ServeConn(conn)
}

type rpcServer struct{ *rpc.Server }

// Similar to rpc.NewServer() provides a custom ServeHTTP override.
func newRPCServer() *rpcServer {
	return &rpcServer{rpc.NewServer()}
}
