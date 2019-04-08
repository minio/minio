/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"io"

	"net/url"

	"github.com/minio/dsync"
	"github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/rest"
	xnet "github.com/minio/minio/pkg/net"
)

// lockRESTClient is authenticable lock REST client
type lockRESTClient struct {
	host       *xnet.Host
	restClient *rest.Client
	serverURL  *url.URL
	connected  bool
}

// ServerAddr - dsync.NetLocker interface compatible method.
func (client *lockRESTClient) ServerAddr() string {
	return client.serverURL.Host
}

// ServiceEndpoint - dsync.NetLocker interface compatible method.
func (client *lockRESTClient) ServiceEndpoint() string {
	return client.serverURL.Path
}

// Reconnect to a lock rest server.
func (client *lockRESTClient) reConnect() error {
	// correct (intelligent) retry logic will be
	// implemented in subsequent PRs.
	client.connected = true
	return nil
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *lockRESTClient) call(method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if !client.connected {
		err := client.reConnect()
		logger.LogIf(context.Background(), err)
		if err != nil {
			return nil, err
		}
	}

	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.Call(method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	if isNetworkError(err) {
		client.connected = false
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *lockRESTClient) String() string {
	return client.host.String()
}

// IsOnline - returns whether REST client failed to connect or not.
func (client *lockRESTClient) IsOnline() bool {
	return client.connected
}

// Close - marks the client as closed.
func (client *lockRESTClient) Close() error {
	client.connected = false
	client.restClient.Close()
	return nil
}

// RLock calls read lock REST API.
func (client *lockRESTClient) RLock(args dsync.LockArgs) (reply bool, err error) {

	reader := bytes.NewBuffer(make([]byte, 0, 1024))
	err = gob.NewEncoder(reader).Encode(args)
	if err != nil {
		return false, err
	}
	respBody, err := client.call(lockRESTMethodRLock, nil, reader, -1)
	if err != nil {
		return false, err
	}

	var resp lockResponse
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&resp)

	if err != nil || !resp.Success {
		reqInfo := &logger.ReqInfo{}
		reqInfo.AppendTags("resource", args.Resource)
		reqInfo.AppendTags("serveraddress", args.ServerAddr)
		reqInfo.AppendTags("serviceendpoint", args.ServiceEndpoint)
		reqInfo.AppendTags("source", args.Source)
		reqInfo.AppendTags("uid", args.UID)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
	}
	return resp.Success, err
}

// Lock calls read lock REST API.
func (client *lockRESTClient) Lock(args dsync.LockArgs) (reply bool, err error) {

	reader := bytes.NewBuffer(make([]byte, 0, 1024))
	err = gob.NewEncoder(reader).Encode(args)
	if err != nil {
		return false, err
	}
	respBody, err := client.call(lockRESTMethodLock, nil, reader, -1)
	if err != nil {
		return false, err
	}

	var resp lockResponse
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&resp)

	if err != nil || !resp.Success {
		reqInfo := &logger.ReqInfo{}
		reqInfo.AppendTags("resource", args.Resource)
		reqInfo.AppendTags("serveraddress", args.ServerAddr)
		reqInfo.AppendTags("serviceendpoint", args.ServiceEndpoint)
		reqInfo.AppendTags("source", args.Source)
		reqInfo.AppendTags("uid", args.UID)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
	}

	return resp.Success, err
}

// RUnlock calls read unlock RPC.
func (client *lockRESTClient) RUnlock(args dsync.LockArgs) (reply bool, err error) {
	reader := bytes.NewBuffer(make([]byte, 0, 1024))
	err = gob.NewEncoder(reader).Encode(args)
	if err != nil {
		return false, err
	}
	respBody, err := client.call(lockRESTMethodRUnlock, nil, reader, -1)
	if err != nil {
		return false, err
	}

	var resp lockResponse
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&resp)

	if err != nil || !resp.Success {
		reqInfo := &logger.ReqInfo{}
		reqInfo.AppendTags("resource", args.Resource)
		reqInfo.AppendTags("serveraddress", args.ServerAddr)
		reqInfo.AppendTags("serviceendpoint", args.ServiceEndpoint)
		reqInfo.AppendTags("source", args.Source)
		reqInfo.AppendTags("uid", args.UID)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
	}

	return resp.Success, err
}

// Unlock calls write unlock RPC.
func (client *lockRESTClient) Unlock(args dsync.LockArgs) (reply bool, err error) {
	reader := bytes.NewBuffer(make([]byte, 0, 1024))
	err = gob.NewEncoder(reader).Encode(args)
	if err != nil {
		return false, err
	}
	respBody, err := client.call(lockRESTMethodUnlock, nil, reader, -1)
	if err != nil {
		return false, err
	}

	var resp lockResponse
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&resp)

	if err != nil || !resp.Success {
		reqInfo := &logger.ReqInfo{}
		reqInfo.AppendTags("resource", args.Resource)
		reqInfo.AppendTags("serveraddress", args.ServerAddr)
		reqInfo.AppendTags("serviceendpoint", args.ServiceEndpoint)
		reqInfo.AppendTags("source", args.Source)
		reqInfo.AppendTags("uid", args.UID)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
	}

	return resp.Success, err
}

// ForceUnlock calls force unlock RPC.
func (client *lockRESTClient) ForceUnlock(args dsync.LockArgs) (reply bool, err error) {
	reader := bytes.NewBuffer(make([]byte, 0, 1024))
	err = gob.NewEncoder(reader).Encode(args)
	if err != nil {
		return false, err
	}
	respBody, err := client.call(lockRESTMethodForceUnlock, nil, reader, -1)
	if err != nil {
		return false, err
	}

	var resp lockResponse
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&resp)

	if err != nil || !resp.Success {
		reqInfo := &logger.ReqInfo{}
		reqInfo.AppendTags("resource", args.Resource)
		reqInfo.AppendTags("serveraddress", args.ServerAddr)
		reqInfo.AppendTags("serviceendpoint", args.ServiceEndpoint)
		reqInfo.AppendTags("source", args.Source)
		reqInfo.AppendTags("uid", args.UID)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
	}

	return resp.Success, err
}

// Expired calls expired RPC.
func (client *lockRESTClient) Expired(args dsync.LockArgs) (reply bool, err error) {
	reader := bytes.NewBuffer(make([]byte, 0, 1024))
	err = gob.NewEncoder(reader).Encode(args)
	if err != nil {
		return false, err
	}
	respBody, err := client.call(lockRESTMethodExpired, nil, reader, -1)
	if err != nil {
		return false, err
	}

	var resp lockResponse
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&resp)

	if err != nil || !resp.Success {
		reqInfo := &logger.ReqInfo{}
		reqInfo.AppendTags("resource", args.Resource)
		reqInfo.AppendTags("serveraddress", args.ServerAddr)
		reqInfo.AppendTags("serviceendpoint", args.ServiceEndpoint)
		reqInfo.AppendTags("source", args.Source)
		reqInfo.AppendTags("uid", args.UID)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
	}

	return resp.Success, err
}

// Returns a lock rest client.
func newlockRESTClient(peer *xnet.Host) (*lockRESTClient, error) {

	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   peer.String(),
		Path:   lockRESTPath,
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: peer.Name,
			RootCAs:    globalRootCAs,
		}
	}

	restClient, err := rest.NewClient(serverURL, tlsConfig, rest.DefaultRESTTimeout, newAuthToken)

	if err != nil {
		return nil, err
	}

	return &lockRESTClient{serverURL: serverURL, host: peer, restClient: restClient, connected: true}, nil
}
