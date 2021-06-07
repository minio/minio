// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strconv"

	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/http"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/rest"
)

// lockRESTClient is authenticable lock REST client
type lockRESTClient struct {
	restClient *rest.Client
	u          *url.URL
}

func toLockError(err error) error {
	if err == nil {
		return nil
	}

	switch err.Error() {
	case errLockConflict.Error():
		return errLockConflict
	case errLockNotFound.Error():
		return errLockNotFound
	}
	return err
}

// String stringer *dsync.NetLocker* interface compatible method.
func (client *lockRESTClient) String() string {
	return client.u.String()
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *lockRESTClient) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.Call(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	return nil, toLockError(err)
}

// IsOnline - returns whether REST client failed to connect or not.
func (client *lockRESTClient) IsOnline() bool {
	return client.restClient.IsOnline()
}

// Not a local locker
func (client *lockRESTClient) IsLocal() bool {
	return false
}

// Close - marks the client as closed.
func (client *lockRESTClient) Close() error {
	client.restClient.Close()
	return nil
}

// restCall makes a call to the lock REST server.
func (client *lockRESTClient) restCall(ctx context.Context, call string, args dsync.LockArgs) (reply bool, err error) {
	values := url.Values{}
	values.Set(lockRESTUID, args.UID)
	values.Set(lockRESTOwner, args.Owner)
	values.Set(lockRESTSource, args.Source)
	values.Set(lockRESTQuorum, strconv.Itoa(args.Quorum))
	var buffer bytes.Buffer
	for _, resource := range args.Resources {
		buffer.WriteString(resource)
		buffer.WriteString("\n")
	}
	respBody, err := client.callWithContext(ctx, call, values, &buffer, -1)
	defer http.DrainBody(respBody)
	switch err {
	case nil:
		return true, nil
	case errLockConflict, errLockNotFound:
		return false, nil
	default:
		return false, err
	}
}

// RLock calls read lock REST API.
func (client *lockRESTClient) RLock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return client.restCall(ctx, lockRESTMethodRLock, args)
}

// Lock calls lock REST API.
func (client *lockRESTClient) Lock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return client.restCall(ctx, lockRESTMethodLock, args)
}

// RUnlock calls read unlock REST API.
func (client *lockRESTClient) RUnlock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return client.restCall(ctx, lockRESTMethodRUnlock, args)
}

// RUnlock calls read unlock REST API.
func (client *lockRESTClient) Refresh(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return client.restCall(ctx, lockRESTMethodRefresh, args)
}

// Unlock calls write unlock RPC.
func (client *lockRESTClient) Unlock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return client.restCall(ctx, lockRESTMethodUnlock, args)
}

// ForceUnlock calls force unlock handler to forcibly unlock an active lock.
func (client *lockRESTClient) ForceUnlock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return client.restCall(ctx, lockRESTMethodForceUnlock, args)
}

func newLockAPI(endpoint Endpoint) dsync.NetLocker {
	if endpoint.IsLocal {
		return globalLockServer
	}
	return newlockRESTClient(endpoint)
}

// Returns a lock rest client.
func newlockRESTClient(endpoint Endpoint) *lockRESTClient {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   pathJoin(lockRESTPrefix, lockRESTVersion),
	}

	restClient := rest.NewClient(serverURL, globalInternodeTransport, newAuthToken)
	restClient.ExpectTimeouts = true
	// Use a separate client to avoid recursive calls.
	healthClient := rest.NewClient(serverURL, globalInternodeTransport, newAuthToken)
	healthClient.ExpectTimeouts = true
	healthClient.NoMetrics = true
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), restClient.HealthCheckTimeout)
		defer cancel()
		respBody, err := healthClient.Call(ctx, lockRESTMethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		return !isNetworkError(err)
	}

	return &lockRESTClient{u: &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
	}, restClient: restClient}
}
