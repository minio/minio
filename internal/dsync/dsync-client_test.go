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

package dsync

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/rest"
)

// ReconnectRESTClient is a wrapper type for rest.Client which provides reconnect on first failure.
type ReconnectRESTClient struct {
	u    *url.URL
	rest *rest.Client
}

// newClient constructs a ReconnectRESTClient object with addr and endpoint initialized.
// It _doesn't_ connect to the remote endpoint. See Call method to see when the
// connect happens.
func newClient(endpoint string) NetLocker {
	u, err := url.Parse(endpoint)
	if err != nil {
		panic(err)
	}

	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConnsPerHost:   1024,
		WriteBufferSize:       32 << 10, // 32KiB moving up from 4KiB default
		ReadBufferSize:        32 << 10, // 32KiB moving up from 4KiB default
		IdleConnTimeout:       15 * time.Second,
		ResponseHeaderTimeout: 15 * time.Minute, // Set conservative timeouts for MinIO internode.
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 15 * time.Second,
		// Go net/http automatically unzip if content-type is
		// gzip disable this feature, as we are always interested
		// in raw stream.
		DisableCompression: true,
	}

	return &ReconnectRESTClient{
		u:    u,
		rest: rest.NewClient(u, tr, nil),
	}
}

// Close closes the underlying socket file descriptor.
func (restClient *ReconnectRESTClient) IsOnline() bool {
	// If rest client has not connected yet there is nothing to close.
	return restClient.rest != nil
}

func (restClient *ReconnectRESTClient) IsLocal() bool {
	return false
}

// Close closes the underlying socket file descriptor.
func (restClient *ReconnectRESTClient) Close() error {
	return nil
}

var (
	errLockConflict = errors.New("lock conflict")
	errLockNotFound = errors.New("lock not found")
)

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

// Call makes a REST call to the remote endpoint using the msgp codec
func (restClient *ReconnectRESTClient) Call(method string, args LockArgs) (status bool, err error) {
	buf, err := args.MarshalMsg(nil)
	if err != nil {
		return false, err
	}
	body := bytes.NewReader(buf)
	respBody, err := restClient.rest.Call(context.Background(), method,
		url.Values{}, body, body.Size())
	defer xhttp.DrainBody(respBody)

	switch toLockError(err) {
	case nil:
		return true, nil
	case errLockConflict, errLockNotFound:
		return false, nil
	default:
		return false, err
	}
}

func (restClient *ReconnectRESTClient) RLock(ctx context.Context, args LockArgs) (status bool, err error) {
	return restClient.Call("/v1/rlock", args)
}

func (restClient *ReconnectRESTClient) Lock(ctx context.Context, args LockArgs) (status bool, err error) {
	return restClient.Call("/v1/lock", args)
}

func (restClient *ReconnectRESTClient) RUnlock(ctx context.Context, args LockArgs) (status bool, err error) {
	return restClient.Call("/v1/runlock", args)
}

func (restClient *ReconnectRESTClient) Unlock(ctx context.Context, args LockArgs) (status bool, err error) {
	return restClient.Call("/v1/unlock", args)
}

func (restClient *ReconnectRESTClient) Refresh(ctx context.Context, args LockArgs) (refreshed bool, err error) {
	return restClient.Call("/v1/refresh", args)
}

func (restClient *ReconnectRESTClient) ForceUnlock(ctx context.Context, args LockArgs) (reply bool, err error) {
	return restClient.Call("/v1/force-unlock", args)
}

func (restClient *ReconnectRESTClient) String() string {
	return restClient.u.String()
}
