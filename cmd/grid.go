// Copyright (c) 2015-2023 MinIO, Inc.
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
	"context"
	"crypto/tls"
	"sync/atomic"

	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/grid"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/rest"
)

// globalGrid is the global grid manager.
var globalGrid atomic.Pointer[grid.Manager]

// globalLockGrid is the global lock grid manager.
var globalLockGrid atomic.Pointer[grid.Manager]

// globalGridStart is a channel that will block startup of grid connections until closed.
var globalGridStart = make(chan struct{})

// globalLockGridStart is a channel that will block startup of lock grid connections until closed.
var globalLockGridStart = make(chan struct{})

func initGlobalGrid(ctx context.Context, eps EndpointServerPools) error {
	hosts, local := eps.GridHosts()
	lookupHost := globalDNSCache.LookupHost
	g, err := grid.NewManager(ctx, grid.ManagerOptions{
		// Pass Dialer for websocket grid, make sure we do not
		// provide any DriveOPTimeout() function, as that is not
		// useful over persistent connections.
		Dialer: grid.ConnectWS(
			grid.ContextDialer(xhttp.DialContextWithLookupHost(lookupHost, xhttp.NewInternodeDialContext(rest.DefaultTimeout, globalTCPOptions.ForWebsocket()))),
			newCachedAuthToken(),
			&tls.Config{
				RootCAs:          globalRootCAs,
				CipherSuites:     crypto.TLSCiphers(),
				CurvePreferences: crypto.TLSCurveIDs(),
			}),
		Local:        local,
		Hosts:        hosts,
		AuthToken:    validateStorageRequestToken,
		AuthFn:       newCachedAuthToken(),
		BlockConnect: globalGridStart,
		// Record incoming and outgoing bytes.
		Incoming:  globalConnStats.incInternodeInputBytes,
		Outgoing:  globalConnStats.incInternodeOutputBytes,
		TraceTo:   globalTrace,
		RoutePath: grid.RoutePath,
	})
	if err != nil {
		return err
	}
	globalGrid.Store(g)
	return nil
}

func initGlobalLockGrid(ctx context.Context, eps EndpointServerPools) error {
	hosts, local := eps.GridHosts()
	lookupHost := globalDNSCache.LookupHost
	g, err := grid.NewManager(ctx, grid.ManagerOptions{
		// Pass Dialer for websocket grid, make sure we do not
		// provide any DriveOPTimeout() function, as that is not
		// useful over persistent connections.
		Dialer: grid.ConnectWSWithRoutePath(
			grid.ContextDialer(xhttp.DialContextWithLookupHost(lookupHost, xhttp.NewInternodeDialContext(rest.DefaultTimeout, globalTCPOptions.ForWebsocket()))),
			newCachedAuthToken(),
			&tls.Config{
				RootCAs:          globalRootCAs,
				CipherSuites:     crypto.TLSCiphers(),
				CurvePreferences: crypto.TLSCurveIDs(),
			}, grid.RouteLockPath),
		Local:        local,
		Hosts:        hosts,
		AuthToken:    validateStorageRequestToken,
		AuthFn:       newCachedAuthToken(),
		BlockConnect: globalGridStart,
		// Record incoming and outgoing bytes.
		Incoming:  globalConnStats.incInternodeInputBytes,
		Outgoing:  globalConnStats.incInternodeOutputBytes,
		TraceTo:   globalTrace,
		RoutePath: grid.RouteLockPath,
	})
	if err != nil {
		return err
	}
	globalLockGrid.Store(g)
	return nil
}
