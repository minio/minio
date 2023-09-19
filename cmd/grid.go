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

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/fips"
	"github.com/minio/minio/internal/grid"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/rest"
)

// globalGrid is the global grid manager.
var globalGrid atomic.Pointer[grid.Manager]

func initGlobalGrid(ctx context.Context, eps EndpointServerPools) error {
	seenHosts := set.NewStringSet()
	var hosts []string
	var local string
	for _, ep := range eps {
		for _, endpoint := range ep.Endpoints {
			u := endpoint.GridHost()
			if seenHosts.Contains(u) {
				continue
			}
			seenHosts.Add(u)

			// Only proceed for remote endpoints.
			if endpoint.IsLocal {
				local = u
			}
			hosts = append(hosts, u)
		}
	}
	lookupHost := globalDNSCache.LookupHost
	if IsKubernetes() || IsDocker() {
		lookupHost = nil
	}
	g, err := grid.NewManager(ctx, grid.ManagerOptions{
		Dialer: grid.ContextDialer(xhttp.DialContextWithLookupHost(lookupHost, xhttp.NewInternodeDialContext(rest.DefaultTimeout, globalTCPOptions))),
		Local:  local,
		Hosts:  hosts,
		Auth:   newCachedAuthToken(),
		TLSConfig: &tls.Config{
			RootCAs:          globalRootCAs,
			CipherSuites:     fips.TLSCiphers(),
			CurvePreferences: fips.TLSCurveIDs(),
		},
	})
	if err != nil {
		return err
	}
	globalGrid.Store(g)
	return err
}
