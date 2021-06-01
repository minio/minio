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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7/pkg/set"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/rest"
)

const (
	bootstrapRESTVersion       = "v1"
	bootstrapRESTVersionPrefix = SlashSeparator + bootstrapRESTVersion
	bootstrapRESTPrefix        = minioReservedBucketPath + "/bootstrap"
	bootstrapRESTPath          = bootstrapRESTPrefix + bootstrapRESTVersionPrefix
)

const (
	bootstrapRESTMethodHealth = "/health"
	bootstrapRESTMethodVerify = "/verify"
)

// To abstract a node over network.
type bootstrapRESTServer struct{}

// ServerSystemConfig - captures information about server configuration.
type ServerSystemConfig struct {
	MinioPlatform  string
	MinioRuntime   string
	MinioEndpoints EndpointServerPools
}

// Diff - returns error on first difference found in two configs.
func (s1 ServerSystemConfig) Diff(s2 ServerSystemConfig) error {
	if s1.MinioPlatform != s2.MinioPlatform {
		return fmt.Errorf("Expected platform '%s', found to be running '%s'",
			s1.MinioPlatform, s2.MinioPlatform)
	}
	if s1.MinioEndpoints.NEndpoints() != s2.MinioEndpoints.NEndpoints() {
		return fmt.Errorf("Expected number of endpoints %d, seen %d", s1.MinioEndpoints.NEndpoints(),
			s2.MinioEndpoints.NEndpoints())
	}

	for i, ep := range s1.MinioEndpoints {
		if ep.SetCount != s2.MinioEndpoints[i].SetCount {
			return fmt.Errorf("Expected set count %d, seen %d", ep.SetCount,
				s2.MinioEndpoints[i].SetCount)
		}
		if ep.DrivesPerSet != s2.MinioEndpoints[i].DrivesPerSet {
			return fmt.Errorf("Expected drives pet set %d, seen %d", ep.DrivesPerSet,
				s2.MinioEndpoints[i].DrivesPerSet)
		}
		for j, endpoint := range ep.Endpoints {
			if endpoint.String() != s2.MinioEndpoints[i].Endpoints[j].String() {
				return fmt.Errorf("Expected endpoint %s, seen %s", endpoint,
					s2.MinioEndpoints[i].Endpoints[j])
			}
		}

	}
	return nil
}

func getServerSystemCfg() ServerSystemConfig {
	return ServerSystemConfig{
		MinioPlatform:  fmt.Sprintf("OS: %s | Arch: %s", runtime.GOOS, runtime.GOARCH),
		MinioEndpoints: globalEndpoints,
	}
}

// HealthHandler returns success if request is valid
func (b *bootstrapRESTServer) HealthHandler(w http.ResponseWriter, r *http.Request) {}

func (b *bootstrapRESTServer) VerifyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "VerifyHandler")
	cfg := getServerSystemCfg()
	logger.LogIf(ctx, json.NewEncoder(w).Encode(&cfg))
	w.(http.Flusher).Flush()
}

// registerBootstrapRESTHandlers - register bootstrap rest router.
func registerBootstrapRESTHandlers(router *mux.Router) {
	server := &bootstrapRESTServer{}
	subrouter := router.PathPrefix(bootstrapRESTPrefix).Subrouter()

	subrouter.Methods(http.MethodPost).Path(bootstrapRESTVersionPrefix + bootstrapRESTMethodHealth).HandlerFunc(
		httpTraceHdrs(server.HealthHandler))

	subrouter.Methods(http.MethodPost).Path(bootstrapRESTVersionPrefix + bootstrapRESTMethodVerify).HandlerFunc(
		httpTraceHdrs(server.VerifyHandler))
}

// client to talk to bootstrap NEndpoints.
type bootstrapRESTClient struct {
	endpoint   Endpoint
	restClient *rest.Client
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *bootstrapRESTClient) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.Call(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *bootstrapRESTClient) String() string {
	return client.endpoint.String()
}

// Verify - fetches system server config.
func (client *bootstrapRESTClient) Verify(ctx context.Context, srcCfg ServerSystemConfig) (err error) {
	if newObjectLayerFn() != nil {
		return nil
	}
	respBody, err := client.callWithContext(ctx, bootstrapRESTMethodVerify, nil, nil, -1)
	if err != nil {
		return
	}
	defer xhttp.DrainBody(respBody)
	recvCfg := ServerSystemConfig{}
	if err = json.NewDecoder(respBody).Decode(&recvCfg); err != nil {
		return err
	}
	return srcCfg.Diff(recvCfg)
}

func verifyServerSystemConfig(ctx context.Context, endpointServerPools EndpointServerPools) error {
	srcCfg := getServerSystemCfg()
	clnts := newBootstrapRESTClients(endpointServerPools)
	var onlineServers int
	var offlineEndpoints []string
	var retries int
	for onlineServers < len(clnts)/2 {
		for _, clnt := range clnts {
			if err := clnt.Verify(ctx, srcCfg); err != nil {
				if isNetworkError(err) {
					offlineEndpoints = append(offlineEndpoints, clnt.String())
					continue
				}
				return fmt.Errorf("%s as has incorrect configuration: %w", clnt.String(), err)
			}
			onlineServers++
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Sleep for a while - so that we don't go into
			// 100% CPU when half the endpoints are offline.
			time.Sleep(100 * time.Millisecond)
			retries++
			// after 5 retries start logging that servers are not reachable yet
			if retries >= 5 {
				logger.Info(fmt.Sprintf("Waiting for atleast %d remote servers to be online for bootstrap check", len(clnts)/2))
				logger.Info(fmt.Sprintf("Following servers are currently offline or unreachable %s", offlineEndpoints))
				retries = 0 // reset to log again after 5 retries.
			}
			offlineEndpoints = nil
		}
	}
	return nil
}

func newBootstrapRESTClients(endpointServerPools EndpointServerPools) []*bootstrapRESTClient {
	seenHosts := set.NewStringSet()
	var clnts []*bootstrapRESTClient
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			if seenHosts.Contains(endpoint.Host) {
				continue
			}
			seenHosts.Add(endpoint.Host)

			// Only proceed for remote endpoints.
			if !endpoint.IsLocal {
				clnts = append(clnts, newBootstrapRESTClient(endpoint))
			}
		}
	}
	return clnts
}

// Returns a new bootstrap client.
func newBootstrapRESTClient(endpoint Endpoint) *bootstrapRESTClient {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   bootstrapRESTPath,
	}

	restClient := rest.NewClient(serverURL, globalInternodeTransport, newAuthToken)
	restClient.HealthCheckFn = nil

	return &bootstrapRESTClient{endpoint: endpoint, restClient: restClient}
}
