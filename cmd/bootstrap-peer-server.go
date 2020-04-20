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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v6/pkg/set"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/rest"
)

const (
	bootstrapRESTVersion       = "v1"
	bootstrapRESTVersionPrefix = SlashSeparator + bootstrapRESTVersion
	bootstrapRESTPrefix        = minioReservedBucketPath + "/bootstrap"
	bootstrapRESTPath          = bootstrapRESTPrefix + bootstrapRESTVersionPrefix
)

const (
	bootstrapRESTMethodVerify = "/verify"
)

// To abstract a node over network.
type bootstrapRESTServer struct{}

// ServerSystemConfig - captures information about server configuration.
type ServerSystemConfig struct {
	MinioPlatform  string
	MinioRuntime   string
	MinioEndpoints EndpointZones
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

	subrouter.Methods(http.MethodPost).Path(bootstrapRESTVersionPrefix + bootstrapRESTMethodVerify).HandlerFunc(
		httpTraceHdrs(server.VerifyHandler))
}

// client to talk to bootstrap NEndpoints.
type bootstrapRESTClient struct {
	endpoint   Endpoint
	restClient *rest.Client
	connected  int32
}

// Reconnect to a bootstrap rest server.k
func (client *bootstrapRESTClient) reConnect() {
	atomic.StoreInt32(&client.connected, 1)
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *bootstrapRESTClient) call(method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	return client.callWithContext(GlobalContext, method, values, body, length)
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *bootstrapRESTClient) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if !client.IsOnline() {
		client.reConnect()
	}

	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.CallWithContext(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	if isNetworkError(err) {
		atomic.StoreInt32(&client.connected, 0)
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *bootstrapRESTClient) String() string {
	return client.endpoint.String()
}

// IsOnline - returns whether RPC client failed to connect or not.
func (client *bootstrapRESTClient) IsOnline() bool {
	return atomic.LoadInt32(&client.connected) == 1
}

// Close - marks the client as closed.
func (client *bootstrapRESTClient) Close() error {
	atomic.StoreInt32(&client.connected, 0)
	client.restClient.Close()
	return nil
}

// Verify - fetches system server config.
func (client *bootstrapRESTClient) Verify(srcCfg ServerSystemConfig) (err error) {
	if newObjectLayerFn() != nil {
		return nil
	}
	respBody, err := client.call(bootstrapRESTMethodVerify, nil, nil, -1)
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

func verifyServerSystemConfig(endpointZones EndpointZones) error {
	srcCfg := getServerSystemCfg()
	clnts := newBootstrapRESTClients(endpointZones)
	var onlineServers int
	for onlineServers < len(clnts)/2 {
		for _, clnt := range clnts {
			if err := clnt.Verify(srcCfg); err != nil {
				if isNetworkError(err) {
					continue
				}
				return fmt.Errorf("%s as has incorrect configuration: %w", clnt.String(), err)
			}
			onlineServers++
		}
		// Sleep for a while - so that we don't go into
		// 100% CPU when half the endpoints are offline.
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func newBootstrapRESTClients(endpointZones EndpointZones) []*bootstrapRESTClient {
	seenHosts := set.NewStringSet()
	var clnts []*bootstrapRESTClient
	for _, ep := range endpointZones {
		for _, endpoint := range ep.Endpoints {
			if seenHosts.Contains(endpoint.Host) {
				continue
			}
			seenHosts.Add(endpoint.Host)

			// Only proceed for remote endpoints.
			if !endpoint.IsLocal {
				clnt, err := newBootstrapRESTClient(endpoint)
				if err != nil {
					continue
				}
				clnts = append(clnts, clnt)
			}
		}
	}
	return clnts
}

// Returns a new bootstrap client.
func newBootstrapRESTClient(endpoint Endpoint) (*bootstrapRESTClient, error) {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   bootstrapRESTPath,
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: endpoint.Hostname(),
			RootCAs:    globalRootCAs,
		}
	}

	trFn := newCustomHTTPTransport(tlsConfig, rest.DefaultRESTTimeout)
	restClient, err := rest.NewClient(serverURL, trFn, newAuthToken)
	if err != nil {
		return nil, err
	}

	return &bootstrapRESTClient{endpoint: endpoint, restClient: restClient, connected: 1}, nil
}
