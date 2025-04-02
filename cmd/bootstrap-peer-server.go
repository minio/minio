// Copyright (c) 2015-2022 MinIO, Inc.
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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/grid"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/env"
)

// To abstract a node over network.
type bootstrapRESTServer struct{}

//go:generate msgp -file=$GOFILE

// ServerSystemConfig - captures information about server configuration.
type ServerSystemConfig struct {
	NEndpoints int
	CmdLines   []string
	MinioEnv   map[string]string
	Checksum   string
}

// Diff - returns error on first difference found in two configs.
func (s1 *ServerSystemConfig) Diff(s2 *ServerSystemConfig) error {
	if s1.Checksum != s2.Checksum {
		return fmt.Errorf("Expected MinIO binary checksum: %s, seen: %s", s1.Checksum, s2.Checksum)
	}

	ns1 := s1.NEndpoints
	ns2 := s2.NEndpoints
	if ns1 != ns2 {
		return fmt.Errorf("Expected number of endpoints %d, seen %d", ns1, ns2)
	}

	for i, cmdLine := range s1.CmdLines {
		if cmdLine != s2.CmdLines[i] {
			return fmt.Errorf("Expected command line argument %s, seen %s", cmdLine,
				s2.CmdLines[i])
		}
	}

	if reflect.DeepEqual(s1.MinioEnv, s2.MinioEnv) {
		return nil
	}

	// Report differences in environment variables.
	var missing []string
	var mismatching []string
	for k, v := range s1.MinioEnv {
		ev, ok := s2.MinioEnv[k]
		if !ok {
			missing = append(missing, k)
		} else if v != ev {
			mismatching = append(mismatching, k)
		}
	}
	var extra []string
	for k := range s2.MinioEnv {
		_, ok := s1.MinioEnv[k]
		if !ok {
			extra = append(extra, k)
		}
	}
	msg := "Expected MINIO_* environment name and values across all servers to be same: "
	if len(missing) > 0 {
		msg += fmt.Sprintf(`Missing environment values: %v. `, missing)
	}
	if len(mismatching) > 0 {
		msg += fmt.Sprintf(`Mismatching environment values: %v. `, mismatching)
	}
	if len(extra) > 0 {
		msg += fmt.Sprintf(`Extra environment values: %v. `, extra)
	}

	return errors.New(strings.TrimSpace(msg))
}

var skipEnvs = map[string]struct{}{
	"MINIO_OPTS":                   {},
	"MINIO_CERT_PASSWD":            {},
	"MINIO_SERVER_DEBUG":           {},
	"MINIO_DSYNC_TRACE":            {},
	"MINIO_ROOT_USER":              {},
	"MINIO_ROOT_PASSWORD":          {},
	"MINIO_ACCESS_KEY":             {},
	"MINIO_SECRET_KEY":             {},
	"MINIO_OPERATOR_VERSION":       {},
	"MINIO_VSPHERE_PLUGIN_VERSION": {},
	"MINIO_CI_CD":                  {},
}

func getServerSystemCfg() *ServerSystemConfig {
	envs := env.List("MINIO_")
	envValues := make(map[string]string, len(envs))
	for _, envK := range envs {
		// skip certain environment variables as part
		// of the whitelist and could be configured
		// differently on each nodes, update skipEnvs()
		// map if there are such environment values
		if _, ok := skipEnvs[envK]; ok {
			continue
		}
		envValues[envK] = logger.HashString(env.Get(envK, ""))
	}
	scfg := &ServerSystemConfig{NEndpoints: globalEndpoints.NEndpoints(), MinioEnv: envValues, Checksum: binaryChecksum}
	var cmdLines []string
	for _, ep := range globalEndpoints {
		cmdLines = append(cmdLines, ep.CmdLine)
	}
	scfg.CmdLines = cmdLines
	return scfg
}

func (s *bootstrapRESTServer) VerifyHandler(params *grid.MSS) (*ServerSystemConfig, *grid.RemoteErr) {
	return getServerSystemCfg(), nil
}

var serverVerifyHandler = grid.NewSingleHandler[*grid.MSS, *ServerSystemConfig](grid.HandlerServerVerify, grid.NewMSS, func() *ServerSystemConfig { return &ServerSystemConfig{} })

// registerBootstrapRESTHandlers - register bootstrap rest router.
func registerBootstrapRESTHandlers(gm *grid.Manager) {
	server := &bootstrapRESTServer{}
	logger.FatalIf(serverVerifyHandler.Register(gm, server.VerifyHandler), "unable to register handler")
}

// client to talk to bootstrap NEndpoints.
type bootstrapRESTClient struct {
	gridConn *grid.Connection
}

// Verify function verifies the server config.
func (client *bootstrapRESTClient) Verify(ctx context.Context, srcCfg *ServerSystemConfig) (err error) {
	if newObjectLayerFn() != nil {
		return nil
	}

	recvCfg, err := serverVerifyHandler.Call(ctx, client.gridConn, grid.NewMSS())
	if err != nil {
		return err
	}
	// We do not need the response after returning.
	defer serverVerifyHandler.PutResponse(recvCfg)

	return srcCfg.Diff(recvCfg)
}

// Stringer provides a canonicalized representation of node.
func (client *bootstrapRESTClient) String() string {
	return client.gridConn.String()
}

var binaryChecksum = getBinaryChecksum()

func getBinaryChecksum() string {
	mw := md5.New()
	binPath, err := os.Executable()
	if err != nil {
		logger.Error("Calculating checksum failed: %s", err)
		return "00000000000000000000000000000000"
	}
	b, err := os.Open(binPath)
	if err != nil {
		logger.Error("Calculating checksum failed: %s", err)
		return "00000000000000000000000000000000"
	}

	defer b.Close()
	io.Copy(mw, b)
	return hex.EncodeToString(mw.Sum(nil))
}

func verifyServerSystemConfig(ctx context.Context, endpointServerPools EndpointServerPools, gm *grid.Manager) error {
	srcCfg := getServerSystemCfg()
	clnts := newBootstrapRESTClients(endpointServerPools, gm)
	var onlineServers int
	var offlineEndpoints []error
	var incorrectConfigs []error
	var retries int
	var mu sync.Mutex
	for onlineServers < len(clnts)/2 {
		var wg sync.WaitGroup
		wg.Add(len(clnts))
		onlineServers = 0
		for _, clnt := range clnts {
			go func(clnt *bootstrapRESTClient) {
				defer wg.Done()

				if clnt.gridConn.State() != grid.StateConnected {
					mu.Lock()
					offlineEndpoints = append(offlineEndpoints, fmt.Errorf("%s is unreachable: %w", clnt, grid.ErrDisconnected))
					mu.Unlock()
					return
				}

				ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()

				err := clnt.Verify(ctx, srcCfg)
				mu.Lock()
				if err != nil {
					bootstrapTraceMsg(fmt.Sprintf("bootstrapVerify: %v, endpoint: %s", err, clnt))
					if !isNetworkError(err) {
						bootLogOnceIf(context.Background(), fmt.Errorf("%s has incorrect configuration: %w", clnt, err), "incorrect_"+clnt.String())
						incorrectConfigs = append(incorrectConfigs, fmt.Errorf("%s has incorrect configuration: %w", clnt, err))
					} else {
						offlineEndpoints = append(offlineEndpoints, fmt.Errorf("%s is unreachable: %w", clnt, err))
					}
				} else {
					onlineServers++
				}
				mu.Unlock()
			}(clnt)
		}
		wg.Wait()

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Sleep and stagger to avoid blocked CPU and thundering
			// herd upon start up sequence.
			time.Sleep(25*time.Millisecond + time.Duration(rand.Int63n(int64(100*time.Millisecond))))
			retries++
			// after 20 retries start logging that servers are not reachable yet
			if retries >= 20 {
				logger.Info(fmt.Sprintf("Waiting for at least %d remote servers with valid configuration to be online", len(clnts)/2))
				if len(offlineEndpoints) > 0 {
					logger.Info(fmt.Sprintf("Following servers are currently offline or unreachable %s", offlineEndpoints))
				}
				if len(incorrectConfigs) > 0 {
					logger.Info(fmt.Sprintf("Following servers have mismatching configuration %s", incorrectConfigs))
				}
				retries = 0 // reset to log again after 20 retries.
			}
			offlineEndpoints = nil
			incorrectConfigs = nil
		}
	}
	return nil
}

func newBootstrapRESTClients(endpointServerPools EndpointServerPools, gm *grid.Manager) []*bootstrapRESTClient {
	seenClient := set.NewStringSet()
	var clnts []*bootstrapRESTClient
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				continue
			}
			if seenClient.Contains(endpoint.Host) {
				continue
			}
			seenClient.Add(endpoint.Host)
			clnts = append(clnts, &bootstrapRESTClient{gm.Connection(endpoint.GridHost())})
		}
	}
	return clnts
}
