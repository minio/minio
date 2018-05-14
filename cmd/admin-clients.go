/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/minio/minio/cmd/logger"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/quorum"
)

var errOperationTimedOut = errors.New("operation timed out")

type AdminClient interface {
	String() string

	SendSignal(signal serviceSignal) error

	ReloadFormat(dryRun bool) error

	ListLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error)

	GetServerInfo() (ServerInfoData, error)

	GetConfig() (serverConfig, error)

	SaveStageConfig(stageFilename string, config serverConfig) error

	CommitConfig(stageFilename string) error
}

// getValidServerConfig - finds the server config that is present in
// quorum or more number of servers.
func getValidServerConfig(serverConfigs []*serverConfig, quorum int) (*serverConfig, error) {
	// Count the number of disks a config.json was found in.
	configCounter := make([]int, len(serverConfigs))

	// We group equal serverConfigs by the lowest index of the
	// same value;  e.g, let us take the following serverConfigs
	// in a 4-node setup,
	// serverConfigs == [c1, c2, c1, c1]
	// configCounter == [3, 1, 0, 0]
	// c1, c2 are the only distinct values that appear.  c1 is
	// identified by 0, the lowest index it appears in and c2 is
	// identified by 1. So, we need to find the number of times
	// each of these distinct values occur.

	// Invariants:

	// 1. At the beginning of the i-th iteration, the number of
	// unique configurations seen so far is equal to the number of
	// non-zero counter values in config[:i].

	// 2. At the beginning of the i-th iteration, the sum of
	// elements of configCounter[:i] is equal to the number of
	// non-error configurations seen so far.

	// For each of the serverConfig ...
	for i := range serverConfigs {
		if serverConfigs[i] == nil {
			continue
		}

		// Check if it is equal to any of the configurations
		// seen so far. If j == i is reached then we have an
		// unseen configuration.
		for j := 0; j <= i; j++ {
			if j < i && configCounter[j] == 0 {
				// serverConfigs[j] is known to be
				// equal to a value that was already
				// seen. See example above for
				// clarity.
				continue
			} else if j < i && serverConfigs[i].ConfigDiff(serverConfigs[j]) == "" {
				// serverConfigs[i] is equal to
				// serverConfigs[j], update
				// serverConfigs[j]'s counter since it
				// is the lower index.
				configCounter[j]++
				break
			} else if j == i {
				// serverConfigs[i] is equal to no
				// other value seen before. It is
				// unique so far.
				configCounter[i] = 1
				break
			} // else invariants specified above are violated.
		}
	}

	// We find the maximally occurring server config and check if
	// there is quorum.
	var configJSON *serverConfig
	maxOccurrence := 0
	for i, count := range configCounter {
		if maxOccurrence < count {
			maxOccurrence = count
			configJSON = serverConfigs[i]
		}
	}

	// If quorum nodes don't agree.
	if maxOccurrence < quorum {
		return nil, fmt.Errorf("insufficient similar configuration; found: %v, expected minimum: %v", maxOccurrence, quorum)
	}

	return configJSON, nil
}

type AdminClients struct {
	LocalClient    *localAdminClient
	RPCClients     []*AdminRPCClient
	ReadQuorum     int
	WriteQuorum    int
	MaxExecTime    time.Duration
	MaxSuccessWait time.Duration
}

func (clients *AdminClients) readQuorumCall(functions []quorum.Func) error {
	return callFunctions(functions, clients.ReadQuorum, clients.MaxExecTime, clients.MaxSuccessWait, true)
}

func (clients *AdminClients) writeQuorumCall(functions []quorum.Func, quorum int) error {
	return callFunctions(functions, quorum, clients.MaxExecTime, clients.MaxSuccessWait, false)
}

func (clients *AdminClients) SendSignal(signal serviceSignal) error {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.RPCClients[i].SendSignal(signal); err != nil {
					errch <- quorum.Error{Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i))
	}

	if err := clients.writeQuorumCall(functions, clients.WriteQuorum-1); err != nil {
		return err
	}

	return clients.LocalClient.SendSignal(signal)
}

func (clients *AdminClients) ReloadFormat(dryRun bool) error {
	quorumFunc := func(i int) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				if err := clients.RPCClients[i].ReloadFormat(dryRun); err != nil {
					errch <- quorum.Error{Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i))
	}

	return clients.writeQuorumCall(functions, clients.WriteQuorum-1)
}

func (clients *AdminClients) ListLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error) {
	infos := make([][]VolumeLockInfo, len(clients.RPCClients)+1)

	quorumFunc := func(i int, client AdminClient) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				var err error
				if infos[i], err = client.ListLocks(bucket, prefix, duration); err != nil {
					errch <- quorum.Error{ID: client.String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i, clients.RPCClients[i]))
	}
	functions = append(functions, quorumFunc(len(functions), clients.LocalClient))

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	// Group lock information across nodes by (bucket, object)
	// pair. For readability only.
	paramLockMap := make(map[nsParam][]VolumeLockInfo)
	for _, nodeLocks := range infos {
		if nodeLocks == nil {
			continue
		}

		for _, lockInfo := range nodeLocks {
			param := nsParam{
				volume: lockInfo.Bucket,
				path:   lockInfo.Object,
			}
			paramLockMap[param] = append(paramLockMap[param], lockInfo)
		}
	}

	groupedLockInfos := []VolumeLockInfo{}
	for _, volLocks := range paramLockMap {
		groupedLockInfos = append(groupedLockInfos, volLocks...)
	}

	return groupedLockInfos, nil
}

func (clients *AdminClients) GetServerInfo() []ServerInfo {
	replies := make([]*ServerInfoData, len(clients.RPCClients)+1)

	quorumFunc := func(i int, client AdminClient) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				info, err := client.GetServerInfo()
				if err != nil {
					errch <- quorum.Error{ID: client.String(), Err: err}
				} else {
					replies[i] = &info
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i, clients.RPCClients[i]))
	}
	functions = append(functions, quorumFunc(len(functions), clients.LocalClient))

	// As we need to send whatever received, ignoring return code.
	_, errs := quorum.Call(functions, len(functions), clients.MaxExecTime, clients.MaxSuccessWait)

	infos := []ServerInfo{}
	for _, reply := range replies {
		if reply != nil {
			infos = append(infos, ServerInfo{Data: reply})
		}
	}

	for _, err := range errs {
		infos = append(infos, ServerInfo{Error: err.Error()})
	}

	err := errReadQuorum
	if len(errs) > 0 {
		qerr := errs[len(errs)-1]
		if _, ok := qerr.(*quorum.Error); !ok {
			err = qerr
		}
	}

	pending := len(functions) - len(infos)
	for i := 0; i < pending; i++ {
		infos = append(infos, ServerInfo{Error: err.Error()})
	}

	return infos
}

func (clients *AdminClients) GetLatestUptime() (time.Duration, error) {
	infos := make([]ServerInfoData, len(clients.RPCClients)+1)

	quorumFunc := func(i int, client AdminClient) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)
				var err error
				if infos[i], err = client.GetServerInfo(); err != nil {
					errch <- quorum.Error{ID: client.String(), Err: err}
				}
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i, clients.RPCClients[i]))
	}
	functions = append(functions, quorumFunc(len(functions), clients.LocalClient))

	if err := clients.readQuorumCall(functions); err != nil {
		return time.Duration(0), err
	}

	latestUptime := time.Duration(math.MaxInt64)
	for _, info := range infos {
		if info.Properties.Uptime != time.Duration(0) && info.Properties.Uptime < latestUptime {
			latestUptime = info.Properties.Uptime
		}
	}

	return latestUptime, nil
}

func (clients *AdminClients) GetConfig() (*serverConfig, error) {
	configs := make([]*serverConfig, len(clients.RPCClients)+1)

	quorumFunc := func(i int, client AdminClient) quorum.Func {
		return quorum.Func(func() <-chan quorum.Error {
			errch := make(chan quorum.Error)

			go func() {
				defer close(errch)

				config, err := client.GetConfig()
				if err != nil {
					errch <- quorum.Error{ID: client.String(), Err: err}
				}

				configs[i] = &config
			}()

			return errch
		})
	}

	functions := []quorum.Func{}
	for i := range clients.RPCClients {
		functions = append(functions, quorumFunc(i, clients.RPCClients[i]))
	}
	functions = append(functions, quorumFunc(len(functions), clients.LocalClient))

	if err := clients.readQuorumCall(functions); err != nil {
		return nil, err
	}

	return getValidServerConfig(configs, clients.WriteQuorum)
}

func (clients *AdminClients) SetConfig(config *serverConfig) ([]*quorum.Error, error) {
	stageFilename := minioConfigFile + "." + mustGetUUID()

	saveStageConfig := func() ([]*quorum.Error, error) {
		errs := make([]*quorum.Error, len(clients.RPCClients)+1)

		quorumFunc := func(i int, client AdminClient) quorum.Func {
			return quorum.Func(func() <-chan quorum.Error {
				errch := make(chan quorum.Error)

				go func() {
					defer close(errch)
					qerr := quorum.Error{ID: client.String()}
					qerr.Err = client.SaveStageConfig(stageFilename, *config)
					errs[i] = &qerr
					if qerr.Err != nil {
						errch <- qerr
					}
				}()

				return errch
			})
		}

		functions := []quorum.Func{}
		for i := range clients.RPCClients {
			functions = append(functions, quorumFunc(i, clients.RPCClients[i]))
		}
		functions = append(functions, quorumFunc(len(functions), clients.LocalClient))

		return errs, clients.writeQuorumCall(functions, clients.WriteQuorum)
	}

	commitConfig := func() ([]*quorum.Error, error) {
		errs := make([]*quorum.Error, len(clients.RPCClients)+1)

		quorumFunc := func(i int, client AdminClient) quorum.Func {
			return quorum.Func(func() <-chan quorum.Error {
				errch := make(chan quorum.Error)

				go func() {
					defer close(errch)
					qerr := quorum.Error{ID: client.String()}
					qerr.Err = client.CommitConfig(stageFilename)
					errs[i] = &qerr
					if qerr.Err != nil {
						errch <- qerr
					}
				}()

				return errch
			})
		}

		functions := []quorum.Func{}
		for i := range clients.RPCClients {
			functions = append(functions, quorumFunc(i, clients.RPCClients[i]))
		}
		functions = append(functions, quorumFunc(len(functions), clients.LocalClient))

		return errs, clients.writeQuorumCall(functions, clients.WriteQuorum)
	}

	errs, err := saveStageConfig()
	if err != nil {
		for i := range errs {
			if errs[i].ID == "" {
				errs[i].Err = err
				if i == len(clients.RPCClients) {
					errs[i].ID = clients.LocalClient.String()
				} else {
					errs[i].ID = clients.RPCClients[i].String()
				}
			}
		}

		return errs, nil
	}

	configLock := globalNSMutex.NewNSLock(minioReservedBucket, minioConfigFile)
	if configLock.GetLock(globalObjectTimeout) != nil {
		return nil, errOperationTimedOut
	}
	defer configLock.Unlock()

	errs, err = commitConfig()
	if err != nil {
		for i := range errs {
			if errs[i].ID == "" {
				errs[i].Err = err
				if i == len(clients.RPCClients) {
					errs[i].ID = clients.LocalClient.String()
				} else {
					errs[i].ID = clients.RPCClients[i].String()
				}
			}
		}
	}

	return errs, nil
}

func NewAdminClients(endpoints EndpointList) *AdminClients {
	rpcClients := []*AdminRPCClient{}
	for _, hostStr := range GetRemotePeers(endpoints) {
		host, err := xnet.ParseHost(hostStr)
		logger.CriticalIf(context.Background(), err)
		rpcClient, err := NewAdminRPCClient(host)
		logger.CriticalIf(context.Background(), err)
		rpcClients = append(rpcClients, rpcClient)
	}

	return &AdminClients{
		LocalClient:    &localAdminClient{},
		RPCClients:     rpcClients,
		ReadQuorum:     len(endpoints) / 2,
		WriteQuorum:    1 + len(endpoints)/2,
		MaxExecTime:    30 * time.Second,
		MaxSuccessWait: 1 * time.Second,
	}
}
