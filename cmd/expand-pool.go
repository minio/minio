// Copyright (c) 2015-2024 MinIO, Inc.
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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
	"gopkg.in/yaml.v2"
)

const (
	// PoolExpandStatusWaitForFileChange - waiting for the file to change
	PoolExpandStatusWaitForFileChange = "PoolExpandStatusWaitForFileChange"
	// PoolExpandStatusSetEnvToRestart - set the minio_args env to restart
	PoolExpandStatusSetEnvToRestart = "PoolExpandStatusSetEnvToRestart"
	// PoolExpandStatusStartDecommission - start the decommission process
	PoolExpandStatusStartDecommission = "PoolExpandStatusStartDecommission"
	// PoolExpandStatusWaitForDecommissionComplete - waiting for the decommission to complete
	PoolExpandStatusWaitForDecommissionComplete = "PoolExpandStatusWaitForDecommissionComplete"
	// PoolExpandStatusWaitForRenameDataDir - waiting for the rename of the data dir to complete
	PoolExpandStatusWaitForRenameDataDir = "PoolExpandStatusWaitForRenameDataDir"
)

func (s *SelfPoolExpand) syncExpandPoolsStatusToPeer() (err error) {
	defer logExpandPoolError(err)
	status, errs := globalNotificationSys.SyncExpandPoolsStatus(s.BeforePools, s.AfterPools, s.Status)
	for _, err := range errs {
		if err.Err != nil {
			// some peer returned an error
			return err.Err
		}
	}
	for _, st := range status {
		if !st.Success {
			// some peer returned an error
			return fmt.Errorf("peer sync expand pools status %v got false", st.Status)
		}
	}
	return nil
}

func (s *SelfPoolExpand) renameDataDir() (err error) {
	defer logExpandPoolError(err)
	if !globalServerCtxt.ExpandPools {
		return
	}
	if len(globalEndpoints) != 2 {
		return
	}
	// freeze the services
	freezeServices()
	defer unfreezeServices()
	// delete the dir and rename the dir
	dirPath := map[string]renamePath{}
	for _, ep := range globalEndpoints {
		for _, ep := range ep.Endpoints {
			if ep.IsLocal {
				path := ep.Path[:len(ep.Path)-1]
				tempPath := ""
				if strings.HasSuffix(ep.Path, "1") {
					tempPath = ep.Path
				}
				dataPath := ""
				dataBackupPath := ""
				if strings.HasSuffix(ep.Path, "0") {
					dataPath = ep.Path
					dataBackupPath = ep.Path[:len(ep.Path)-1] + "2"
				}
				pat := dirPath[path]
				if dataPath != "" {
					pat.dataPath = dataPath
					pat.dataBackupPath = dataBackupPath
				}
				if tempPath != "" {
					pat.newDataPath = tempPath
				}
				dirPath[path] = pat
			}
		}
	}
	successCount := 0
	dataCount := 0
	for _, pathInfo := range dirPath {
		if pathInfo.newDataPath != "" {
			dataCount++
		}
		if pathInfo.dataPath != "" {
			err := os.Rename(pathInfo.dataPath, pathInfo.dataBackupPath)
			if serverDebugLog {
				log.Infof("[Expand pool]: rename old data dir %s to backup dir %s: error %v", pathInfo.dataPath, pathInfo.dataBackupPath, err)
			}
			if err != nil {
				continue
			}
		}
		renameNewDataSuccess := false
		if pathInfo.newDataPath != "" {
			rPath := pathInfo.newDataPath[:len(pathInfo.newDataPath)-1] + "0"
			err := os.Rename(pathInfo.newDataPath, rPath)
			if serverDebugLog {
				log.Infof("[Expand pool]: Rename new data dir %s to dir %s: error %v", pathInfo.newDataPath, rPath, err)
			}
			if err != nil {
				continue
			}
			renameNewDataSuccess = true
			successCount++
		}
		if pathInfo.dataBackupPath != "" && renameNewDataSuccess {
			err := os.RemoveAll(pathInfo.dataBackupPath)
			if serverDebugLog {
				log.Infof("[Expand pool]: Remove old data dir %v: error %v", pathInfo.dataBackupPath, err)
			}
		}
	}
	if successCount > dataCount/2 {
		// remove only if has more than half of the data dirs renamed
		for _, pathInfo := range dirPath {
			if pathInfo.newDataPath == "" && pathInfo.dataBackupPath != "" {
				err := os.RemoveAll(pathInfo.dataBackupPath)
				if serverDebugLog {
					log.Infof("[Expand pool]: Remove old data dir %v: error %v", pathInfo.dataBackupPath, err)
				}
			}
		}
	}
	return nil
}

func (s *SelfPoolExpand) saveNextStatus(dirHaveRenamed bool) (err error) {
	defer logExpandPoolError(err)
	s.Status = nextPoolExpandStatus(s.Status)
	return writePoolExpandStats(&SelfPoolExpand{
		Status:         s.Status,
		BeforePools:    s.BeforePools,
		AfterPools:     s.AfterPools,
		FileMD5:        s.FileMD5,
		DirHaveRenamed: dirHaveRenamed,
	})
}

func nextPoolExpandStatus(ps string) string {
	switch ps {
	case "":
		return PoolExpandStatusWaitForFileChange
	case PoolExpandStatusWaitForFileChange:
		return PoolExpandStatusSetEnvToRestart
	case PoolExpandStatusSetEnvToRestart:
		return PoolExpandStatusStartDecommission
	case PoolExpandStatusStartDecommission:
		return PoolExpandStatusWaitForDecommissionComplete
	case PoolExpandStatusWaitForDecommissionComplete:
		return PoolExpandStatusWaitForRenameDataDir
	case PoolExpandStatusWaitForRenameDataDir:
		return ""
	}
	return ""
}

func loadPoolExpandStats() (status SelfPoolExpand, err error) {
	defer logExpandPoolError(err)
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return status, fmt.Errorf("invalid object layer")
	}
	z, ok := objectAPI.(*erasureServerPools)
	if !ok {
		return status, fmt.Errorf("invalid object layer")
	}
	return z.LoadSelfPoolExpand(context.Background())
}

func getConfigFileInfo() (filemd5 string, pools []string, err error) {
	if !globalServerCtxt.ExpandPools {
		return "", nil, fmt.Errorf("pool expand is not enabled")
	}
	fileBody, err := os.ReadFile(globalServerCtxt.ConfigPath)
	if err != nil {
		return "", nil, err
	}
	m5 := md5.New()
	m5.Write(fileBody)
	cf := &config.ServerConfig{}
	dec := yaml.NewDecoder(bytes.NewReader(fileBody))
	dec.SetStrict(true)
	if err = dec.Decode(cf); err != nil {
		return "", nil, err
	}
	if len(cf.Pools) != 1 || !cf.EnableExpandPools {
		return "", nil, fmt.Errorf("invalid config file for expand pool")
	}
	return hex.EncodeToString(m5.Sum(nil)), cf.Pools[0], nil
}

func writePoolExpandStats(poolExpandStatus *SelfPoolExpand) (err error) {
	defer logExpandPoolError(err)
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return fmt.Errorf("invalid object layer")
	}
	z, ok := objectAPI.(*erasureServerPools)
	if !ok {
		return fmt.Errorf("invalid object layer")
	}
	return z.SaveSelfPoolExpand(context.Background(), *poolExpandStatus)
}

type renamePath struct {
	dataPath       string
	newDataPath    string
	dataBackupPath string
}

func initExpandPool(configFile string, ctxt *serverCtxt, pools [][]string) error {
	if serverDebugLog {
		log.Infof("[Expand pool]: Starting with: %v", ctxt.Layout)
	}
	if !ctxt.ExpandPools {
		return nil
	}
	stat, err := os.Stat(configFile)
	if err != nil {
		return err
	}
	ltime := stat.ModTime()
	ctxt.ExpandPoolHandler = func() {
		if !globalEndpoints.FirstLocal() {
			return
		}
		if serverDebugLog {
			log.Infof("[Expand pool]: Starting pool expansion")
		}
		go func() {
			ticker := time.NewTicker(time.Second * 10)
			defer ticker.Stop()
			for {
			loop:
				select {
				case <-ticker.C:
					stats, err := loadPoolExpandStats()
					if err != nil {
						continue
					}
					if serverDebugLog {
						log.Infof("[Expand pool]: Load expansion status: %v", stats)
					}
					if len(stats.AfterPools) == 0 || len(stats.BeforePools) == 0 {
						continue
					}
					switch stats.Status {
					case "":
						if stats.FileMD5 != "" || len(stats.AfterPools) != 0 || len(stats.BeforePools) != 0 {
							// wait for new config file change
							_ = writePoolExpandStats(&SelfPoolExpand{})
						}
						continue
					case PoolExpandStatusWaitForFileChange:
						err := stats.syncExpandPoolsStatusToPeer()
						if err != nil {
							continue
						}
						_ = stats.saveNextStatus(false)
					case PoolExpandStatusSetEnvToRestart:
						err := stats.syncExpandPoolsStatusToPeer()
						if err != nil {
							continue
						}
						err = stats.saveNextStatus(false)
						if err != nil {
							continue
						}
						setEnvToMinioArgs(fmt.Sprintf("%s %s",
							strings.Join(stats.BeforePools, ","),
							strings.Join(stats.AfterPools, ","),
						))
						errs := globalNotificationSys.SignalService(serviceRestart)
						for _, err := range errs {
							if err.Err != nil {
								log.Error("[Expand pool]: SignalService to restart error:", err.Err)
								goto loop
							}
						}
						// restart self
						globalServiceSignalCh <- serviceRestart
						return
					case PoolExpandStatusStartDecommission:
						objectAPI := newObjectLayerFn()
						if objectAPI == nil {
							continue
						}
						z, ok := objectAPI.(*erasureServerPools)
						if !ok {
							continue
						}
						err := z.Decommission(context.Background(), 0)
						if err != nil {
							log.Error("[Expand pool]: Decommission error:", err)
							continue
						}
						_ = stats.saveNextStatus(false)
					case PoolExpandStatusWaitForDecommissionComplete:
						objectAPI := newObjectLayerFn()
						if objectAPI == nil {
							continue
						}
						z, ok := objectAPI.(*erasureServerPools)
						if !ok {
							continue
						}
						dstatus, err := z.Status(GlobalContext, 0)
						if err != nil {
							log.Error("[Expand pool]: Decommission Status error:", err)
							continue
						}
						if dstatus.Decommission == nil || !dstatus.Decommission.Complete {
							continue
						}
						_ = stats.saveNextStatus(false)
						// rename dir right now
						fallthrough
					case PoolExpandStatusWaitForRenameDataDir:
						// load pre status
						// if not renamed, rename now
						preStats, err := loadPoolExpandStats()
						if err != nil {
							continue
						}
						// if already renamed, skip
						if preStats.DirHaveRenamed {
							continue
						}
						err = preStats.saveNextStatus(true)
						if err != nil {
							continue
						}
						errRemote := stats.syncExpandPoolsStatusToPeer()
						errLocal := preStats.renameDataDir()
						// if both success, restart minio cluster
						// or if one of them failed, manual processing is required
						if errRemote == nil && errLocal == nil {
							setEnvToMinioArgs("\"\"")
							errs := globalNotificationSys.SignalService(serviceRestart)
							for _, err := range errs {
								if err.Err != nil {
									if serverDebugLog {
										log.Error("[Expand pool]: SignalService to restart error:", err.Err)
									}
									goto loop
								}
							}
							// restart self
							select {
							case globalServiceSignalCh <- serviceRestart:
							}
							return
						}
						log.Errorf(`[Expand pool]: Manual processing is required here, error %v,%v
Step1: Stop every node.
Step2: Rename path/0 -> path/2
Step3: Rename path/1 -> path/0")
Step4: Remove path/2
Step5: Restart every node. `, errRemote, errLocal)
					}
				case <-GlobalContext.Done():
					return
				}
			}
		}()
		go func() {
			ticker := time.NewTicker(time.Second * 10)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					lstat, err := os.Stat(configFile)
					if err != nil {
						continue
					}
					if !lstat.ModTime().After(ltime) {
						continue
					}
					rd, err := Open(configFile)
					if err != nil {
						continue
					}
					cf := &config.ServerConfig{}
					dec := yaml.NewDecoder(rd)
					dec.SetStrict(true)
					if err = dec.Decode(cf); err != nil {
						_ = rd.Close()
						continue
					}
					_ = rd.Close()
					layout, err := buildDisksLayoutFromConfFile(true, cf.Pools, true)
					if err != nil {
						continue
					}
					if reflect.DeepEqual(layout, ctxt.Layout) || len(layout.pools) != 1 {
						continue
					}
					// found the config.yaml has been changed
					if !cf.EnableExpandPools || len(cf.Pools) != 1 {
						// we only support one pool expand now
						continue
					}
					// delay 5s to make sure the config.yaml has been loaded
					time.Sleep(time.Second * 5)
					fileMd5, _, err := getConfigFileInfo()
					if err != nil {
						continue
					}
					if len(ctxt.Layout.pools) != 1 {
						continue
					}
					nowStatus, err := loadPoolExpandStats()
					if err != nil {
						continue
					}
					// before the change, the status should be empty or PoolExpandStatusComplete
					if nowStatus.Status == "" && len(pools) == 1 && len(cf.Pools) == 1 && !reflect.DeepEqual(cf.Pools[0], pools[0]) {
						err = writePoolExpandStats(&SelfPoolExpand{
							Status:         nextPoolExpandStatus(""),
							BeforePools:    pools[0],
							AfterPools:     cf.Pools[0],
							FileMD5:        fileMd5,
							DirHaveRenamed: false,
						})
						if err == nil {
							// exit the watcher
							return
						}
					}
				case <-GlobalContext.Done():
					return
				}
			}
		}()
	}
	return nil
}

// SelfPoolExpandResponse - peer client api response
type SelfPoolExpandResponse struct {
	SelfPoolExpand
	Success bool `json:"success"`
}

func (s *peerRESTServer) SyncExpandPoolsStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	z, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	if z.IsRebalanceStarted() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminRebalanceAlreadyStarted), r.URL)
		return
	}

	resp := SelfPoolExpandResponse{}
	status := r.Form.Get("poolExpandStatus")
	_, pools, err := getConfigFileInfo()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	before := strings.Split(r.Form.Get("before"), ",")
	after := strings.Split(r.Form.Get("after"), ",")
	if len(before) == 0 || len(after) == 0 {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, fmt.Errorf("empty pool")), r.URL)
		return
	}
	switch status {
	case "":
	case PoolExpandStatusWaitForFileChange:
		if !reflect.DeepEqual(pools, after) {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, fmt.Errorf("after pools not match")), r.URL)
			return
		}
		resp.Success = true
	case PoolExpandStatusSetEnvToRestart:
		if !reflect.DeepEqual(after, pools) {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, fmt.Errorf("after not match")), r.URL)
			return
		}
		setEnvToMinioArgs(fmt.Sprintf("%s %s",
			strings.Join(before, ","),
			strings.Join(after, ","),
		))
		resp.Success = true
	case PoolExpandStatusWaitForRenameDataDir:
		_ = (&SelfPoolExpand{}).renameDataDir()
		setEnvToMinioArgs("\"\"")
		resp.Success = true
	}
	data, err := json.Marshal(resp)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, data)
}

// SyncExpandPoolsStatus - sync expand pool status on a remote peer dynamically.
func (client *peerRESTClient) SyncExpandPoolsStatus(before, after []string, poolExpandStatus string) (rsl SelfPoolExpandResponse, err error) {
	values := make(url.Values)
	values.Set("poolExpandStatus", poolExpandStatus)
	if len(before) != 0 {
		values.Set("before", strings.Join(before, ","))
	}
	if len(after) != 0 {
		values.Set("after", strings.Join(after, ","))
	}
	respBody, err := client.call(peerRESTMethodSyncExpandPoolStatus, values, nil, -1)
	if err != nil {
		return rsl, err
	}
	defer xhttp.DrainBody(respBody)
	body, err := io.ReadAll(respBody)
	if err != nil {
		return rsl, err
	} else if err = json.Unmarshal(body, &rsl); err != nil {
		return rsl, err
	} else if !rsl.Success {
		return rsl, fmt.Errorf("failed to sync expand pool status")
	}
	return rsl, err
}

// SyncExpandPoolsStatus sync expand pool status on a remote peer dynamically.
func (sys *NotificationSys) SyncExpandPoolsStatus(before, after []string, poolExpandStatus string) (rsl []SelfPoolExpandResponse, errs []NotificationPeerErr) {
	ng := WithNPeers(len(sys.peerClients))
	rsl = make([]SelfPoolExpandResponse, len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		idx := idx
		ng.Go(GlobalContext, func() error {
			rs, err := client.SyncExpandPoolsStatus(before, after, poolExpandStatus)
			rsl[idx] = rs
			return err
		}, idx, *client.host)
	}
	errs = ng.Wait()
	return rsl, errs
}

func logExpandPoolError(err error) {
	if err != nil && serverDebugLog {
		log.Error("[Expand pool]: Interval error:", err)
	}
}

func setEnvToMinioArgs(envstr string) {
	os.Setenv(config.EnvArgs, envstr)
	if serverDebugLog {
		log.Infof("[Expand pool]:Set env [%s=%s] to restart", config.EnvArgs, os.Getenv(config.EnvArgs))
	}
}
