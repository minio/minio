/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/minio/cli"
)

// SystemLockState - Structure to fill the lock state of entire object storage.
// That is the total locks held, total calls blocked on locks and state of all the locks for the entire system.
type SystemLockState struct {
	TotalLocks         int64            `json:"totalLocks"`
	TotalBlockedLocks  int64            `json:"totalBlockedLocks"`  // count of operations which are blocked waiting for the lock to be released.
	TotalAcquiredLocks int64            `json:"totalAcquiredLocks"` // count of operations which has successfully acquired the lock but hasn't unlocked yet( operation in progress).
	LocksInfoPerObject []VolumeLockInfo `json:"locksInfoPerObject"`
}

// VolumeLockInfo - Structure to contain the lock state info for volume, path pair.
type VolumeLockInfo struct {
	Bucket                string         `json:"bucket"`
	Object                string         `json:"object"`
	LocksOnObject         int64          `json:"locksOnObject"`         // All locks blocked + running for given <volume,path> pair.
	LocksAcquiredOnObject int64          `json:"locksAcquiredOnObject"` // count of operations which has successfully acquired the lock but hasn't unlocked yet( operation in progress).
	TotalBlockedLocks     int64          `json:"locksBlockedOnObject"`  // count of operations which are blocked waiting for the lock to be released.
	LockDetailsOnObject   []OpsLockState `json:"lockDetailsOnObject"`   // state information containing state of the locks for all operations on given <volume,path> pair.
}

// OpsLockState - structure to fill in state information of the lock.
// structure to fill in status information for each operation with given operation ID.
type OpsLockState struct {
	OperationID string `json:"opsID"`      // string containing operation ID.
	LockOrigin  string `json:"lockOrigin"` // contant which mentions the operation type (Get Obejct, PutObject...)
	LockType    string `json:"lockType"`
	Status      string `json:"status"`      // status can be running/ready/blocked.
	StatusSince string `json:"statusSince"` // time info of the since how long the status holds true, value in seconds.
}

// Read entire state of the locks in the system and return.
func generateSystemLockResponse() (SystemLockState, error) {
	nsMutex.lockMapMutex.Lock()
	defer nsMutex.lockMapMutex.Unlock()

	if nsMutex.debugLockMap == nil {
		return SystemLockState{}, LockInfoNil{}
	}

	lockState := SystemLockState{}

	lockState.TotalBlockedLocks = nsMutex.blockedCounter
	lockState.TotalLocks = nsMutex.globalLockCounter
	lockState.TotalAcquiredLocks = nsMutex.runningLockCounter

	for param := range nsMutex.debugLockMap {
		volLockInfo := VolumeLockInfo{}
		volLockInfo.Bucket = param.volume
		volLockInfo.Object = param.path
		volLockInfo.TotalBlockedLocks = nsMutex.debugLockMap[param].blocked
		volLockInfo.LocksAcquiredOnObject = nsMutex.debugLockMap[param].running
		volLockInfo.LocksOnObject = nsMutex.debugLockMap[param].ref
		for opsID := range nsMutex.debugLockMap[param].lockInfo {
			opsState := OpsLockState{}
			opsState.OperationID = opsID
			opsState.LockOrigin = nsMutex.debugLockMap[param].lockInfo[opsID].lockOrigin
			opsState.LockType = nsMutex.debugLockMap[param].lockInfo[opsID].lockType
			opsState.Status = nsMutex.debugLockMap[param].lockInfo[opsID].status
			opsState.StatusSince = time.Now().UTC().Sub(nsMutex.debugLockMap[param].lockInfo[opsID].since).String()

			volLockInfo.LockDetailsOnObject = append(volLockInfo.LockDetailsOnObject, opsState)
		}
		lockState.LocksInfoPerObject = append(lockState.LocksInfoPerObject, volLockInfo)
	}

	return lockState, nil
}

var lockCmd = cli.Command{
	Name:   "lock",
	Usage:  "info about the locks in the node.",
	Action: lockControl,
	Flags:  globalFlags,
	CustomHelpTemplate: `NAME:
  minio control {{.Name}} - {{.Usage}}

USAGE:
  minio control {{.Name}} http://localhost:9000/

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}

EAMPLES:
  1. Get all the info about the blocked/held locks in the node:
    $ minio control lock http://localhost:9000/
`,
}

// "minio control lock" entry point.
func lockControl(c *cli.Context) {
	if len(c.Args()) != 1 {
		cli.ShowCommandHelpAndExit(c, "lock", 1)
	}

	parsedURL, err := url.Parse(c.Args()[0])
	fatalIf(err, "Unable to parse URL.")

	authCfg := &authConfig{
		accessKey:   serverConfig.GetCredential().AccessKeyID,
		secretKey:   serverConfig.GetCredential().SecretAccessKey,
		address:     parsedURL.Host,
		path:        path.Join(reservedBucket, controlPath),
		loginMethod: "Controller.LoginHandler",
	}
	client := newAuthClient(authCfg)

	args := &GenericArgs{}
	reply := &SystemLockState{}
	err = client.Call("Controller.LockInfo", args, reply)
	// logs the error and returns if err != nil.
	fatalIf(err, "RPC Controller.LockInfo call failed")
	// print the lock info on the console.
	b, err := json.MarshalIndent(*reply, "", "  ")
	fatalIf(err, "Failed to parse the RPC lock info response")
	fmt.Print(string(b))
}
