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

package madmin

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/logger/message/log"
)

// LogInfo holds console log messages
type LogInfo struct {
	log.Entry
	ConsoleMsg string
	NodeName   string `json:"node"`
	Err        error  `json:"-"`
}

// SendLog returns true if log pertains to node specified in args.
func (l LogInfo) SendLog(node, logKind string) bool {
	nodeFltr := (node == "" || strings.EqualFold(node, l.NodeName))
	typeFltr := strings.EqualFold(logKind, "all") || strings.EqualFold(l.LogKind, logKind)
	return nodeFltr && typeFltr
}

// GetLogs - listen on console log messages.
func (adm AdminClient) GetLogs(node string, lineCnt int, logKind string, doneCh <-chan struct{}) <-chan LogInfo {
	logCh := make(chan LogInfo, 1)

	// Only success, start a routine to start reading line by line.
	go func(logCh chan<- LogInfo) {
		defer close(logCh)
		urlValues := make(url.Values)
		urlValues.Set("node", node)
		urlValues.Set("limit", strconv.Itoa(lineCnt))
		urlValues.Set("logType", logKind)
		for {
			reqData := requestData{
				relPath:     adminAPIPrefix + "/log",
				queryValues: urlValues,
			}
			// Execute GET to call log handler
			resp, err := adm.executeMethod("GET", reqData)
			if err != nil {
				closeResponse(resp)
				return
			}

			if resp.StatusCode != http.StatusOK {
				logCh <- LogInfo{Err: httpRespToErrorResponse(resp)}
				return
			}
			dec := json.NewDecoder(resp.Body)
			for {
				var info LogInfo
				if err = dec.Decode(&info); err != nil {
					break
				}
				select {
				case <-doneCh:
					return
				case logCh <- info:
				}
			}

		}
	}(logCh)

	// Returns the log info channel, for caller to start reading from.
	return logCh
}
