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

	trace "github.com/minio/minio/pkg/trace"
)

// TraceInfo holds http trace
type TraceInfo struct {
	Trace trace.Info
	Err   error `json:"-"`
}

// Trace - listen on http trace notifications.
func (adm AdminClient) Trace(allTrace, errTrace bool, doneCh <-chan struct{}) <-chan TraceInfo {
	traceInfoCh := make(chan TraceInfo)
	// Only success, start a routine to start reading line by line.
	go func(traceInfoCh chan<- TraceInfo) {
		defer close(traceInfoCh)
		for {
			urlValues := make(url.Values)
			urlValues.Set("all", strconv.FormatBool(allTrace))
			urlValues.Set("err", strconv.FormatBool(errTrace))
			reqData := requestData{
				relPath:     "/v1/trace",
				queryValues: urlValues,
			}
			// Execute GET to call trace handler
			resp, err := adm.executeMethod("GET", reqData)
			if err != nil {
				closeResponse(resp)
				return
			}

			if resp.StatusCode != http.StatusOK {
				traceInfoCh <- TraceInfo{Err: httpRespToErrorResponse(resp)}
				return
			}

			dec := json.NewDecoder(resp.Body)
			for {
				var info trace.Info
				if err = dec.Decode(&info); err != nil {
					break
				}
				select {
				case <-doneCh:
					return
				case traceInfoCh <- TraceInfo{Trace: info}:
				}
			}
		}
	}(traceInfoCh)

	// Returns the trace info channel, for caller to start reading from.
	return traceInfoCh
}
