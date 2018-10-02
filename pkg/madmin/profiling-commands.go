/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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
 *
 */

package madmin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

// ProfilerType represents the profiler type
// passed to the profiler subsystem.
type ProfilerType string

// Different supported profiler types.
const (
	ProfilerCPU   ProfilerType = "cpu"   // represents CPU profiler type
	ProfilerMEM                = "mem"   // represents MEM profiler type
	ProfilerBlock              = "block" // represents Block profiler type
	ProfilerMutex              = "mutex" // represents Mutex profiler type
	ProfilerTrace              = "trace" // represents Trace profiler type
)

// StartProfilingResult holds the result of starting
// profiler result in a given node.
type StartProfilingResult struct {
	NodeName string `json:"nodeName"`
	Success  bool   `json:"success"`
	Error    string `json:"error"`
}

// StartProfiling makes an admin call to remotely start profiling on a standalone
// server or the whole cluster in  case of a distributed setup.
func (adm *AdminClient) StartProfiling(profiler ProfilerType) ([]StartProfilingResult, error) {
	v := url.Values{}
	v.Set("profilerType", string(profiler))
	resp, err := adm.executeMethod("POST", requestData{
		relPath:     "/v1/profiling/start",
		queryValues: v,
	})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	jsonResult, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var startResults []StartProfilingResult
	err = json.Unmarshal(jsonResult, &startResults)
	if err != nil {
		return nil, err
	}

	return startResults, nil
}

// DownloadProfilingData makes an admin call to download profiling data of a standalone
// server or of the whole cluster in  case of a distributed setup.
func (adm *AdminClient) DownloadProfilingData() (io.ReadCloser, error) {
	path := fmt.Sprintf("/v1/profiling/download")
	resp, err := adm.executeMethod("GET", requestData{
		relPath: path,
	})

	if err != nil {
		closeResponse(resp)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	if resp.Body == nil {
		return nil, errors.New("body is nil")
	}

	return resp.Body, nil
}
