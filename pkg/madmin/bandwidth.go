/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio/pkg/bandwidth"
)

// Report includes the bandwidth report or the error encountered.
type Report struct {
	Report bandwidth.Report
	Err    error
}

// GetBucketBandwidth - Gets a channel reporting bandwidth measurements for replication buckets. If no buckets
// generate replication traffic an empty map is returned in the report until traffic is seen.
func (adm *AdminClient) GetBucketBandwidth(ctx context.Context, buckets ...string) <-chan Report {
	queryValues := url.Values{}
	ch := make(chan Report)
	if len(buckets) > 0 {
		queryValues.Set("buckets", strings.Join(buckets, ","))
	}

	reqData := requestData{
		relPath:     adminAPIPrefix + "/bandwidth",
		queryValues: queryValues,
	}
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)
	if err != nil {
		defer closeResponse(resp)
		ch <- Report{bandwidth.Report{}, err}
		return ch
	}
	if resp.StatusCode != http.StatusOK {
		ch <- Report{bandwidth.Report{}, httpRespToErrorResponse(resp)}
		return ch
	}

	dec := json.NewDecoder(resp.Body)

	go func(ctx context.Context, ch chan<- Report, resp *http.Response) {
		defer func() {
			closeResponse(resp)
			close(ch)
		}()
		for {
			var report bandwidth.Report
			err = dec.Decode(&report)
			if err != nil {
				ch <- Report{bandwidth.Report{}, err}
				return
			}
			select {
			case <-ctx.Done():
				return
			case ch <- Report{Report: report, Err: err}:
			}
		}
	}(ctx, ch, resp)
	return ch
}
