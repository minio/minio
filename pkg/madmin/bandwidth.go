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
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio/pkg/bandwidth"
)

// GetBucketBandwidth - Get a snapshot of the bandwidth measurements for replication buckets. If no buckets
// generate replication traffic an empty map is returned.
func (adm *AdminClient) GetBucketBandwidth(ctx context.Context, buckets ...string) (bandwidth.Report, error) {
	queryValues := url.Values{}
	if len(buckets) > 0 {
		queryValues.Set("buckets", strings.Join(buckets, ","))
	}

	reqData := requestData{
		relPath:     adminAPIPrefix + "/bandwidth",
		queryValues: queryValues,
	}

	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)
	if err != nil {
		closeResponse(resp)
		return bandwidth.Report{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return bandwidth.Report{}, httpRespToErrorResponse(resp)
	}
	dec := json.NewDecoder(resp.Body)
	for {
		var report bandwidth.Report
		err = dec.Decode(&report)
		if err != nil && err != io.EOF {
			return bandwidth.Report{}, err
		}
		return report, nil
	}
}
