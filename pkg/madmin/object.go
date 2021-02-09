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
)

// GetObjectDebugInfo - Gets the debug info for an object.
func (adm *AdminClient) GetObjectDebugInfo(ctx context.Context, bucket, object string) (map[string]string, error) {
	queryValues := url.Values{}
	queryValues.Set("bucket", bucket)
	queryValues.Set("object", object)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/object",
		queryValues: queryValues,
	}
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	var debugInfo map[string]string
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&debugInfo)
	if err != nil {
		return nil, err
	}
	return debugInfo, nil
}
