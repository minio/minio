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
 *
 */

package madmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ClearConfigHistoryKV - clears the config entry represented by restoreID.
// optionally allows setting `all` as a special keyword to automatically
// erase all config set history entires.
func (adm *AdminClient) ClearConfigHistoryKV(ctx context.Context, restoreID string) (err error) {
	v := url.Values{}
	v.Set("restoreId", restoreID)
	reqData := requestData{
		relPath:     adminAPIPrefix + "/clear-config-history-kv",
		queryValues: v,
	}

	// Execute DELETE on /minio/admin/v3/clear-config-history-kv
	resp, err := adm.executeMethod(ctx, http.MethodDelete, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// RestoreConfigHistoryKV - Restore a previous config set history.
// Input is a unique id which represents the previous setting.
func (adm *AdminClient) RestoreConfigHistoryKV(ctx context.Context, restoreID string) (err error) {
	v := url.Values{}
	v.Set("restoreId", restoreID)
	reqData := requestData{
		relPath:     adminAPIPrefix + "/restore-config-history-kv",
		queryValues: v,
	}

	// Execute PUT on /minio/admin/v3/set-config-kv to set config key/value.
	resp, err := adm.executeMethod(ctx, http.MethodPut, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// ConfigHistoryEntry - captures config set history with a unique
// restore ID and createTime
type ConfigHistoryEntry struct {
	RestoreID  string    `json:"restoreId"`
	CreateTime time.Time `json:"createTime"`
	Data       string    `json:"data"`
}

// CreateTimeFormatted is used to print formatted time for CreateTime.
func (ch ConfigHistoryEntry) CreateTimeFormatted() string {
	return ch.CreateTime.Format(http.TimeFormat)
}

// ListConfigHistoryKV - lists a slice of ConfigHistoryEntries sorted by createTime.
func (adm *AdminClient) ListConfigHistoryKV(ctx context.Context, count int) ([]ConfigHistoryEntry, error) {
	if count == 0 {
		count = 10
	}
	v := url.Values{}
	v.Set("count", strconv.Itoa(count))

	// Execute GET on /minio/admin/v3/list-config-history-kv
	resp, err := adm.executeMethod(ctx,
		http.MethodGet,
		requestData{
			relPath:     adminAPIPrefix + "/list-config-history-kv",
			queryValues: v,
		})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	data, err := DecryptData(adm.getSecretKey(), resp.Body)
	if err != nil {
		return nil, err
	}

	var chEntries []ConfigHistoryEntry
	if err = json.Unmarshal(data, &chEntries); err != nil {
		return chEntries, err
	}

	return chEntries, nil
}
