// Copyright (c) 2015-2021 MinIO, Inc.
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
