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
	"io/ioutil"
	"net/http"
	"net/url"
)

// QuotaType represents bucket quota type
type QuotaType string

const (
	// HardQuota specifies a hard quota of usage for bucket
	HardQuota QuotaType = "hard"
	// FIFOQuota specifies a quota limit beyond which older files are deleted from bucket
	FIFOQuota QuotaType = "fifo"
)

// IsValid returns true if quota type is one of FIFO or Hard
func (t QuotaType) IsValid() bool {
	return t == HardQuota || t == FIFOQuota
}

// BucketQuota holds bucket quota restrictions
type BucketQuota struct {
	Quota uint64    `json:"quota"`
	Type  QuotaType `json:"quotatype,omitempty"`
}

// IsValid returns false if quota is invalid
// empty quota when Quota == 0 is always true.
func (q BucketQuota) IsValid() bool {
	if q.Quota > 0 {
		return q.Type.IsValid()
	}
	// Empty configs are valid.
	return true
}

// GetBucketQuota - get info on a user
func (adm *AdminClient) GetBucketQuota(ctx context.Context, bucket string) (q BucketQuota, err error) {
	queryValues := url.Values{}
	queryValues.Set("bucket", bucket)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/get-bucket-quota",
		queryValues: queryValues,
	}

	// Execute GET on /minio/admin/v3/get-quota
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)

	defer closeResponse(resp)
	if err != nil {
		return q, err
	}

	if resp.StatusCode != http.StatusOK {
		return q, httpRespToErrorResponse(resp)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return q, err
	}
	if err = json.Unmarshal(b, &q); err != nil {
		return q, err
	}

	return q, nil
}

// SetBucketQuota - sets a bucket's quota, if quota is set to '0'
// quota is disabled.
func (adm *AdminClient) SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error {
	data, err := json.Marshal(quota)
	if err != nil {
		return err
	}

	queryValues := url.Values{}
	queryValues.Set("bucket", bucket)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/set-bucket-quota",
		queryValues: queryValues,
		content:     data,
	}

	// Execute PUT on /minio/admin/v3/set-bucket-quota to set quota for a bucket.
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
