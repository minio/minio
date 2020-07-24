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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/minio/minio/pkg/auth"
)

// BucketReplicationTarget represents the target bucket and site to be replicated to.
type BucketReplicationTarget struct {
	Endpoint     string            `json:"endpoint"`
	Credentials  *auth.Credentials `json:"credentials"`
	TargetBucket string            `json:"targetbucket"`
	IsSSL        bool              `json:"isssl"`
	Path         string            `json:"path,omitempty"`
	API          string            `json:"api,omitempty"`
	Arn          string            `json:"arn,omitempty"`
}

// URL returns replication target url
func (t BucketReplicationTarget) URL() string {
	scheme := "http"
	if t.IsSSL {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, t.Endpoint)
}

// Empty returns true if struct is empty.
func (t BucketReplicationTarget) Empty() bool {
	return t.String() == "" || t.Credentials == nil
}

func (t *BucketReplicationTarget) String() string {
	return fmt.Sprintf("%s %s", t.Endpoint, t.TargetBucket)
}

// GetBucketReplicationTarget - gets replication target for this bucket
func (adm *AdminClient) GetBucketReplicationTarget(ctx context.Context, bucket string) (target BucketReplicationTarget, err error) {
	queryValues := url.Values{}
	queryValues.Set("bucket", bucket)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/get-bucket-replication-target",
		queryValues: queryValues,
	}

	// Execute GET on /minio/admin/v3/get-bucket-replication-target
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)

	defer closeResponse(resp)
	if err != nil {
		return target, err
	}

	if resp.StatusCode != http.StatusOK {
		return target, httpRespToErrorResponse(resp)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return target, err
	}
	if err = json.Unmarshal(b, &target); err != nil {
		return target, err
	}
	if target.Empty() {
		return target, errors.New("No Replication target configured")
	}
	return target, nil
}

// SetBucketReplicationTarget sets up a replication target for this bucket
func (adm *AdminClient) SetBucketReplicationTarget(ctx context.Context, bucket string, target *BucketReplicationTarget) error {
	data, err := json.Marshal(target)
	if err != nil {
		return err
	}
	encData, err := EncryptData(adm.getSecretKey(), data)
	if err != nil {
		return err
	}
	queryValues := url.Values{}
	queryValues.Set("bucket", bucket)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/set-bucket-replication-target",
		queryValues: queryValues,
		content:     encData,
	}

	// Execute PUT on /minio/admin/v3/set-bucket-replication-target to set a replication target for this bucket.
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

// GetBucketReplicationARN - gets replication Arn for this remote
func (adm *AdminClient) GetBucketReplicationARN(ctx context.Context, rURL string) (arn string, err error) {
	queryValues := url.Values{}
	queryValues.Set("url", rURL)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/get-bucket-replication-arn",
		queryValues: queryValues,
	}

	// Execute GET on /minio/admin/v3/list-bucket-replication-arn
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)

	defer closeResponse(resp)
	if err != nil {
		return arn, err
	}

	if resp.StatusCode != http.StatusOK {
		return arn, httpRespToErrorResponse(resp)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return arn, err
	}
	if err = json.Unmarshal(b, &arn); err != nil {
		return arn, err
	}
	if arn == "" {
		return arn, fmt.Errorf("Missing Replication ARN")
	}
	return arn, nil
}
