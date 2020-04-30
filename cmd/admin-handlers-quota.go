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
 */

package cmd

import (
	"io/ioutil"
	"net/http"
	"path"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

const (
	bucketQuotaConfigFile = "quota.json"
)

// PutBucketQuotaConfigHandler - PUT Bucket quota configuration.
// ----------
// Places a quota configuration on the specified bucket. The quota
// specified in the quota configuration will be applied by default
// to enforce total quota for the specified bucket.
func (a adminAPIHandlers) PutBucketQuotaConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketQuotaConfig")
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SetBucketQuotaAdminAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Turn off quota commands if data usage info is unavailable.
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOff {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminBucketQuotaDisabled), r.URL)
		return
	}

	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	quotaCfg, err := parseBucketQuota(data)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	configFile := path.Join(bucketConfigPrefix, bucket, bucketQuotaConfigFile)
	if err = saveConfig(ctx, objectAPI, configFile, data); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	if quotaCfg.Quota > 0 {
		globalBucketQuotaSys.Set(bucket, quotaCfg)
		globalNotificationSys.PutBucketQuotaConfig(ctx, bucket, quotaCfg)

	} else {
		globalBucketQuotaSys.Remove(bucket)
		globalNotificationSys.RemoveBucketQuotaConfig(ctx, bucket)

	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketQuotaConfigHandler - gets bucket quota configuration
func (a adminAPIHandlers) GetBucketQuotaConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketQuotaConfig")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.GetBucketQuotaAdminAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	configFile := path.Join(bucketConfigPrefix, bucket, bucketQuotaConfigFile)
	configData, err := readConfig(ctx, objectAPI, configFile)
	if err != nil {
		if err != errConfigNotFound {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, BucketQuotaConfigNotFound{Bucket: bucket}), r.URL)
		return
	}
	// Write success response.
	writeSuccessResponseJSON(w, configData)
}

// RemoveBucketQuotaConfigHandler - removes Bucket quota configuration.
// ----------
// Removes quota configuration on the specified bucket.
func (a adminAPIHandlers) RemoveBucketQuotaConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveBucketQuotaConfig")
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SetBucketQuotaAdminAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	configFile := path.Join(bucketConfigPrefix, bucket, bucketQuotaConfigFile)
	if err := deleteConfig(ctx, objectAPI, configFile); err != nil {
		if err != errConfigNotFound {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, BucketQuotaConfigNotFound{Bucket: bucket}), r.URL)
		return
	}
	globalBucketQuotaSys.Remove(bucket)
	globalNotificationSys.RemoveBucketQuotaConfig(ctx, bucket)
	// Write success response.
	writeSuccessNoContent(w)
}
