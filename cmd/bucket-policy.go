/*
 * MinIO Cloud Storage, (C) 2018,2020 MinIO, Inc.
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
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	miniogopolicy "github.com/minio/minio-go/v6/pkg/policy"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/handlers"
)

// PolicySys - policy subsystem.
type PolicySys struct{}

// Get returns stored bucket policy
func (sys *PolicySys) Get(bucket string) (*policy.Policy, error) {
	return globalBucketMetadataSys.GetPolicyConfig(bucket)
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (sys *PolicySys) IsAllowed(args policy.Args) bool {
	p, err := sys.Get(args.BucketName)
	if err == nil {
		return p.IsAllowed(args)
	}

	// Log unhandled errors.
	if _, ok := err.(BucketPolicyNotFound); !ok {
		logger.LogIf(GlobalContext, err)
	}

	// As policy is not available for given bucket name, returns IsOwner i.e.
	// operation is allowed only for owner.
	return args.IsOwner
}

// NewPolicySys - creates new policy system.
func NewPolicySys() *PolicySys {
	return &PolicySys{}
}

func getConditionValues(r *http.Request, lc string, username string, claims map[string]interface{}) map[string][]string {
	currTime := UTCNow()

	principalType := "Anonymous"
	if username != "" {
		principalType = "User"
	}

	vid := r.URL.Query().Get("versionId")
	if vid == "" {
		if u, err := url.Parse(r.Header.Get(xhttp.AmzCopySource)); err == nil {
			vid = u.Query().Get("versionId")
		}
		if vid == "" {
			vid = r.Header.Get(xhttp.AmzCopySourceVersionID)
		}
	}

	args := map[string][]string{
		"CurrentTime":     {currTime.Format(time.RFC3339)},
		"EpochTime":       {strconv.FormatInt(currTime.Unix(), 10)},
		"SecureTransport": {strconv.FormatBool(r.TLS != nil)},
		"SourceIp":        {handlers.GetSourceIP(r)},
		"UserAgent":       {r.UserAgent()},
		"Referer":         {r.Referer()},
		"principaltype":   {principalType},
		"userid":          {username},
		"username":        {username},
		"versionid":       {vid},
	}

	if lc != "" {
		args["LocationConstraint"] = []string{lc}
	}

	cloneHeader := r.Header.Clone()

	for _, objLock := range []string{
		xhttp.AmzObjectLockMode,
		xhttp.AmzObjectLockLegalHold,
		xhttp.AmzObjectLockRetainUntilDate,
	} {
		if values, ok := cloneHeader[objLock]; ok {
			args[strings.TrimPrefix(objLock, "X-Amz-")] = values
		}
		cloneHeader.Del(objLock)
	}

	for key, values := range cloneHeader {
		if existingValues, found := args[key]; found {
			args[key] = append(existingValues, values...)
		} else {
			args[key] = values
		}
	}

	var cloneURLValues = url.Values{}
	for k, v := range r.URL.Query() {
		cloneURLValues[k] = v
	}

	for _, objLock := range []string{
		xhttp.AmzObjectLockMode,
		xhttp.AmzObjectLockLegalHold,
		xhttp.AmzObjectLockRetainUntilDate,
	} {
		if values, ok := cloneURLValues[objLock]; ok {
			args[strings.TrimPrefix(objLock, "X-Amz-")] = values
		}
		cloneURLValues.Del(objLock)
	}

	for key, values := range cloneURLValues {
		if existingValues, found := args[key]; found {
			args[key] = append(existingValues, values...)
		} else {
			args[key] = values
		}
	}

	// JWT specific values
	for k, v := range claims {
		vStr, ok := v.(string)
		if ok {
			args[k] = []string{vStr}
		}
	}

	return args
}

// PolicyToBucketAccessPolicy converts a MinIO policy into a minio-go policy data structure.
func PolicyToBucketAccessPolicy(bucketPolicy *policy.Policy) (*miniogopolicy.BucketAccessPolicy, error) {
	// Return empty BucketAccessPolicy for empty bucket policy.
	if bucketPolicy == nil {
		return &miniogopolicy.BucketAccessPolicy{Version: policy.DefaultVersion}, nil
	}

	data, err := json.Marshal(bucketPolicy)
	if err != nil {
		// This should not happen because bucketPolicy is valid to convert to JSON data.
		return nil, err
	}

	var policyInfo miniogopolicy.BucketAccessPolicy
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(data, &policyInfo); err != nil {
		// This should not happen because data is valid to JSON data.
		return nil, err
	}

	return &policyInfo, nil
}

// BucketAccessPolicyToPolicy - converts minio-go/policy.BucketAccessPolicy to policy.Policy.
func BucketAccessPolicyToPolicy(policyInfo *miniogopolicy.BucketAccessPolicy) (*policy.Policy, error) {
	data, err := json.Marshal(policyInfo)
	if err != nil {
		// This should not happen because policyInfo is valid to convert to JSON data.
		return nil, err
	}

	var bucketPolicy policy.Policy
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(data, &bucketPolicy); err != nil {
		// This should not happen because data is valid to JSON data.
		return nil, err
	}

	return &bucketPolicy, nil
}
