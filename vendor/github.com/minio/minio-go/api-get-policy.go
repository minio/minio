/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015, 2016 Minio, Inc.
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

package minio

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/minio/minio-go/pkg/policy"
)

// GetBucketPolicy - get bucket policy at a given path.
func (c Client) GetBucketPolicy(bucketName, objectPrefix string) (bucketPolicy policy.BucketPolicy, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return policy.BucketPolicyNone, err
	}
	if err := isValidObjectPrefix(objectPrefix); err != nil {
		return policy.BucketPolicyNone, err
	}
	policyInfo, err := c.getBucketPolicy(bucketName, objectPrefix)
	if err != nil {
		return policy.BucketPolicyNone, err
	}
	return policy.GetPolicy(policyInfo.Statements, bucketName, objectPrefix), nil
}

// ListBucketPolicies - list all policies for a given prefix and all its children.
func (c Client) ListBucketPolicies(bucketName, objectPrefix string) (bucketPolicies map[string]policy.BucketPolicy, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return map[string]policy.BucketPolicy{}, err
	}
	if err := isValidObjectPrefix(objectPrefix); err != nil {
		return map[string]policy.BucketPolicy{}, err
	}
	policyInfo, err := c.getBucketPolicy(bucketName, objectPrefix)
	if err != nil {
		return map[string]policy.BucketPolicy{}, err
	}
	return policy.GetPolicies(policyInfo.Statements, bucketName), nil
}

// Request server for current bucket policy.
func (c Client) getBucketPolicy(bucketName string, objectPrefix string) (policy.BucketAccessPolicy, error) {
	// Get resources properly escaped and lined up before
	// using them in http request.
	urlValues := make(url.Values)
	urlValues.Set("policy", "")

	// Execute GET on bucket to list objects.
	resp, err := c.executeMethod("GET", requestMetadata{
		bucketName:  bucketName,
		queryValues: urlValues,
	})

	defer closeResponse(resp)
	if err != nil {
		return policy.BucketAccessPolicy{}, err
	}

	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			errResponse := httpRespToErrorResponse(resp, bucketName, "")
			if ToErrorResponse(errResponse).Code == "NoSuchBucketPolicy" {
				return policy.BucketAccessPolicy{Version: "2012-10-17"}, nil
			}
			return policy.BucketAccessPolicy{}, errResponse
		}
	}
	bucketPolicyBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return policy.BucketAccessPolicy{}, err
	}

	policy := policy.BucketAccessPolicy{}
	err = json.Unmarshal(bucketPolicyBuf, &policy)
	return policy, err
}
