/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package drivers

// This file implements compatability layer for AWS clients

// Resource delimiter
const (
	AwsResource = "arn:aws:s3:::"
)

// TODO support canonical user
// Principal delimiter
const (
	AwsPrincipal = "arn:aws:iam::"
)

// Action map
var SupportedActionMapCompat = map[string]bool{
	"*":                     true,
	"s3:GetObject":          true,
	"s3:ListBucket":         true,
	"s3:PutObject":          true,
	"s3:CreateBucket":       true,
	"s3:GetBucketPolicy":    true,
	"s3:DeleteBucketPolicy": true,
	"s3:ListAllMyBuckets":   true,
	"s3:PutBucketPolicy":    true,
}

func isValidActionS3(action []string) bool {
	for _, a := range action {
		if !SupportedActionMapCompat[a] {
			return false
		}
	}
	return true
}
