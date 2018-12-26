/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package policy

import (
	"encoding/json"
	"fmt"

	"github.com/minio/minio/pkg/policy/condition"
)

// Action - policy action.
// Refer https://docs.aws.amazon.com/IAM/latest/UserGuide/list_amazons3.html
// for more information about available actions.
type Action string

const (
	// AbortMultipartUploadAction - AbortMultipartUpload Rest API action.
	AbortMultipartUploadAction Action = "s3:AbortMultipartUpload"

	// CreateBucketAction - CreateBucket Rest API action.
	CreateBucketAction = "s3:CreateBucket"

	// DeleteBucketAction - DeleteBucket Rest API action.
	DeleteBucketAction = "s3:DeleteBucket"

	// DeleteBucketPolicyAction - DeleteBucketPolicy Rest API action.
	DeleteBucketPolicyAction = "s3:DeleteBucketPolicy"

	// DeleteObjectAction - DeleteObject Rest API action.
	DeleteObjectAction = "s3:DeleteObject"

	// GetBucketLocationAction - GetBucketLocation Rest API action.
	GetBucketLocationAction = "s3:GetBucketLocation"

	// GetBucketNotificationAction - GetBucketNotification Rest API action.
	GetBucketNotificationAction = "s3:GetBucketNotification"

	// GetBucketPolicyAction - GetBucketPolicy Rest API action.
	GetBucketPolicyAction = "s3:GetBucketPolicy"

	// GetObjectAction - GetObject Rest API action.
	GetObjectAction = "s3:GetObject"

	// HeadBucketAction - HeadBucket Rest API action. This action is unused in minio.
	HeadBucketAction = "s3:HeadBucket"

	// ListAllMyBucketsAction - ListAllMyBuckets (List buckets) Rest API action.
	ListAllMyBucketsAction = "s3:ListAllMyBuckets"

	// ListBucketAction - ListBucket Rest API action.
	ListBucketAction = "s3:ListBucket"

	// ListBucketMultipartUploadsAction - ListMultipartUploads Rest API action.
	ListBucketMultipartUploadsAction = "s3:ListBucketMultipartUploads"

	// ListenBucketNotificationAction - ListenBucketNotification Rest API action.
	// This is Minio extension.
	ListenBucketNotificationAction = "s3:ListenBucketNotification"

	// ListMultipartUploadPartsAction - ListParts Rest API action.
	ListMultipartUploadPartsAction = "s3:ListMultipartUploadParts"

	// PutBucketNotificationAction - PutObjectNotification Rest API action.
	PutBucketNotificationAction = "s3:PutBucketNotification"

	// PutBucketPolicyAction - PutBucketPolicy Rest API action.
	PutBucketPolicyAction = "s3:PutBucketPolicy"

	// PutObjectAction - PutObject Rest API action.
	PutObjectAction = "s3:PutObject"
)

// isObjectAction - returns whether action is object type or not.
func (action Action) isObjectAction() bool {
	switch action {
	case AbortMultipartUploadAction, DeleteObjectAction, GetObjectAction:
		fallthrough
	case ListMultipartUploadPartsAction, PutObjectAction:
		return true
	}

	return false
}

// IsValid - checks if action is valid or not.
func (action Action) IsValid() bool {
	switch action {
	case AbortMultipartUploadAction, CreateBucketAction, DeleteBucketAction:
		fallthrough
	case DeleteBucketPolicyAction, DeleteObjectAction, GetBucketLocationAction:
		fallthrough
	case GetBucketNotificationAction, GetBucketPolicyAction, GetObjectAction:
		fallthrough
	case HeadBucketAction, ListAllMyBucketsAction, ListBucketAction:
		fallthrough
	case ListBucketMultipartUploadsAction, ListenBucketNotificationAction:
		fallthrough
	case ListMultipartUploadPartsAction, PutBucketNotificationAction:
		fallthrough
	case PutBucketPolicyAction, PutObjectAction:
		return true
	}

	return false
}

// MarshalJSON - encodes Action to JSON data.
func (action Action) MarshalJSON() ([]byte, error) {
	if action.IsValid() {
		return json.Marshal(string(action))
	}

	return nil, fmt.Errorf("invalid action '%v'", action)
}

// UnmarshalJSON - decodes JSON data to Action.
func (action *Action) UnmarshalJSON(data []byte) error {
	var s string

	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	a := Action(s)
	if !a.IsValid() {
		return fmt.Errorf("invalid action '%v'", s)
	}

	*action = a

	return nil
}

func parseAction(s string) (Action, error) {
	action := Action(s)

	if action.IsValid() {
		return action, nil
	}

	return action, fmt.Errorf("unsupported action '%v'", s)
}

// actionConditionKeyMap - holds mapping of supported condition key for an action.
var actionConditionKeyMap = map[Action]condition.KeySet{
	AbortMultipartUploadAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	CreateBucketAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	DeleteBucketPolicyAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	DeleteObjectAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	GetBucketLocationAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	GetBucketNotificationAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	GetBucketPolicyAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	GetObjectAction: condition.NewKeySet(
		condition.S3XAmzServerSideEncryption,
		condition.S3XAmzServerSideEncryptionAwsKMSKeyID,
		condition.S3XAmzStorageClass,
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	HeadBucketAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	ListAllMyBucketsAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	ListBucketAction: condition.NewKeySet(
		condition.S3Prefix,
		condition.S3Delimiter,
		condition.S3MaxKeys,
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	ListBucketMultipartUploadsAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	ListenBucketNotificationAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	ListMultipartUploadPartsAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	PutBucketNotificationAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	PutBucketPolicyAction: condition.NewKeySet(
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),

	PutObjectAction: condition.NewKeySet(
		condition.S3XAmzCopySource,
		condition.S3XAmzServerSideEncryption,
		condition.S3XAmzServerSideEncryptionAwsKMSKeyID,
		condition.S3XAmzMetadataDirective,
		condition.S3XAmzStorageClass,
		condition.AWSReferer,
		condition.AWSSourceIP,
		condition.AWSUserAgent,
		condition.AWSSecureTransport,
	),
}
