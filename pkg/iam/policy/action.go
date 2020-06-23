/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package iampolicy

import (
	"github.com/minio/minio/pkg/bucket/policy/condition"
	"github.com/minio/minio/pkg/wildcard"
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

	// ForceDeleteBucketAction - DeleteBucket Rest API action when x-minio-force-delete flag
	// is specified.
	ForceDeleteBucketAction = "s3:ForceDeleteBucket"

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
	// This is MinIO extension.
	ListenBucketNotificationAction = "s3:ListenBucketNotification"

	// ListMultipartUploadPartsAction - ListParts Rest API action.
	ListMultipartUploadPartsAction = "s3:ListMultipartUploadParts"

	// PutBucketLifecycleAction - PutBucketLifecycle Rest API action.
	PutBucketLifecycleAction = "s3:PutLifecycleConfiguration"

	// GetBucketLifecycleAction - GetBucketLifecycle Rest API action.
	GetBucketLifecycleAction = "s3:GetLifecycleConfiguration"

	// PutBucketNotificationAction - PutObjectNotification Rest API action.
	PutBucketNotificationAction = "s3:PutBucketNotification"

	// PutBucketPolicyAction - PutBucketPolicy Rest API action.
	PutBucketPolicyAction = "s3:PutBucketPolicy"

	// PutObjectAction - PutObject Rest API action.
	PutObjectAction = "s3:PutObject"

	// DeleteObjectVersionAction - DeleteObjectVersion Rest API action.
	DeleteObjectVersionAction = "s3:DeleteObjectVersion"

	// DeleteObjectVersionTaggingAction - DeleteObjectVersionTagging Rest API action.
	DeleteObjectVersionTaggingAction = "s3:DeleteObjectVersionTagging"

	// GetObjectVersionAction - GetObjectVersionAction Rest API action.
	GetObjectVersionAction = "s3:GetObjectVersion"

	// GetObjectVersionTaggingAction - GetObjectVersionTagging Rest API action.
	GetObjectVersionTaggingAction = "s3:GetObjectVersionTagging"

	// PutObjectVersionTaggingAction - PutObjectVersionTagging Rest API action.
	PutObjectVersionTaggingAction = "s3:PutObjectVersionTagging"

	// BypassGovernanceRetentionAction - bypass governance retention for PutObjectRetention, PutObject and DeleteObject Rest API action.
	BypassGovernanceRetentionAction = "s3:BypassGovernanceRetention"

	// PutObjectRetentionAction - PutObjectRetention Rest API action.
	PutObjectRetentionAction = "s3:PutObjectRetention"

	// GetObjectRetentionAction - GetObjectRetention, GetObject, HeadObject Rest API action.
	GetObjectRetentionAction = "s3:GetObjectRetention"

	// GetObjectLegalHoldAction - GetObjectLegalHold, GetObject Rest API action.
	GetObjectLegalHoldAction = "s3:GetObjectLegalHold"

	// PutObjectLegalHoldAction - PutObjectLegalHold, PutObject Rest API action.
	PutObjectLegalHoldAction = "s3:PutObjectLegalHold"

	// GetBucketObjectLockConfigurationAction - GetBucketObjectLockConfiguration Rest API action
	GetBucketObjectLockConfigurationAction = "s3:GetBucketObjectLockConfiguration"

	// PutBucketObjectLockConfigurationAction - PutBucketObjectLockConfiguration Rest API action
	PutBucketObjectLockConfigurationAction = "s3:PutBucketObjectLockConfiguration"

	// GetBucketTaggingAction - GetBucketTagging Rest API action
	GetBucketTaggingAction = "s3:GetBucketTagging"

	// PutBucketTaggingAction - PutBucketTagging Rest API action
	PutBucketTaggingAction = "s3:PutBucketTagging"

	// GetObjectTaggingAction - Get Object Tags API action
	GetObjectTaggingAction = "s3:GetObjectTagging"

	// PutObjectTaggingAction - Put Object Tags API action
	PutObjectTaggingAction = "s3:PutObjectTagging"

	// DeleteObjectTaggingAction - Delete Object Tags API action
	DeleteObjectTaggingAction = "s3:DeleteObjectTagging"

	// PutBucketEncryptionAction - PutBucketEncryption REST API action
	PutBucketEncryptionAction = "s3:PutEncryptionConfiguration"

	// GetBucketEncryptionAction - GetBucketEncryption REST API action
	GetBucketEncryptionAction = "s3:GetEncryptionConfiguration"

	// PutBucketVersioningAction - PutBucketVersioning REST API action
	PutBucketVersioningAction = "s3:PutBucketVersioning"

	// GetBucketVersioningAction - GetBucketVersioning REST API action
	GetBucketVersioningAction = "s3:GetBucketVersioning"

	// AllActions - all API actions
	AllActions = "s3:*"
)

// List of all supported actions.
var supportedActions = map[Action]struct{}{
	AbortMultipartUploadAction:             {},
	CreateBucketAction:                     {},
	DeleteBucketAction:                     {},
	ForceDeleteBucketAction:                {},
	DeleteBucketPolicyAction:               {},
	DeleteObjectAction:                     {},
	GetBucketLocationAction:                {},
	GetBucketNotificationAction:            {},
	GetBucketPolicyAction:                  {},
	GetObjectAction:                        {},
	HeadBucketAction:                       {},
	ListAllMyBucketsAction:                 {},
	ListBucketAction:                       {},
	ListBucketMultipartUploadsAction:       {},
	ListenBucketNotificationAction:         {},
	ListMultipartUploadPartsAction:         {},
	PutBucketLifecycleAction:               {},
	GetBucketLifecycleAction:               {},
	PutBucketNotificationAction:            {},
	PutBucketPolicyAction:                  {},
	PutObjectAction:                        {},
	BypassGovernanceRetentionAction:        {},
	PutObjectRetentionAction:               {},
	GetObjectRetentionAction:               {},
	GetObjectLegalHoldAction:               {},
	PutObjectLegalHoldAction:               {},
	GetBucketObjectLockConfigurationAction: {},
	PutBucketObjectLockConfigurationAction: {},
	GetBucketTaggingAction:                 {},
	PutBucketTaggingAction:                 {},
	GetObjectVersionAction:                 {},
	GetObjectVersionTaggingAction:          {},
	DeleteObjectVersionAction:              {},
	DeleteObjectVersionTaggingAction:       {},
	PutObjectVersionTaggingAction:          {},
	GetObjectTaggingAction:                 {},
	PutObjectTaggingAction:                 {},
	DeleteObjectTaggingAction:              {},
	PutBucketEncryptionAction:              {},
	GetBucketEncryptionAction:              {},
	PutBucketVersioningAction:              {},
	GetBucketVersioningAction:              {},
	AllActions:                             {},
}

// List of all supported object actions.
var supportedObjectActions = map[Action]struct{}{
	AllActions:                       {},
	AbortMultipartUploadAction:       {},
	DeleteObjectAction:               {},
	GetObjectAction:                  {},
	ListMultipartUploadPartsAction:   {},
	PutObjectAction:                  {},
	BypassGovernanceRetentionAction:  {},
	PutObjectRetentionAction:         {},
	GetObjectRetentionAction:         {},
	PutObjectLegalHoldAction:         {},
	GetObjectLegalHoldAction:         {},
	GetObjectTaggingAction:           {},
	PutObjectTaggingAction:           {},
	DeleteObjectTaggingAction:        {},
	GetObjectVersionAction:           {},
	GetObjectVersionTaggingAction:    {},
	DeleteObjectVersionAction:        {},
	DeleteObjectVersionTaggingAction: {},
	PutObjectVersionTaggingAction:    {},
}

// isObjectAction - returns whether action is object type or not.
func (action Action) isObjectAction() bool {
	_, ok := supportedObjectActions[action]
	return ok
}

// Match - matches object name with resource pattern.
func (action Action) Match(a Action) bool {
	return wildcard.Match(string(action), string(a))
}

// IsValid - checks if action is valid or not.
func (action Action) IsValid() bool {
	_, ok := supportedActions[action]
	return ok
}

// actionConditionKeyMap - holds mapping of supported condition key for an action.
var actionConditionKeyMap = map[Action]condition.KeySet{
	AllActions: condition.NewKeySet(condition.AllSupportedKeys...),

	AbortMultipartUploadAction: condition.NewKeySet(condition.CommonKeys...),

	CreateBucketAction: condition.NewKeySet(condition.CommonKeys...),

	DeleteBucketPolicyAction: condition.NewKeySet(condition.CommonKeys...),

	DeleteObjectAction: condition.NewKeySet(condition.CommonKeys...),

	GetBucketLocationAction: condition.NewKeySet(condition.CommonKeys...),

	GetBucketNotificationAction: condition.NewKeySet(condition.CommonKeys...),

	GetBucketPolicyAction: condition.NewKeySet(condition.CommonKeys...),

	GetObjectAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3XAmzServerSideEncryption,
			condition.S3XAmzServerSideEncryptionCustomerAlgorithm,
		}, condition.CommonKeys...)...),

	HeadBucketAction: condition.NewKeySet(condition.CommonKeys...),

	ListAllMyBucketsAction: condition.NewKeySet(condition.CommonKeys...),

	ListBucketAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3Prefix,
			condition.S3Delimiter,
			condition.S3MaxKeys,
		}, condition.CommonKeys...)...),

	ListBucketMultipartUploadsAction: condition.NewKeySet(condition.CommonKeys...),

	ListenBucketNotificationAction: condition.NewKeySet(condition.CommonKeys...),

	ListMultipartUploadPartsAction: condition.NewKeySet(condition.CommonKeys...),

	PutBucketNotificationAction: condition.NewKeySet(condition.CommonKeys...),

	PutBucketPolicyAction: condition.NewKeySet(condition.CommonKeys...),

	PutObjectAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3XAmzCopySource,
			condition.S3XAmzServerSideEncryption,
			condition.S3XAmzServerSideEncryptionCustomerAlgorithm,
			condition.S3XAmzMetadataDirective,
			condition.S3XAmzStorageClass,
			condition.S3ObjectLockRetainUntilDate,
			condition.S3ObjectLockMode,
			condition.S3ObjectLockLegalHold,
		}, condition.CommonKeys...)...),

	// https://docs.aws.amazon.com/AmazonS3/latest/dev/list_amazons3.html
	// LockLegalHold is not supported with PutObjectRetentionAction
	PutObjectRetentionAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3ObjectLockRemainingRetentionDays,
			condition.S3ObjectLockRetainUntilDate,
			condition.S3ObjectLockMode,
		}, condition.CommonKeys...)...),

	GetObjectRetentionAction: condition.NewKeySet(condition.CommonKeys...),
	PutObjectLegalHoldAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3ObjectLockLegalHold,
		}, condition.CommonKeys...)...),
	GetObjectLegalHoldAction: condition.NewKeySet(condition.CommonKeys...),

	// https://docs.aws.amazon.com/AmazonS3/latest/dev/list_amazons3.html
	BypassGovernanceRetentionAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3ObjectLockRemainingRetentionDays,
			condition.S3ObjectLockRetainUntilDate,
			condition.S3ObjectLockMode,
			condition.S3ObjectLockLegalHold,
		}, condition.CommonKeys...)...),

	GetBucketObjectLockConfigurationAction: condition.NewKeySet(condition.CommonKeys...),
	PutBucketObjectLockConfigurationAction: condition.NewKeySet(condition.CommonKeys...),
	GetBucketTaggingAction:                 condition.NewKeySet(condition.CommonKeys...),
	PutBucketTaggingAction:                 condition.NewKeySet(condition.CommonKeys...),
	PutObjectTaggingAction:                 condition.NewKeySet(condition.CommonKeys...),
	GetObjectTaggingAction:                 condition.NewKeySet(condition.CommonKeys...),
	DeleteObjectTaggingAction:              condition.NewKeySet(condition.CommonKeys...),

	PutObjectVersionTaggingAction: condition.NewKeySet(condition.CommonKeys...),
	GetObjectVersionAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3VersionID,
		}, condition.CommonKeys...)...),
	GetObjectVersionTaggingAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3VersionID,
		}, condition.CommonKeys...)...),
	DeleteObjectVersionAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3VersionID,
		}, condition.CommonKeys...)...),
	DeleteObjectVersionTaggingAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3VersionID,
		}, condition.CommonKeys...)...),
}
