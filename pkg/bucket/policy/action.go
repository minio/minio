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

package policy

import (
	"encoding/json"

	"github.com/minio/pkg/bucket/policy/condition"
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

	// GetBucketPolicyStatusAction - Retrieves the policy status for a bucket.
	GetBucketPolicyStatusAction = "s3:GetBucketPolicyStatus"

	// ListBucketMultipartUploadsAction - ListMultipartUploads Rest API action.
	ListBucketMultipartUploadsAction = "s3:ListBucketMultipartUploads"

	// ListBucketVersionsAction - ListBucket versions Rest API action.
	ListBucketVersionsAction = "s3:ListBucketVersions"

	// ListenNotificationAction - ListenNotification Rest API action.
	// This is MinIO extension.
	ListenNotificationAction = "s3:ListenNotification"

	// ListenBucketNotificationAction - ListenBucketNotification Rest API action.
	// This is MinIO extension.
	ListenBucketNotificationAction = "s3:ListenBucketNotification"

	// ListMultipartUploadPartsAction - ListParts Rest API action.
	ListMultipartUploadPartsAction = "s3:ListMultipartUploadParts"

	// PutBucketNotificationAction - PutObjectNotification Rest API action.
	PutBucketNotificationAction = "s3:PutBucketNotification"

	// PutBucketPolicyAction - PutBucketPolicy Rest API action.
	PutBucketPolicyAction = "s3:PutBucketPolicy"

	// PutObjectAction - PutObject Rest API action.
	PutObjectAction = "s3:PutObject"

	// PutBucketLifecycleAction - PutBucketLifecycle Rest API action.
	PutBucketLifecycleAction = "s3:PutLifecycleConfiguration"

	// GetBucketLifecycleAction - GetBucketLifecycle Rest API action.
	GetBucketLifecycleAction = "s3:GetLifecycleConfiguration"

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
	// GetBucketObjectLockConfigurationAction - GetObjectLockConfiguration Rest API action
	GetBucketObjectLockConfigurationAction = "s3:GetBucketObjectLockConfiguration"
	// PutBucketObjectLockConfigurationAction - PutObjectLockConfiguration Rest API action
	PutBucketObjectLockConfigurationAction = "s3:PutBucketObjectLockConfiguration"

	// GetBucketTaggingAction - GetTagging Rest API action
	GetBucketTaggingAction = "s3:GetBucketTagging"
	// PutBucketTaggingAction - PutTagging Rest API action
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

	// GetReplicationConfigurationAction  - GetReplicationConfiguration REST API action
	GetReplicationConfigurationAction = "s3:GetReplicationConfiguration"
	// PutReplicationConfigurationAction  - PutReplicationConfiguration REST API action
	PutReplicationConfigurationAction = "s3:PutReplicationConfiguration"

	// ReplicateObjectAction  - ReplicateObject REST API action
	ReplicateObjectAction = "s3:ReplicateObject"

	// ReplicateDeleteAction  - ReplicateDelete REST API action
	ReplicateDeleteAction = "s3:ReplicateDelete"

	// ReplicateTagsAction  - ReplicateTags REST API action
	ReplicateTagsAction = "s3:ReplicateTags"

	// GetObjectVersionForReplicationAction  - GetObjectVersionForReplication REST API action
	GetObjectVersionForReplicationAction = "s3:GetObjectVersionForReplication"

	// RestoreObjectAction - RestoreObject REST API action
	RestoreObjectAction = "s3:RestoreObject"
	// ResetBucketReplicationStateAction - MinIO extension API ResetBucketReplicationState to reset replication state
	// on a bucket
	ResetBucketReplicationStateAction = "s3:ResetBucketReplicationState"
)

// List of all supported object actions.
var supportedObjectActions = map[Action]struct{}{
	AbortMultipartUploadAction:           {},
	DeleteObjectAction:                   {},
	GetObjectAction:                      {},
	ListMultipartUploadPartsAction:       {},
	PutObjectAction:                      {},
	BypassGovernanceRetentionAction:      {},
	PutObjectRetentionAction:             {},
	GetObjectRetentionAction:             {},
	PutObjectLegalHoldAction:             {},
	GetObjectLegalHoldAction:             {},
	GetObjectTaggingAction:               {},
	PutObjectTaggingAction:               {},
	DeleteObjectTaggingAction:            {},
	GetObjectVersionAction:               {},
	GetObjectVersionTaggingAction:        {},
	DeleteObjectVersionAction:            {},
	DeleteObjectVersionTaggingAction:     {},
	PutObjectVersionTaggingAction:        {},
	ReplicateObjectAction:                {},
	ReplicateDeleteAction:                {},
	ReplicateTagsAction:                  {},
	GetObjectVersionForReplicationAction: {},
	RestoreObjectAction:                  {},
	ResetBucketReplicationStateAction:    {},
}

// isObjectAction - returns whether action is object type or not.
func (action Action) isObjectAction() bool {
	_, ok := supportedObjectActions[action]
	return ok
}

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
	GetBucketPolicyStatusAction:            {},
	ListBucketVersionsAction:               {},
	ListBucketMultipartUploadsAction:       {},
	ListenNotificationAction:               {},
	ListenBucketNotificationAction:         {},
	ListMultipartUploadPartsAction:         {},
	PutBucketNotificationAction:            {},
	PutBucketPolicyAction:                  {},
	PutObjectAction:                        {},
	GetBucketLifecycleAction:               {},
	PutBucketLifecycleAction:               {},
	PutObjectRetentionAction:               {},
	GetObjectRetentionAction:               {},
	GetObjectLegalHoldAction:               {},
	PutObjectLegalHoldAction:               {},
	PutBucketObjectLockConfigurationAction: {},
	GetBucketObjectLockConfigurationAction: {},
	PutBucketTaggingAction:                 {},
	GetBucketTaggingAction:                 {},
	GetObjectVersionAction:                 {},
	GetObjectVersionTaggingAction:          {},
	DeleteObjectVersionAction:              {},
	DeleteObjectVersionTaggingAction:       {},
	PutObjectVersionTaggingAction:          {},
	BypassGovernanceRetentionAction:        {},
	GetObjectTaggingAction:                 {},
	PutObjectTaggingAction:                 {},
	DeleteObjectTaggingAction:              {},
	PutBucketEncryptionAction:              {},
	GetBucketEncryptionAction:              {},
	PutBucketVersioningAction:              {},
	GetBucketVersioningAction:              {},
	GetReplicationConfigurationAction:      {},
	PutReplicationConfigurationAction:      {},
	ReplicateObjectAction:                  {},
	ReplicateDeleteAction:                  {},
	ReplicateTagsAction:                    {},
	GetObjectVersionForReplicationAction:   {},
	RestoreObjectAction:                    {},
	ResetBucketReplicationStateAction:      {},
}

// IsValid - checks if action is valid or not.
func (action Action) IsValid() bool {
	_, ok := supportedActions[action]
	return ok
}

// MarshalJSON - encodes Action to JSON data.
func (action Action) MarshalJSON() ([]byte, error) {
	if action.IsValid() {
		return json.Marshal(string(action))
	}

	return nil, Errorf("invalid action '%v'", action)
}

// UnmarshalJSON - decodes JSON data to Action.
func (action *Action) UnmarshalJSON(data []byte) error {
	var s string

	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	a := Action(s)
	if !a.IsValid() {
		return Errorf("invalid action '%v'", s)
	}

	*action = a

	return nil
}

func parseAction(s string) (Action, error) {
	action := Action(s)

	if action.IsValid() {
		return action, nil
	}

	return action, Errorf("unsupported action '%v'", s)
}

// actionConditionKeyMap - holds mapping of supported condition key for an action.
var actionConditionKeyMap = map[Action]condition.KeySet{
	AbortMultipartUploadAction: condition.NewKeySet(condition.CommonKeys...),

	CreateBucketAction: condition.NewKeySet(condition.CommonKeys...),

	DeleteObjectAction: condition.NewKeySet(condition.CommonKeys...),

	GetBucketLocationAction: condition.NewKeySet(condition.CommonKeys...),

	GetBucketPolicyStatusAction: condition.NewKeySet(condition.CommonKeys...),

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

	ListBucketVersionsAction: condition.NewKeySet(
		append([]condition.Key{
			condition.S3Prefix,
			condition.S3Delimiter,
			condition.S3MaxKeys,
		}, condition.CommonKeys...)...),

	ListBucketMultipartUploadsAction: condition.NewKeySet(condition.CommonKeys...),

	ListenNotificationAction: condition.NewKeySet(condition.CommonKeys...),

	ListenBucketNotificationAction: condition.NewKeySet(condition.CommonKeys...),

	ListMultipartUploadPartsAction: condition.NewKeySet(condition.CommonKeys...),

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
	GetReplicationConfigurationAction:    condition.NewKeySet(condition.CommonKeys...),
	PutReplicationConfigurationAction:    condition.NewKeySet(condition.CommonKeys...),
	ReplicateObjectAction:                condition.NewKeySet(condition.CommonKeys...),
	ReplicateDeleteAction:                condition.NewKeySet(condition.CommonKeys...),
	ReplicateTagsAction:                  condition.NewKeySet(condition.CommonKeys...),
	GetObjectVersionForReplicationAction: condition.NewKeySet(condition.CommonKeys...),
	RestoreObjectAction:                  condition.NewKeySet(condition.CommonKeys...),
	ResetBucketReplicationStateAction:    condition.NewKeySet(condition.CommonKeys...),
}
