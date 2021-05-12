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

	// GetBucketPolicyStatusAction - Retrieves the policy status for a bucket.
	GetBucketPolicyStatusAction = "s3:GetBucketPolicyStatus"

	// ListBucketVersionsAction - ListBucketVersions Rest API action.
	ListBucketVersionsAction = "s3:ListBucketVersions"

	// ListBucketMultipartUploadsAction - ListMultipartUploads Rest API action.
	ListBucketMultipartUploadsAction = "s3:ListBucketMultipartUploads"

	// ListenNotificationAction - ListenNotification Rest API action.
	// This is MinIO extension.
	ListenNotificationAction = "s3:ListenNotification"

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
	GetBucketPolicyStatusAction:            {},
	ListBucketVersionsAction:               {},
	ListBucketMultipartUploadsAction:       {},
	ListenNotificationAction:               {},
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
	GetReplicationConfigurationAction:      {},
	PutReplicationConfigurationAction:      {},
	ReplicateObjectAction:                  {},
	ReplicateDeleteAction:                  {},
	ReplicateTagsAction:                    {},
	GetObjectVersionForReplicationAction:   {},
	AllActions:                             {},
}

// List of all supported object actions.
var supportedObjectActions = map[Action]struct{}{
	AllActions:                           {},
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
}

// isObjectAction - returns whether action is object type or not.
func (action Action) isObjectAction() bool {
	for supAction := range supportedObjectActions {
		if action.Match(supAction) {
			return true
		}
	}
	return false
}

// Match - matches action name with action patter.
func (action Action) Match(a Action) bool {
	return wildcard.Match(string(action), string(a))
}

// IsValid - checks if action is valid or not.
func (action Action) IsValid() bool {
	for supAction := range supportedActions {
		if action.Match(supAction) {
			return true
		}
	}
	return false
}

type actionConditionKeyMap map[Action]condition.KeySet

func (a actionConditionKeyMap) Lookup(action Action) condition.KeySet {
	commonKeys := []condition.Key{}
	for _, keyName := range condition.CommonKeys {
		commonKeys = append(commonKeys, keyName.ToKey())
	}

	var ckeysMerged = condition.NewKeySet(commonKeys...)
	for act, ckey := range a {
		if action.Match(act) {
			ckeysMerged.Merge(ckey)
		}
	}
	return ckeysMerged
}

// iamActionConditionKeyMap - holds mapping of supported condition key for an action.
var iamActionConditionKeyMap = createActionConditionKeyMap()

func createActionConditionKeyMap() actionConditionKeyMap {
	commonKeys := []condition.Key{}
	for _, keyName := range condition.CommonKeys {
		commonKeys = append(commonKeys, keyName.ToKey())
	}

	allSupportedKeys := []condition.Key{}
	for _, keyName := range condition.AllSupportedKeys {
		allSupportedKeys = append(allSupportedKeys, keyName.ToKey())
	}

	return actionConditionKeyMap{
		AllActions: condition.NewKeySet(allSupportedKeys...),

		GetObjectAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3XAmzServerSideEncryption.ToKey(),
				condition.S3XAmzServerSideEncryptionCustomerAlgorithm.ToKey(),
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),

		ListBucketAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3Prefix.ToKey(),
				condition.S3Delimiter.ToKey(),
				condition.S3MaxKeys.ToKey(),
			}, commonKeys...)...),

		ListBucketVersionsAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3Prefix.ToKey(),
				condition.S3Delimiter.ToKey(),
				condition.S3MaxKeys.ToKey(),
			}, commonKeys...)...),

		DeleteObjectAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),

		PutObjectAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3XAmzCopySource.ToKey(),
				condition.S3XAmzServerSideEncryption.ToKey(),
				condition.S3XAmzServerSideEncryptionCustomerAlgorithm.ToKey(),
				condition.S3XAmzMetadataDirective.ToKey(),
				condition.S3XAmzStorageClass.ToKey(),
				condition.S3VersionID.ToKey(),
				condition.S3ObjectLockRetainUntilDate.ToKey(),
				condition.S3ObjectLockMode.ToKey(),
				condition.S3ObjectLockLegalHold.ToKey(),
			}, commonKeys...)...),

		// https://docs.aws.amazon.com/AmazonS3/latest/dev/list_amazons3.html
		// LockLegalHold is not supported with PutObjectRetentionAction
		PutObjectRetentionAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3XAmzServerSideEncryption.ToKey(),
				condition.S3XAmzServerSideEncryptionCustomerAlgorithm.ToKey(),
				condition.S3ObjectLockRemainingRetentionDays.ToKey(),
				condition.S3ObjectLockRetainUntilDate.ToKey(),
				condition.S3ObjectLockMode.ToKey(),
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),

		GetObjectRetentionAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3XAmzServerSideEncryption.ToKey(),
				condition.S3XAmzServerSideEncryptionCustomerAlgorithm.ToKey(),
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),

		PutObjectLegalHoldAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3XAmzServerSideEncryption.ToKey(),
				condition.S3XAmzServerSideEncryptionCustomerAlgorithm.ToKey(),
				condition.S3ObjectLockLegalHold.ToKey(),
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		GetObjectLegalHoldAction: condition.NewKeySet(commonKeys...),

		// https://docs.aws.amazon.com/AmazonS3/latest/dev/list_amazons3.html
		BypassGovernanceRetentionAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
				condition.S3ObjectLockRemainingRetentionDays.ToKey(),
				condition.S3ObjectLockRetainUntilDate.ToKey(),
				condition.S3ObjectLockMode.ToKey(),
				condition.S3ObjectLockLegalHold.ToKey(),
			}, commonKeys...)...),

		PutObjectTaggingAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		GetObjectTaggingAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		DeleteObjectTaggingAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),

		PutObjectVersionTaggingAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		GetObjectVersionAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		GetObjectVersionTaggingAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		DeleteObjectVersionAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		DeleteObjectVersionTaggingAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		ReplicateObjectAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		ReplicateDeleteAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		ReplicateTagsAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
		GetObjectVersionForReplicationAction: condition.NewKeySet(
			append([]condition.Key{
				condition.S3VersionID.ToKey(),
			}, commonKeys...)...),
	}
}
