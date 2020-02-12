/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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
	"context"
	"net/http"

	"github.com/minio/minio/cmd/logger"
	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
)

// enforceRetentionBypassForDelete enforces whether an existing object under governance can be deleted
// with governance bypass headers set in the request.
// Objects under site wide WORM can never be overwritten.
// For objects in "Governance" mode, overwrite is allowed if a) object retention date is past OR
// governance bypass headers are set and user has governance bypass permissions.
// Objects in "Compliance" mode can be overwritten only if retention date is past.
func enforceRetentionBypassForDelete(ctx context.Context, r *http.Request, bucket, object string, getObjectInfoFn GetObjectInfoFn, govBypassPerm APIErrorCode) (oi ObjectInfo, s3Err APIErrorCode) {
	if globalWORMEnabled {
		return oi, ErrObjectLocked
	}
	var err error
	var opts ObjectOptions
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return oi, toAPIErrorCode(ctx, err)
	}
	oi, err = getObjectInfoFn(ctx, bucket, object, opts)
	if err != nil {
		// ignore case where object no longer exists
		if toAPIError(ctx, err).Code == "NoSuchKey" {
			oi.UserDefined = map[string]string{}
			return oi, ErrNone
		}
		return oi, toAPIErrorCode(ctx, err)
	}
	ret := objectlock.GetObjectRetentionMeta(oi.UserDefined)
	lhold := objectlock.GetObjectLegalHoldMeta(oi.UserDefined)
	if lhold.Status == objectlock.ON {
		return oi, ErrObjectLocked
	}
	// Here bucket does not support object lock
	if ret.Mode == objectlock.Invalid {
		return oi, ErrNone
	}
	if ret.Mode != objectlock.Compliance && ret.Mode != objectlock.Governance {
		return oi, ErrUnknownWORMModeDirective
	}
	t, err := objectlock.UTCNowNTP()
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, ErrObjectLocked
	}
	if ret.RetainUntilDate.Before(t) {
		return oi, ErrNone
	}
	if objectlock.IsObjectLockGovernanceBypassSet(r.Header) && ret.Mode == objectlock.Governance && govBypassPerm == ErrNone {
		return oi, ErrNone
	}
	return oi, ErrObjectLocked
}

// enforceRetentionBypassForPut enforces whether an existing object under governance can be overwritten
// with governance bypass headers set in the request.
// Objects under site wide WORM cannot be overwritten.
// For objects in "Governance" mode, overwrite is allowed if a) object retention date is past OR
// governance bypass headers are set and user has governance bypass permissions.
// Objects in compliance mode can be overwritten only if retention date is being extended. No mode change is permitted.
func enforceRetentionBypassForPut(ctx context.Context, r *http.Request, bucket, object string, getObjectInfoFn GetObjectInfoFn, govBypassPerm APIErrorCode, objRetention *objectlock.ObjectRetention) (oi ObjectInfo, s3Err APIErrorCode) {
	if globalWORMEnabled {
		return oi, ErrObjectLocked
	}

	var err error
	var opts ObjectOptions
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return oi, toAPIErrorCode(ctx, err)
	}
	oi, err = getObjectInfoFn(ctx, bucket, object, opts)
	if err != nil {
		// ignore case where object no longer exists
		if toAPIError(ctx, err).Code == "NoSuchKey" {
			oi.UserDefined = map[string]string{}
			return oi, ErrNone
		}
		return oi, toAPIErrorCode(ctx, err)
	}

	ret := objectlock.GetObjectRetentionMeta(oi.UserDefined)
	// no retention metadata on object
	if ret.Mode == objectlock.Invalid {
		if _, isWORMBucket := globalBucketObjectLockConfig.Get(bucket); !isWORMBucket {
			return oi, ErrInvalidBucketObjectLockConfiguration
		}
		return oi, ErrNone
	}
	t, err := objectlock.UTCNowNTP()
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, ErrObjectLocked
	}

	if ret.Mode == objectlock.Compliance {
		// Compliance retention mode cannot be changed and retention period cannot be shortened as per
		// https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html#object-lock-retention-modes
		if objRetention.Mode != objectlock.Compliance || objRetention.RetainUntilDate.Before(ret.RetainUntilDate.Time) {
			return oi, ErrObjectLocked
		}
		if objRetention.RetainUntilDate.Before(t) {
			return oi, ErrInvalidRetentionDate
		}
		return oi, ErrNone
	}

	if ret.Mode == objectlock.Governance {
		if !objectlock.IsObjectLockGovernanceBypassSet(r.Header) {
			if objRetention.RetainUntilDate.Before(t) {
				return oi, ErrInvalidRetentionDate
			}
			if objRetention.RetainUntilDate.Before((ret.RetainUntilDate.Time)) {
				return oi, ErrObjectLocked
			}
			return oi, ErrNone
		}
		return oi, govBypassPerm
	}
	return oi, ErrNone
}

// checkPutObjectLockAllowed enforces object retention policy and legal hold policy
// for requests with WORM headers
// See https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-managing.html for the spec.
// For non-existing objects with object retention headers set, this method returns ErrNone if bucket has
// locking enabled and user has requisite permissions (s3:PutObjectRetention)
// If object exists on object store and site wide WORM enabled - this method
// returns an error. For objects in "Governance" mode, overwrite is allowed if the retention date has expired.
// For objects in "Compliance" mode, retention date cannot be shortened, and mode cannot be altered.
// For objects with legal hold header set, the s3:PutObjectLegalHold permission is expected to be set
// Both legal hold and retention can be applied independently on an object
func checkPutObjectLockAllowed(ctx context.Context, r *http.Request, bucket, object string, getObjectInfoFn GetObjectInfoFn, retentionPermErr, legalHoldPermErr APIErrorCode) (objectlock.Mode, objectlock.RetentionDate, objectlock.ObjectLegalHold, APIErrorCode) {
	var mode objectlock.Mode
	var retainDate objectlock.RetentionDate
	var legalHold objectlock.ObjectLegalHold

	retention, isWORMBucket := globalBucketObjectLockConfig.Get(bucket)

	retentionRequested := objectlock.IsObjectLockRetentionRequested(r.Header)
	legalHoldRequested := objectlock.IsObjectLockLegalHoldRequested(r.Header)

	var objExists bool
	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		return mode, retainDate, legalHold, toAPIErrorCode(ctx, err)
	}
	if objInfo, err := getObjectInfoFn(ctx, bucket, object, opts); err == nil {
		objExists = true
		r := objectlock.GetObjectRetentionMeta(objInfo.UserDefined)
		if globalWORMEnabled || r.Mode == objectlock.Compliance {
			return mode, retainDate, legalHold, ErrObjectLocked
		}
		mode = r.Mode
		retainDate = r.RetainUntilDate
		legalHold = objectlock.GetObjectLegalHoldMeta(objInfo.UserDefined)
		// Disallow overwriting an object on legal hold
		if legalHold.Status == "ON" {
			return mode, retainDate, legalHold, ErrObjectLocked
		}
	}
	if legalHoldRequested {
		if !isWORMBucket {
			return mode, retainDate, legalHold, ErrInvalidBucketObjectLockConfiguration
		}
		var lerr error
		if legalHold, lerr = objectlock.ParseObjectLockLegalHoldHeaders(r.Header); lerr != nil {
			return mode, retainDate, legalHold, toAPIErrorCode(ctx, err)
		}
	}
	if retentionRequested {
		if !isWORMBucket {
			return mode, retainDate, legalHold, ErrInvalidBucketObjectLockConfiguration
		}
		legalHold, err := objectlock.ParseObjectLockLegalHoldHeaders(r.Header)
		if err != nil {
			return mode, retainDate, legalHold, toAPIErrorCode(ctx, err)
		}
		rMode, rDate, err := objectlock.ParseObjectLockRetentionHeaders(r.Header)
		if err != nil {
			return mode, retainDate, legalHold, toAPIErrorCode(ctx, err)
		}
		// AWS S3 just creates a new version of object when an object is being overwritten.
		t, err := objectlock.UTCNowNTP()
		if err != nil {
			logger.LogIf(ctx, err)
			return mode, retainDate, legalHold, ErrObjectLocked
		}
		if objExists && retainDate.After(t) {
			return mode, retainDate, legalHold, ErrObjectLocked
		}
		if rMode == objectlock.Invalid {
			return mode, retainDate, legalHold, toAPIErrorCode(ctx, objectlock.ErrObjectLockInvalidHeaders)
		}
		if retentionPermErr != ErrNone {
			return mode, retainDate, legalHold, retentionPermErr
		}
		return rMode, rDate, legalHold, ErrNone
	}

	if !retentionRequested && isWORMBucket {
		if retention.IsEmpty() && (mode == objectlock.Compliance || mode == objectlock.Governance) {
			return mode, retainDate, legalHold, ErrObjectLocked
		}
		if retentionPermErr != ErrNone {
			return mode, retainDate, legalHold, retentionPermErr
		}
		t, err := objectlock.UTCNowNTP()
		if err != nil {
			logger.LogIf(ctx, err)
			return mode, retainDate, legalHold, ErrObjectLocked
		}
		// AWS S3 just creates a new version of object when an object is being overwritten.
		if objExists && retainDate.After(t) {
			return mode, retainDate, legalHold, ErrObjectLocked
		}
		if !legalHoldRequested {
			// inherit retention from bucket configuration
			return retention.Mode, objectlock.RetentionDate{Time: t.Add(retention.Validity)}, legalHold, ErrNone
		}
	}
	return mode, retainDate, legalHold, ErrNone
}
