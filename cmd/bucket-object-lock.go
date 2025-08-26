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

package cmd

import (
	"context"
	"errors"
	"math"
	"net/http"

	"github.com/minio/minio/internal/auth"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/policy"
)

// BucketObjectLockSys - map of bucket and retention configuration.
type BucketObjectLockSys struct{}

// Get - Get retention configuration.
func (sys *BucketObjectLockSys) Get(bucketName string) (r objectlock.Retention, err error) {
	config, _, err := globalBucketMetadataSys.GetObjectLockConfig(bucketName)
	if err != nil {
		if errors.Is(err, BucketObjectLockConfigNotFound{Bucket: bucketName}) {
			return r, nil
		}
		if errors.Is(err, errInvalidArgument) {
			return r, err
		}
		return r, err
	}
	return config.ToRetention(), nil
}

// enforceRetentionForDeletion checks if it is appropriate to remove an
// object according to locking configuration when this is lifecycle/ bucket quota asking.
func enforceRetentionForDeletion(ctx context.Context, objInfo ObjectInfo) (locked bool) {
	if objInfo.DeleteMarker {
		return false
	}

	lhold := objectlock.GetObjectLegalHoldMeta(objInfo.UserDefined)
	if lhold.Status.Valid() && lhold.Status == objectlock.LegalHoldOn {
		return true
	}

	ret := objectlock.GetObjectRetentionMeta(objInfo.UserDefined)
	if ret.Mode.Valid() && (ret.Mode == objectlock.RetCompliance || ret.Mode == objectlock.RetGovernance) {
		t, err := objectlock.UTCNowNTP()
		if err != nil {
			internalLogIf(ctx, err, logger.WarningKind)
			return true
		}
		if ret.RetainUntilDate.After(t) {
			return true
		}
	}
	return false
}

// enforceRetentionBypassForDelete enforces whether an existing object under governance can be deleted
// with governance bypass headers set in the request.
// Objects under site wide WORM can never be overwritten.
// For objects in "Governance" mode, overwrite is allowed if a) object retention date is past OR
// governance bypass headers are set and user has governance bypass permissions.
// Objects in "Compliance" mode can be overwritten only if retention date is past.
func enforceRetentionBypassForDelete(ctx context.Context, r *http.Request, bucket string, object ObjectToDelete, oi ObjectInfo, gerr error) error {
	if gerr != nil { // error from GetObjectInfo
		if _, ok := gerr.(MethodNotAllowed); ok {
			// This happens usually for a delete marker
			if oi.DeleteMarker || !oi.VersionPurgeStatus.Empty() {
				// Delete marker should be present and valid.
				return nil
			}
		}
		if isErrObjectNotFound(gerr) || isErrVersionNotFound(gerr) {
			return nil
		}
		return gerr
	}

	lhold := objectlock.GetObjectLegalHoldMeta(oi.UserDefined)
	if lhold.Status.Valid() && lhold.Status == objectlock.LegalHoldOn {
		return ObjectLocked{}
	}

	ret := objectlock.GetObjectRetentionMeta(oi.UserDefined)
	if ret.Mode.Valid() {
		switch ret.Mode {
		case objectlock.RetCompliance:
			// In compliance mode, a protected object version can't be overwritten
			// or deleted by any user, including the root user in your AWS account.
			// When an object is locked in compliance mode, its retention mode can't
			// be changed, and its retention period can't be shortened. Compliance mode
			// ensures that an object version can't be overwritten or deleted for the
			// duration of the retention period.
			t, err := objectlock.UTCNowNTP()
			if err != nil {
				internalLogIf(ctx, err, logger.WarningKind)
				return ObjectLocked{}
			}

			if !ret.RetainUntilDate.Before(t) {
				return ObjectLocked{}
			}
			return nil
		case objectlock.RetGovernance:
			// In governance mode, users can't overwrite or delete an object
			// version or alter its lock settings unless they have special
			// permissions. With governance mode, you protect objects against
			// being deleted by most users, but you can still grant some users
			// permission to alter the retention settings or delete the object
			// if necessary. You can also use governance mode to test retention-period
			// settings before creating a compliance-mode retention period.
			// To override or remove governance-mode retention settings, a
			// user must have the s3:BypassGovernanceRetention permission
			// and must explicitly include x-amz-bypass-governance-retention:true
			// as a request header with any request that requires overriding
			// governance mode.
			//
			byPassSet := objectlock.IsObjectLockGovernanceBypassSet(r.Header)
			if !byPassSet {
				t, err := objectlock.UTCNowNTP()
				if err != nil {
					internalLogIf(ctx, err, logger.WarningKind)
					return ObjectLocked{}
				}

				if !ret.RetainUntilDate.Before(t) {
					return ObjectLocked{}
				}
				return nil
			}
			// https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html#object-lock-retention-modes
			// If you try to delete objects protected by governance mode and have s3:BypassGovernanceRetention, the operation will succeed.
			if checkRequestAuthType(ctx, r, policy.BypassGovernanceRetentionAction, bucket, object.ObjectName) != ErrNone {
				return errAuthentication
			}
		}
	}
	return nil
}

// enforceRetentionBypassForPut enforces whether an existing object under governance can be overwritten
// with governance bypass headers set in the request.
// Objects under site wide WORM cannot be overwritten.
// For objects in "Governance" mode, overwrite is allowed if a) object retention date is past OR
// governance bypass headers are set and user has governance bypass permissions.
// Objects in compliance mode can be overwritten only if retention date is being extended. No mode change is permitted.
func enforceRetentionBypassForPut(ctx context.Context, r *http.Request, oi ObjectInfo, objRetention *objectlock.ObjectRetention, cred auth.Credentials, owner bool) error {
	byPassSet := objectlock.IsObjectLockGovernanceBypassSet(r.Header)

	t, err := objectlock.UTCNowNTP()
	if err != nil {
		internalLogIf(ctx, err, logger.WarningKind)
		return ObjectLocked{Bucket: oi.Bucket, Object: oi.Name, VersionID: oi.VersionID}
	}

	// Pass in relative days from current time, to additionally
	// to verify "object-lock-remaining-retention-days" policy if any.
	days := int(math.Ceil(math.Abs(objRetention.RetainUntilDate.Sub(t).Hours()) / 24))

	ret := objectlock.GetObjectRetentionMeta(oi.UserDefined)
	if ret.Mode.Valid() {
		// Retention has expired you may change whatever you like.
		if ret.RetainUntilDate.Before(t) {
			apiErr := isPutRetentionAllowed(oi.Bucket, oi.Name,
				days, objRetention.RetainUntilDate.Time,
				objRetention.Mode, byPassSet, r, cred,
				owner)
			if apiErr == ErrAccessDenied {
				return errAuthentication
			}
			return nil
		}

		switch ret.Mode {
		case objectlock.RetGovernance:
			govPerm := isPutRetentionAllowed(oi.Bucket, oi.Name, days,
				objRetention.RetainUntilDate.Time, objRetention.Mode,
				byPassSet, r, cred, owner)
			// Governance mode retention period cannot be shortened, if x-amz-bypass-governance is not set.
			if !byPassSet {
				if objRetention.Mode != objectlock.RetGovernance || objRetention.RetainUntilDate.Before((ret.RetainUntilDate.Time)) {
					return ObjectLocked{Bucket: oi.Bucket, Object: oi.Name, VersionID: oi.VersionID}
				}
			}
			if govPerm == ErrAccessDenied {
				return errAuthentication
			}
			return nil
		case objectlock.RetCompliance:
			// Compliance retention mode cannot be changed or shortened.
			// https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html#object-lock-retention-modes
			if objRetention.Mode != objectlock.RetCompliance || objRetention.RetainUntilDate.Before((ret.RetainUntilDate.Time)) {
				return ObjectLocked{Bucket: oi.Bucket, Object: oi.Name, VersionID: oi.VersionID}
			}
			apiErr := isPutRetentionAllowed(oi.Bucket, oi.Name,
				days, objRetention.RetainUntilDate.Time, objRetention.Mode,
				false, r, cred, owner)
			if apiErr == ErrAccessDenied {
				return errAuthentication
			}
			return nil
		}
		return nil
	} // No pre-existing retention metadata present.

	apiErr := isPutRetentionAllowed(oi.Bucket, oi.Name,
		days, objRetention.RetainUntilDate.Time,
		objRetention.Mode, byPassSet, r, cred, owner)
	if apiErr == ErrAccessDenied {
		return errAuthentication
	}
	return nil
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
func checkPutObjectLockAllowed(ctx context.Context, rq *http.Request, bucket, object string, getObjectInfoFn GetObjectInfoFn, retentionPermErr, legalHoldPermErr APIErrorCode) (objectlock.RetMode, objectlock.RetentionDate, objectlock.ObjectLegalHold, APIErrorCode) {
	var mode objectlock.RetMode
	var retainDate objectlock.RetentionDate
	var legalHold objectlock.ObjectLegalHold

	retentionRequested := objectlock.IsObjectLockRetentionRequested(rq.Header)
	legalHoldRequested := objectlock.IsObjectLockLegalHoldRequested(rq.Header)

	retentionCfg, err := globalBucketObjectLockSys.Get(bucket)
	if err != nil {
		return mode, retainDate, legalHold, ErrInvalidBucketObjectLockConfiguration
	}

	if !retentionCfg.LockEnabled {
		if legalHoldRequested || retentionRequested {
			return mode, retainDate, legalHold, ErrInvalidBucketObjectLockConfiguration
		}

		// If this not a WORM enabled bucket, we should return right here.
		return mode, retainDate, legalHold, ErrNone
	}

	opts, err := getOpts(ctx, rq, bucket, object)
	if err != nil {
		return mode, retainDate, legalHold, toAPIErrorCode(ctx, err)
	}

	replica := rq.Header.Get(xhttp.AmzBucketReplicationStatus) == replication.Replica.String()

	if opts.VersionID != "" && !replica {
		if objInfo, err := getObjectInfoFn(ctx, bucket, object, opts); err == nil {
			r := objectlock.GetObjectRetentionMeta(objInfo.UserDefined)
			t, err := objectlock.UTCNowNTP()
			if err != nil {
				internalLogIf(ctx, err, logger.WarningKind)
				return mode, retainDate, legalHold, ErrObjectLocked
			}
			if r.Mode == objectlock.RetCompliance && r.RetainUntilDate.After(t) {
				return mode, retainDate, legalHold, ErrObjectLocked
			}
			mode = r.Mode
			retainDate = r.RetainUntilDate
			legalHold = objectlock.GetObjectLegalHoldMeta(objInfo.UserDefined)
			// Disallow overwriting an object on legal hold
			if legalHold.Status == objectlock.LegalHoldOn {
				return mode, retainDate, legalHold, ErrObjectLocked
			}
		}
	}

	if legalHoldRequested {
		var lerr error
		if legalHold, lerr = objectlock.ParseObjectLockLegalHoldHeaders(rq.Header); lerr != nil {
			return mode, retainDate, legalHold, toAPIErrorCode(ctx, lerr)
		}
		if legalHoldPermErr != ErrNone {
			return mode, retainDate, legalHold, legalHoldPermErr
		}
	}

	if retentionRequested {
		legalHold, err := objectlock.ParseObjectLockLegalHoldHeaders(rq.Header)
		if err != nil {
			return mode, retainDate, legalHold, toAPIErrorCode(ctx, err)
		}
		rMode, rDate, err := objectlock.ParseObjectLockRetentionHeaders(rq.Header)
		if err != nil && (!replica || rMode != "" || !rDate.IsZero()) {
			return mode, retainDate, legalHold, toAPIErrorCode(ctx, err)
		}
		if retentionPermErr != ErrNone {
			return mode, retainDate, legalHold, retentionPermErr
		}
		return rMode, rDate, legalHold, ErrNone
	}
	if replica { // replica inherits retention metadata only from source
		return "", objectlock.RetentionDate{}, legalHold, ErrNone
	}
	if !retentionRequested && retentionCfg.Validity > 0 {
		if retentionPermErr != ErrNone {
			return mode, retainDate, legalHold, retentionPermErr
		}

		t, err := objectlock.UTCNowNTP()
		if err != nil {
			internalLogIf(ctx, err, logger.WarningKind)
			return mode, retainDate, legalHold, ErrObjectLocked
		}

		if !legalHoldRequested && retentionCfg.LockEnabled {
			// inherit retention from bucket configuration
			return retentionCfg.Mode, objectlock.RetentionDate{Time: t.Add(retentionCfg.Validity)}, legalHold, ErrNone
		}
		return "", objectlock.RetentionDate{}, legalHold, ErrNone
	}
	return mode, retainDate, legalHold, ErrNone
}

// NewBucketObjectLockSys returns initialized BucketObjectLockSys
func NewBucketObjectLockSys() *BucketObjectLockSys {
	return &BucketObjectLockSys{}
}
