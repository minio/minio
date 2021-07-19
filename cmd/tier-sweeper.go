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
	"net/http"
	"strings"

	"github.com/minio/minio/internal/bucket/lifecycle"
	xhttp "github.com/minio/minio/internal/http"
)

// objSweeper determines if a transitioned object needs to be removed from the remote tier.
// A typical usage would be like,
// os := newObjSweeper(bucket, object)
// // Perform a ObjectLayer.GetObjectInfo to fetch object version information
// goiOpts := os.GetOpts()
// gerr := objAPI.GetObjectInfo(ctx, bucket, object, goiOpts)
// if gerr == nil {
//    os.SetTransitionState(goi)
// }
//
// // After the overwriting object operation is complete.
// if jentry, ok := os.ShouldRemoveRemoteObject(); ok {
//     err := globalTierJournal.AddEntry(jentry)
//     logger.LogIf(ctx, err)
// }
type objSweeper struct {
	Object              string
	Bucket              string
	ReqVersion          string // version ID set by application, applies only to DeleteObject and DeleteObjects APIs
	Versioned           bool
	Suspended           bool
	TransitionStatus    string
	TransitionTier      string
	TransitionVersionID string
	RemoteObject        string
}

// newObjSweeper returns an objSweeper for a given bucket and object.
// It initializes the versioning information using bucket name.
func newObjSweeper(bucket, object string) *objSweeper {
	versioned := globalBucketVersioningSys.Enabled(bucket)
	suspended := globalBucketVersioningSys.Suspended(bucket)
	return &objSweeper{
		Object:    object,
		Bucket:    bucket,
		Versioned: versioned,
		Suspended: suspended,
	}
}

// versionIDer interface is used to fetch object versionIDer from disparate sources
// like http.Request and ObjectToDelete.
type versionIDer interface {
	GetVersionID() string
}

// multiDelete is a type alias for ObjectToDelete to implement versionID
// interface
type multiDelete ObjectToDelete

// GetVersionID returns object version of an object to be deleted via
// multi-delete API.
func (md multiDelete) GetVersionID() string {
	return md.VersionID
}

// singleDelete is a type alias for http.Request to implement versionID
// interface
type singleDelete http.Request

// GetVersionID returns object version of an object to be deleted via (simple)
// delete API. Note only when the versionID is set explicitly by the application
// will we return a non-empty versionID.
func (sd singleDelete) GetVersionID() string {
	return strings.TrimSpace(sd.URL.Query().Get(xhttp.VersionID))
}

// WithVersion sets the version ID from v
func (os *objSweeper) WithVersion(v versionIDer) *objSweeper {
	os.ReqVersion = v.GetVersionID()
	return os
}

// GetOpts returns ObjectOptions to fetch the object version that may be
// overwritten or deleted depending on bucket versioning status.
func (os *objSweeper) GetOpts() ObjectOptions {
	opts := ObjectOptions{
		VersionID:        os.ReqVersion,
		Versioned:        os.Versioned,
		VersionSuspended: os.Suspended,
	}
	if os.Suspended && os.ReqVersion == "" {
		opts.VersionID = nullVersionID
	}
	return opts
}

// SetTransitionState sets ILM transition related information from given info.
func (os *objSweeper) SetTransitionState(info ObjectInfo) {
	os.TransitionTier = info.TransitionTier
	os.TransitionStatus = info.TransitionStatus
	os.RemoteObject = info.transitionedObjName
	os.TransitionVersionID = info.transitionVersionID
}

// shouldRemoveRemoteObject determines if a transitioned object should be
// removed from remote tier. If remote object is to be deleted, returns the
// corresponding tier deletion journal entry and true. Otherwise returns empty
// jentry value and false.
func (os *objSweeper) shouldRemoveRemoteObject() (jentry, bool) {
	if os.TransitionStatus != lifecycle.TransitionComplete {
		return jentry{}, false
	}

	// 1. If bucket versioning is disabled, remove the remote object.
	// 2. If bucket versioning is suspended and
	//    a. version id is specified, remove its remote object.
	//    b. version id is not specified, remove null version's remote object if it exists.
	// 3. If bucket versioning is enabled and
	//    a. version id is specified, remove its remote object.
	//    b. version id is not specified, nothing to be done (a delete marker is added).
	delTier := false
	switch {
	case !os.Versioned, os.Suspended: // 1, 2.a, 2.b
		delTier = true
	case os.Versioned && os.ReqVersion != "": // 3.a
		delTier = true
	}
	if delTier {
		return jentry{
			ObjName:   os.RemoteObject,
			VersionID: os.TransitionVersionID,
			TierName:  os.TransitionTier,
		}, true
	}
	return jentry{}, false
}

// Sweep removes the transitioned object if it's no longer referred to.
func (os *objSweeper) Sweep() error {
	if je, ok := os.shouldRemoveRemoteObject(); ok {
		return globalTierJournal.AddEntry(je)
	}
	return nil
}
