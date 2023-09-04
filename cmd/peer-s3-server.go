// Copyright (c) 2015-2022 MinIO, Inc.
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
	"encoding/gob"
	"errors"
	"net/http"
	"sort"

	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
	"github.com/minio/pkg/v2/sync/errgroup"
)

const (
	peerS3Version = "v1" // First implementation

	peerS3VersionPrefix = SlashSeparator + peerS3Version
	peerS3Prefix        = minioReservedBucketPath + "/peer-s3"
	peerS3Path          = peerS3Prefix + peerS3VersionPrefix
)

const (
	peerS3MethodHealth        = "/health"
	peerS3MethodMakeBucket    = "/make-bucket"
	peerS3MethodGetBucketInfo = "/get-bucket-info"
	peerS3MethodDeleteBucket  = "/delete-bucket"
	peerS3MethodListBuckets   = "/list-buckets"
)

const (
	peerS3Bucket            = "bucket"
	peerS3BucketDeleted     = "bucket-deleted"
	peerS3BucketForceCreate = "force-create"
	peerS3BucketForceDelete = "force-delete"
)

type peerS3Server struct{}

func (s *peerS3Server) writeErrorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte(err.Error()))
}

// IsValid - To authenticate and verify the time difference.
func (s *peerS3Server) IsValid(w http.ResponseWriter, r *http.Request) bool {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		s.writeErrorResponse(w, errServerNotInitialized)
		return false
	}

	if err := storageServerRequestValidate(r); err != nil {
		s.writeErrorResponse(w, err)
		return false
	}
	return true
}

// HealthHandler - returns true of health
func (s *peerS3Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	s.IsValid(w, r)
}

func listBucketsLocal(ctx context.Context, opts BucketOptions) (buckets []BucketInfo, err error) {
	globalLocalDrivesMu.RLock()
	globalLocalDrives := globalLocalDrives
	globalLocalDrivesMu.RUnlock()

	quorum := (len(globalLocalDrives) / 2)

	buckets = make([]BucketInfo, 0, 32)
	healBuckets := map[string]VolInfo{}

	// lists all unique buckets across drives.
	if err := listAllBuckets(ctx, globalLocalDrives, healBuckets, quorum); err != nil {
		return nil, err
	}

	// include deleted buckets in listBuckets output
	deletedBuckets := map[string]VolInfo{}

	if opts.Deleted {
		// lists all deleted buckets across drives.
		if err := listDeletedBuckets(ctx, globalLocalDrives, deletedBuckets, quorum); err != nil {
			return nil, err
		}
	}

	for _, v := range healBuckets {
		bi := BucketInfo{
			Name:    v.Name,
			Created: v.Created,
		}
		if vi, ok := deletedBuckets[v.Name]; ok {
			bi.Deleted = vi.Created
		}
		buckets = append(buckets, bi)
	}

	for _, v := range deletedBuckets {
		if _, ok := healBuckets[v.Name]; !ok {
			buckets = append(buckets, BucketInfo{
				Name:    v.Name,
				Deleted: v.Created,
			})
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func getBucketInfoLocal(ctx context.Context, bucket string, opts BucketOptions) (BucketInfo, error) {
	globalLocalDrivesMu.RLock()
	globalLocalDrives := globalLocalDrives
	globalLocalDrivesMu.RUnlock()

	g := errgroup.WithNErrs(len(globalLocalDrives)).WithConcurrency(32)
	bucketsInfo := make([]BucketInfo, len(globalLocalDrives))

	// Make a volume entry on all underlying storage disks.
	for index := range globalLocalDrives {
		index := index
		g.Go(func() error {
			if globalLocalDrives[index] == nil {
				return errDiskNotFound
			}
			volInfo, err := globalLocalDrives[index].StatVol(ctx, bucket)
			if err != nil {
				if opts.Deleted {
					dvi, derr := globalLocalDrives[index].StatVol(ctx, pathJoin(minioMetaBucket, bucketMetaPrefix, deletedBucketsPrefix, bucket))
					if derr != nil {
						return err
					}
					bucketsInfo[index] = BucketInfo{Name: bucket, Deleted: dvi.Created}
					return nil
				}
				return err
			}

			bucketsInfo[index] = BucketInfo{Name: bucket, Created: volInfo.Created}
			return nil
		}, index)
	}

	errs := g.Wait()
	if err := reduceReadQuorumErrs(ctx, errs, bucketOpIgnoredErrs, (len(globalLocalDrives) / 2)); err != nil {
		return BucketInfo{}, err
	}

	var bucketInfo BucketInfo
	for i, err := range errs {
		if err == nil {
			bucketInfo = bucketsInfo[i]
			break
		}
	}

	return bucketInfo, nil
}

func deleteBucketLocal(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	globalLocalDrivesMu.RLock()
	globalLocalDrives := globalLocalDrives
	globalLocalDrivesMu.RUnlock()

	g := errgroup.WithNErrs(len(globalLocalDrives)).WithConcurrency(32)

	// Make a volume entry on all underlying storage disks.
	for index := range globalLocalDrives {
		index := index
		g.Go(func() error {
			if globalLocalDrives[index] == nil {
				return errDiskNotFound
			}
			return globalLocalDrives[index].DeleteVol(ctx, bucket, opts.Force)
		}, index)
	}

	var recreate bool
	errs := g.Wait()
	for index, err := range errs {
		if errors.Is(err, errVolumeNotEmpty) {
			recreate = true
		}
		if err == nil && recreate {
			// ignore any errors
			globalLocalDrives[index].MakeVol(ctx, bucket)
		}
	}

	// Since we recreated buckets and error was `not-empty`, return not-empty.
	if recreate {
		return errVolumeNotEmpty
	} // for all other errors reduce by write quorum.

	return reduceWriteQuorumErrs(ctx, errs, bucketOpIgnoredErrs, (len(globalLocalDrives)/2)+1)
}

func makeBucketLocal(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	globalLocalDrivesMu.RLock()
	globalLocalDrives := globalLocalDrives
	globalLocalDrivesMu.RUnlock()

	g := errgroup.WithNErrs(len(globalLocalDrives)).WithConcurrency(32)

	// Make a volume entry on all underlying storage disks.
	for index := range globalLocalDrives {
		index := index
		g.Go(func() error {
			if globalLocalDrives[index] == nil {
				return errDiskNotFound
			}
			err := globalLocalDrives[index].MakeVol(ctx, bucket)
			if opts.ForceCreate && errors.Is(err, errVolumeExists) {
				// No need to return error when force create was
				// requested.
				return nil
			}
			return err
		}, index)
	}

	errs := g.Wait()
	return reduceWriteQuorumErrs(ctx, errs, bucketOpIgnoredErrs, (len(globalLocalDrives)/2)+1)
}

func (s *peerS3Server) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}

	bucketDeleted := r.Form.Get(peerS3BucketDeleted) == "true"

	buckets, err := listBucketsLocal(r.Context(), BucketOptions{
		Deleted: bucketDeleted,
	})
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	logger.LogIf(r.Context(), gob.NewEncoder(w).Encode(buckets))
}

// GetBucketInfoHandler implements peer BuckeInfo call, returns bucket create date.
func (s *peerS3Server) GetBucketInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}

	bucket := r.Form.Get(peerS3Bucket)
	bucketDeleted := r.Form.Get(peerS3BucketDeleted) == "true"
	bucketInfo, err := getBucketInfoLocal(r.Context(), bucket, BucketOptions{
		Deleted: bucketDeleted,
	})
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	logger.LogIf(r.Context(), gob.NewEncoder(w).Encode(bucketInfo))
}

// DeleteBucketHandler implements peer delete bucket call.
func (s *peerS3Server) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}

	bucket := r.Form.Get(peerS3Bucket)
	if isMinioMetaBucket(bucket) {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	forceDelete := r.Form.Get(peerS3BucketForceDelete) == "true"

	err := deleteBucketLocal(r.Context(), bucket, DeleteBucketOptions{
		Force: forceDelete,
	})
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// MakeBucketHandler implements peer create bucket call.
func (s *peerS3Server) MakeBucketHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}

	bucket := r.Form.Get(peerS3Bucket)
	forceCreate := r.Form.Get(peerS3BucketForceCreate) == "true"

	err := makeBucketLocal(r.Context(), bucket, MakeBucketOptions{
		ForceCreate: forceCreate,
	})
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
}

// registerPeerS3Handlers - register peer s3 router.
func registerPeerS3Handlers(router *mux.Router) {
	server := &peerS3Server{}
	subrouter := router.PathPrefix(peerS3Prefix).Subrouter()

	h := func(f http.HandlerFunc) http.HandlerFunc {
		return collectInternodeStats(httpTraceHdrs(f))
	}

	subrouter.Methods(http.MethodPost).Path(peerS3VersionPrefix + peerS3MethodHealth).HandlerFunc(h(server.HealthHandler))
	subrouter.Methods(http.MethodPost).Path(peerS3VersionPrefix + peerS3MethodMakeBucket).HandlerFunc(h(server.MakeBucketHandler))
	subrouter.Methods(http.MethodPost).Path(peerS3VersionPrefix + peerS3MethodDeleteBucket).HandlerFunc(h(server.DeleteBucketHandler))
	subrouter.Methods(http.MethodPost).Path(peerS3VersionPrefix + peerS3MethodGetBucketInfo).HandlerFunc(h(server.GetBucketInfoHandler))
	subrouter.Methods(http.MethodPost).Path(peerS3VersionPrefix + peerS3MethodListBuckets).HandlerFunc(h(server.ListBucketsHandler))
}
