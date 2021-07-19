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
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/sync/errgroup"
)

const (
	// Block size used for all internal operations version 1.

	// TLDR..
	// Not used anymore xl.meta captures the right blockSize
	// so blockSizeV2 should be used for all future purposes.
	// this value is kept here to calculate the max API
	// requests based on RAM size for existing content.
	blockSizeV1 = 10 * humanize.MiByte

	// Block size used in erasure coding version 2.
	blockSizeV2 = 1 * humanize.MiByte

	// Buckets meta prefix.
	bucketMetaPrefix = "buckets"

	// ETag (hex encoded md5sum) of empty string.
	emptyETag = "d41d8cd98f00b204e9800998ecf8427e"
)

// Global object layer mutex, used for safely updating object layer.
var globalObjLayerMutex sync.RWMutex

// Global object layer, only accessed by globalObjectAPI.
var globalObjectAPI ObjectLayer

//Global cacheObjects, only accessed by newCacheObjectsFn().
var globalCacheObjectAPI CacheObjectLayer

// Checks if the object is a directory, this logic uses
// if size == 0 and object ends with SlashSeparator then
// returns true.
func isObjectDir(object string, size int64) bool {
	return HasSuffix(object, SlashSeparator) && size == 0
}

func newStorageAPIWithoutHealthCheck(endpoint Endpoint) (storage StorageAPI, err error) {
	if endpoint.IsLocal {
		storage, err := newXLStorage(endpoint)
		if err != nil {
			return nil, err
		}
		return newXLStorageDiskIDCheck(storage), nil
	}

	return newStorageRESTClient(endpoint, false), nil
}

// Depending on the disk type network or local, initialize storage API.
func newStorageAPI(endpoint Endpoint) (storage StorageAPI, err error) {
	if endpoint.IsLocal {
		storage, err := newXLStorage(endpoint)
		if err != nil {
			return nil, err
		}
		return newXLStorageDiskIDCheck(storage), nil
	}

	return newStorageRESTClient(endpoint, true), nil
}

func listObjectsNonSlash(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int, tpool *TreeWalkPool, listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc, getObjInfo func(context.Context, string, string) (ObjectInfo, error), getObjectInfoDirs ...func(context.Context, string, string) (ObjectInfo, error)) (loi ListObjectsInfo, err error) {
	endWalkCh := make(chan struct{})
	defer close(endWalkCh)
	recursive := true
	walkResultCh := startTreeWalk(ctx, bucket, prefix, "", recursive, listDir, isLeaf, isLeafDir, endWalkCh)

	var objInfos []ObjectInfo
	var eof bool
	var prevPrefix string

	for {
		if len(objInfos) == maxKeys {
			break
		}
		result, ok := <-walkResultCh
		if !ok {
			eof = true
			break
		}

		var objInfo ObjectInfo
		var err error

		index := strings.Index(strings.TrimPrefix(result.entry, prefix), delimiter)
		if index == -1 {
			objInfo, err = getObjInfo(ctx, bucket, result.entry)
			if err != nil {
				// Ignore errFileNotFound as the object might have got
				// deleted in the interim period of listing and getObjectInfo(),
				// ignore quorum error as it might be an entry from an outdated disk.
				if IsErrIgnored(err, []error{
					errFileNotFound,
					errErasureReadQuorum,
				}...) {
					continue
				}
				return loi, toObjectErr(err, bucket, prefix)
			}
		} else {
			index = len(prefix) + index + len(delimiter)
			currPrefix := result.entry[:index]
			if currPrefix == prevPrefix {
				continue
			}
			prevPrefix = currPrefix

			objInfo = ObjectInfo{
				Bucket: bucket,
				Name:   currPrefix,
				IsDir:  true,
			}
		}

		if objInfo.Name <= marker {
			continue
		}

		objInfos = append(objInfos, objInfo)
		if result.end {
			eof = true
			break
		}
	}

	result := ListObjectsInfo{}
	for _, objInfo := range objInfos {
		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof {
		result.IsTruncated = true
		if len(objInfos) > 0 {
			result.NextMarker = objInfos[len(objInfos)-1].Name
		}
	}

	return result, nil
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func fsWalk(ctx context.Context, obj ObjectLayer, bucket, prefix string, listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc, results chan<- ObjectInfo, getObjInfo func(context.Context, string, string) (ObjectInfo, error), getObjectInfoDirs ...func(context.Context, string, string) (ObjectInfo, error)) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, "", obj); err != nil {
		// Upon error close the channel.
		close(results)
		return err
	}

	walkResultCh := startTreeWalk(ctx, bucket, prefix, "", true, listDir, isLeaf, isLeafDir, ctx.Done())

	go func() {
		defer close(results)

		for {
			walkResult, ok := <-walkResultCh
			if !ok {
				break
			}

			var objInfo ObjectInfo
			var err error
			if HasSuffix(walkResult.entry, SlashSeparator) {
				for _, getObjectInfoDir := range getObjectInfoDirs {
					objInfo, err = getObjectInfoDir(ctx, bucket, walkResult.entry)
					if err == nil {
						break
					}
					if err == errFileNotFound {
						err = nil
						objInfo = ObjectInfo{
							Bucket: bucket,
							Name:   walkResult.entry,
							IsDir:  true,
						}
					}
				}
			} else {
				objInfo, err = getObjInfo(ctx, bucket, walkResult.entry)
			}
			if err != nil {
				continue
			}
			results <- objInfo
			if walkResult.end {
				break
			}
		}
	}()
	return nil
}

func listObjects(ctx context.Context, obj ObjectLayer, bucket, prefix, marker, delimiter string, maxKeys int, tpool *TreeWalkPool, listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc, getObjInfo func(context.Context, string, string) (ObjectInfo, error), getObjectInfoDirs ...func(context.Context, string, string) (ObjectInfo, error)) (loi ListObjectsInfo, err error) {
	if delimiter != SlashSeparator && delimiter != "" {
		return listObjectsNonSlash(ctx, bucket, prefix, marker, delimiter, maxKeys, tpool, listDir, isLeaf, isLeafDir, getObjInfo, getObjectInfoDirs...)
	}

	if err := checkListObjsArgs(ctx, bucket, prefix, marker, obj); err != nil {
		return loi, err
	}

	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(marker, prefix) {
			return loi, nil
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == SlashSeparator && prefix == SlashSeparator {
		return loi, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == SlashSeparator {
		recursive = false
	}

	walkResultCh, endWalkCh := tpool.Release(listParams{bucket, recursive, marker, prefix})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		walkResultCh = startTreeWalk(ctx, bucket, prefix, marker, recursive, listDir, isLeaf, isLeafDir, endWalkCh)
	}

	var eof bool
	var nextMarker string

	// List until maxKeys requested.
	g := errgroup.WithNErrs(maxKeys).WithConcurrency(10)
	ctx, cancel := g.WithCancelOnError(ctx)
	defer cancel()

	objInfoFound := make([]*ObjectInfo, maxKeys)
	var i int
	for i = 0; i < maxKeys; i++ {
		i := i
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}

		if HasSuffix(walkResult.entry, SlashSeparator) {
			g.Go(func() error {
				for _, getObjectInfoDir := range getObjectInfoDirs {
					objInfo, err := getObjectInfoDir(ctx, bucket, walkResult.entry)
					if err == nil {
						objInfoFound[i] = &objInfo
						// Done...
						return nil
					}

					// Add temp, may be overridden,
					if err == errFileNotFound {
						objInfoFound[i] = &ObjectInfo{
							Bucket: bucket,
							Name:   walkResult.entry,
							IsDir:  true,
						}
						continue
					}
					return toObjectErr(err, bucket, prefix)
				}
				return nil
			}, i)
		} else {
			g.Go(func() error {
				objInfo, err := getObjInfo(ctx, bucket, walkResult.entry)
				if err != nil {
					// Ignore errFileNotFound as the object might have got
					// deleted in the interim period of listing and getObjectInfo(),
					// ignore quorum error as it might be an entry from an outdated disk.
					if IsErrIgnored(err, []error{
						errFileNotFound,
						errErasureReadQuorum,
					}...) {
						return nil
					}
					return toObjectErr(err, bucket, prefix)
				}
				objInfoFound[i] = &objInfo
				return nil
			}, i)
		}

		if walkResult.end {
			eof = true
			break
		}
	}
	if err := g.WaitErr(); err != nil {
		return loi, err
	}
	// Copy found objects
	objInfos := make([]ObjectInfo, 0, i+1)
	for _, objInfo := range objInfoFound {
		if objInfo == nil {
			continue
		}
		objInfos = append(objInfos, *objInfo)
		nextMarker = objInfo.Name
	}

	// Save list routine for the next marker if we haven't reached EOF.
	params := listParams{bucket, recursive, nextMarker, prefix}
	if !eof {
		tpool.Set(params, walkResultCh, endWalkCh)
	}

	result := ListObjectsInfo{}
	for _, objInfo := range objInfos {
		if objInfo.IsDir && delimiter == SlashSeparator {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof {
		result.IsTruncated = true
		if len(objInfos) > 0 {
			result.NextMarker = objInfos[len(objInfos)-1].Name
		}
	}

	// Success.
	return result, nil
}
