/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"io"
	"sort"
	"strings"

	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// xlSets implements ObjectLayer combining a static list of erasure coded
// object sets. NOTE: There is no dynamic scaling allowed or intended in
// current design.
type xlSets struct {
	sets []*xlObjects

	// Pack level listObjects pool management.
	listPool *treeWalkPool
}

// Initialize new set of erasure coded sets.
func newXLSets(sets []*xlObjects) ObjectLayer {
	return &xlSets{
		sets:     sets,
		listPool: newTreeWalkPool(globalLookupTimeout),
	}
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s xlSets) StorageInfo() StorageInfo {
	var storageInfo StorageInfo
	storageInfo.Backend.Type = Erasure
	for _, layer := range s.sets {
		lstorageInfo := layer.StorageInfo()
		storageInfo.Total = storageInfo.Total + lstorageInfo.Total
		storageInfo.Free = storageInfo.Free + lstorageInfo.Free
		storageInfo.Backend.OnlineDisks = storageInfo.Backend.OnlineDisks + lstorageInfo.Backend.OnlineDisks
		storageInfo.Backend.OfflineDisks = storageInfo.Backend.OfflineDisks + lstorageInfo.Backend.OfflineDisks
	}

	_, scParity := getRedundancyCount(standardStorageClass, globalXLPerSetDiskCount)
	storageInfo.Backend.standardSCParity = scParity

	_, rrSCparity := getRedundancyCount(reducedRedundancyStorageClass, globalXLPerSetDiskCount)
	storageInfo.Backend.rrSCParity = rrSCparity

	return storageInfo
}

// Shutdown shutsdown all erasure coded sets in parallel
// returns error upon first error.
func (s xlSets) Shutdown() error {
	g := errgroup.WithNErrs(len(s.sets))

	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].Shutdown()
		}, index)
	}

	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}

	return nil
}

// MakeBucketLocation - creates a new bucket across all sets simultaneously
// even if one of the sets fail to create buckets, we proceed to undo a
// successful operation.
func (s xlSets) MakeBucketWithLocation(bucket, location string) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Create buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].MakeBucketWithLocation(bucket, location)
		}, index)
	}

	errs := g.Wait()
	// Upon even a single error we undo all previously created buckets.
	for _, err := range errs {
		if err != nil {
			undoMakeBucketSets(bucket, s.sets, errs)
			return err
		}
	}

	// Success.
	return nil
}

// This function is used to undo a successful MakeBucket operation.
func undoMakeBucketSets(bucket string, sets []*xlObjects, errs []error) {
	g := errgroup.WithNErrs(len(sets))

	// Undo previous make bucket entry on all underlying sets.
	for index := range sets {
		index := index
		if errs[index] == nil {
			g.Go(func() error {
				return sets[index].DeleteBucket(bucket)
			}, index)
		}
	}

	// Wait for all delete bucket to finish.
	g.Wait()
}

// Returns always a same erasure coded set for a given input.
func (s xlSets) getHashedSet(input string) (sets *xlObjects) {
	return s.sets[hashOrderFirstElement(input, len(s.sets))-1]
}

// GetBucketInfo - returns bucket info from one of the erasure coded set.
func (s xlSets) GetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	return s.getHashedSet(bucket).GetBucketInfo(bucket)
}

// DeleteBucket - deletes a bucket on all sets simultaneously,
// even if one of the sets fail to delete buckets, we proceed to
// undo a successful operation.
func (s xlSets) DeleteBucket(bucket string) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Delete buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].DeleteBucket(bucket)
		}, index)
	}

	errs := g.Wait()
	// For any failure, we undo all the delete buckets operation
	// by creating all the buckets.
	for _, err := range errs {
		if err != nil {
			undoDeleteBucketSets(bucket, s.sets, errs)
			return err
		}
	}

	// Success.
	return nil
}

// This function is used to undo a successful DeleteBucket operation.
func undoDeleteBucketSets(bucket string, sets []*xlObjects, errs []error) {
	g := errgroup.WithNErrs(len(sets))

	// Undo previous delete bucket on all underlying sets.
	for index := range sets {
		index := index
		if errs[index] == nil {
			g.Go(func() error {
				return sets[index].MakeBucketWithLocation(bucket, "")
			}, index)
		}
	}

	g.Wait()
}

// List all buckets from one of the set, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all sets.
func (s xlSets) ListBuckets() (buckets []BucketInfo, err error) {
	// Always lists from the same set signified by the empty string.
	return s.getHashedSet("").ListBuckets()
}

// --- Object Operations ---

// GetObject - reads an object from the hashedSet based on the object name.
func (s xlSets) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
	return s.getHashedSet(object).GetObject(bucket, object, startOffset, length, writer)
}

// PutObject - writes an object to hashedSet based on the object name.
func (s xlSets) PutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).PutObject(bucket, object, data, metadata)
}

// GetObjectInfo - reads object metadata from the hashedSet based on the object name.
func (s xlSets) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).GetObjectInfo(bucket, object)
}

// DeleteObject - deletes an object from the hashedSet based on the object name.
func (s xlSets) DeleteObject(bucket string, object string) (err error) {
	return s.getHashedSet(object).DeleteObject(bucket, object)
}

// CopyObject - copies objects from one hashedSet to another hashedSet, on server side.
func (s xlSets) CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	srcLayer := s.getHashedSet(srcObject)
	destLayer := s.getHashedSet(destObject)

	objInfo, err = srcLayer.GetObjectInfo(srcBucket, srcObject)
	if err != nil {
		return objInfo, err
	}

	// Check if this request is only metadata update.
	cpMetadataOnly := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(destBucket, destObject))
	if cpMetadataOnly {
		return srcLayer.CopyObject(srcBucket, srcObject, destBucket, destObject, metadata)
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		if gerr := srcLayer.GetObject(srcBucket, srcObject, 0, objInfo.Size, pipeWriter); gerr != nil {
			errorIf(gerr, "Unable to read %s of the object `%s/%s`.", srcBucket, srcObject)
			pipeWriter.CloseWithError(toObjectErr(gerr, srcBucket, srcObject))
			return
		}
		pipeWriter.Close() // Close writer explicitly signalling we wrote all data.
	}()

	hashReader, err := hash.NewReader(pipeReader, objInfo.Size, "", "")
	if err != nil {
		pipeReader.CloseWithError(err)
		return objInfo, toObjectErr(errors.Trace(err), destBucket, destObject)
	}

	objInfo, err = destLayer.PutObject(destBucket, destObject, hashReader, metadata)
	if err != nil {
		pipeReader.CloseWithError(err)
		return objInfo, err
	}

	// Explicitly close the reader.
	pipeReader.Close()

	return objInfo, nil
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry is a leaf or non-leaf entry.
// disks - used for doing disk.ListDir(). Sets passes set of disks.
func listDirSetsFactory(isLeaf isLeafFunc, treeWalkIgnoredErrs []error, sets ...[]StorageAPI) listDirFunc {
	listDirInternal := func(bucket, prefixDir, prefixEntry string, disks []StorageAPI) (entries []string, err error) {
		for _, disk := range disks {
			if disk == nil {
				continue
			}
			entries, err = disk.ListDir(bucket, prefixDir)
			if err != nil {
				// For any reason disk was deleted or goes offline, continue
				// and list from other disks if possible.
				if errors.IsErrIgnored(err, treeWalkIgnoredErrs...) {
					continue
				}
				return nil, errors.Trace(err)
			}

			// Filter entries that have the prefix prefixEntry.
			entries = filterMatchingPrefix(entries, prefixEntry)

			// isLeaf() check has to happen here so that
			// trailing "/" for objects can be removed.
			for i, entry := range entries {
				if isLeaf(bucket, pathJoin(prefixDir, entry)) {
					entries[i] = strings.TrimSuffix(entry, slashSeparator)
				}
			}
			break
		}
		return entries, nil
	}

	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (mergedEntries []string, delayIsLeaf bool, err error) {
		for _, disks := range sets {
			var entries []string
			entries, err = listDirInternal(bucket, prefixDir, prefixEntry, disks)
			if err != nil {
				return nil, false, err
			}

			var newEntries []string
			// Find elements in entries which are not in mergedEntries
			for _, entry := range entries {
				idx := sort.SearchStrings(mergedEntries, entry)
				// if entry is already present in mergedEntries don't add.
				if idx < len(mergedEntries) && mergedEntries[idx] == entry {
					continue
				}
				newEntries = append(newEntries, entry)
			}

			if len(newEntries) > 0 {
				// Merge the entries and sort it.
				mergedEntries = append(mergedEntries, newEntries...)
				sort.Strings(mergedEntries)
			}
		}

		return mergedEntries, false, nil
	}
	return listDir
}

// ListObjects - implements listing of objects across sets, each set is independently
// listed and subsequently merge lexically sorted inside listDirSetsFactory(). Resulting
// value through the walk channel receives the data properly lexically sorted.
func (s *xlSets) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	// validate all the inputs for listObjects
	if err = checkListObjsArgs(bucket, prefix, marker, delimiter, s); err != nil {
		return result, err
	}

	var objInfos []ObjectInfo
	var eof bool
	var nextMarker string

	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	walkResultCh, endWalkCh := s.listPool.Release(listParams{bucket, recursive, marker, prefix, false})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		isLeaf := func(bucket, entry string) bool {
			entry = strings.TrimSuffix(entry, slashSeparator)
			// Verify if we are at the leaf, a leaf is where we
			// see `xl.json` inside a directory.
			return s.getHashedSet(entry).isObject(bucket, entry)
		}

		var setDisks = make([][]StorageAPI, len(s.sets))
		for _, set := range s.sets {
			setDisks = append(setDisks, set.getLoadBalancedDisks())
		}

		listDir := listDirSetsFactory(isLeaf, xlTreeWalkIgnoredErrs, setDisks...)
		walkResultCh = startTreeWalk(bucket, prefix, marker, recursive, listDir, isLeaf, endWalkCh)
	}

	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			return result, toObjectErr(walkResult.err, bucket, prefix)
		}

		entry := walkResult.entry
		var objInfo ObjectInfo
		if hasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			objInfo.Bucket = bucket
			objInfo.Name = entry
			objInfo.IsDir = true
		} else {
			// Set the Mode to a "regular" file.
			var err error
			objInfo, err = s.getHashedSet(entry).getObjectInfo(bucket, entry)
			if err != nil {
				// Ignore errFileNotFound
				if errors.Cause(err) == errFileNotFound {
					continue
				}
				return result, toObjectErr(err, bucket, prefix)
			}
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		i++
		if walkResult.end {
			eof = true
			break
		}
	}

	params := listParams{bucket, recursive, nextMarker, prefix, false}
	if !eof {
		s.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result = ListObjectsInfo{IsTruncated: !eof}
	for _, objInfo := range objInfos {
		result.NextMarker = objInfo.Name
		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}
	return result, nil
}

func (s xlSets) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// In list multipart uploads we are going to treat input prefix as the object,
	// this means that we are not supporting directory navigation.
	return s.getHashedSet(prefix).ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (s xlSets) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	return s.getHashedSet(object).NewMultipartUpload(bucket, object, metadata)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (s xlSets) CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
	startOffset int64, length int64, metadata map[string]string) (partInfo PartInfo, err error) {

	srcLayer := s.getHashedSet(srcObject)
	destLayer := s.getHashedSet(destObject)

	// Initialize pipe to stream from source.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		if gerr := srcLayer.GetObject(srcBucket, srcObject, startOffset, length, pipeWriter); gerr != nil {
			errorIf(gerr, "Unable to read %s of the object `%s/%s`.", srcBucket, srcObject)
			pipeWriter.CloseWithError(toObjectErr(gerr, srcBucket, srcObject))
			return
		}
		// Close writer explicitly signalling we wrote all data.
		pipeWriter.Close()
		return
	}()

	hashReader, err := hash.NewReader(pipeReader, length, "", "")
	if err != nil {
		pipeReader.CloseWithError(err)
		return partInfo, toObjectErr(errors.Trace(err), destBucket, destObject)
	}

	partInfo, err = destLayer.PutObjectPart(destBucket, destObject, uploadID, partID, hashReader)
	if err != nil {
		pipeReader.CloseWithError(err)
		return partInfo, err
	}

	// Close the pipe
	pipeReader.Close()

	return partInfo, nil
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (s xlSets) PutObjectPart(bucket, object, uploadID string, partID int, data *hash.Reader) (info PartInfo, err error) {
	return s.getHashedSet(object).PutObjectPart(bucket, object, uploadID, partID, data)
}

// ListObjectParts - lists all uploaded parts to an object in hashedSet.
func (s xlSets) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	return s.getHashedSet(object).ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
}

// Aborts an in-progress multipart operation on hashedSet based on the object name.
func (s xlSets) AbortMultipartUpload(bucket, object, uploadID string) error {
	return s.getHashedSet(object).AbortMultipartUpload(bucket, object, uploadID)
}

// CompleteMultipartUpload - completes a pending multipart transaction, on hashedSet based on object name.
func (s xlSets) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []CompletePart) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).CompleteMultipartUpload(bucket, object, uploadID, uploadedParts)
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (s xlSets) HealBucket(bucket string) (err error) {
	var g errgroup.Group

	for index := range s.sets {
		index := index // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return s.sets[index].HealBucket(bucket)
		}, index)
	}

	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}

	return nil
}

// HealObject - heals inconsistent object on a hashedSet based on object name.
func (s xlSets) HealObject(bucket, object string) (int, int, error) {
	return s.getHashedSet(object).HealObject(bucket, object)
}

// This is not implemented yet, will be implemented later to comply with Admin API refactor.
func (s xlSets) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return buckets, errors.Trace(NotImplemented{})
}

// This is not implemented yet, will be implemented later to comply with Admin API refactor.
func (s xlSets) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, errors.Trace(NotImplemented{})
}

// This is not implemented yet, will be implemented later to comply with Admin API refactor.
func (s xlSets) ListUploadsHeal(bucket, prefix, marker, uploadIDMarker,
	delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return ListMultipartsInfo{}, errors.Trace(NotImplemented{})
}
