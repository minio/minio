/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio-io/iodine"
)

// MakeBucket - make a new bucket
func (d donut) MakeBucket(bucket string) error {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return iodine.New(errors.New("invalid argument"), nil)
	}
	return d.makeDonutBucket(bucket)
}

// GetBucketMetadata - get bucket metadata
func (d donut) GetBucketMetadata(bucket string) (map[string]string, error) {
	err := d.getDonutBuckets()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, iodine.New(errors.New("bucket does not exist"), nil)
	}
	metadata := make(map[string]string)
	metadata["name"] = bucket
	metadata["created"] = time.Now().Format(time.RFC3339Nano) // TODO get this, from whatever is written from SetBucketMetadata
	return metadata, nil
}

// SetBucketMetadata - set bucket metadata
func (d donut) SetBucketMetadata(bucket string, metadata map[string]string) error {
	return errors.New("Not implemented")
}

// ListBuckets - return list of buckets
func (d donut) ListBuckets() (results []string, err error) {
	err = d.getDonutBuckets()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	for name := range d.buckets {
		results = append(results, name)
	}
	sort.Strings(results)
	return results, nil
}

// ListObjects - return list of objects
func (d donut) ListObjects(bucket, prefix, marker, delimiter string, maxkeys int) ([]string, []string, bool, error) {
	// TODO: Marker is not yet handled please handle it
	errParams := map[string]string{
		"bucket":    bucket,
		"prefix":    prefix,
		"marker":    marker,
		"delimiter": delimiter,
		"maxkeys":   strconv.Itoa(maxkeys),
	}
	err := d.getDonutBuckets()
	if err != nil {
		return nil, nil, false, iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, nil, false, iodine.New(errors.New("bucket does not exist"), errParams)
	}
	objectList, err := d.buckets[bucket].ListObjects()
	if err != nil {
		return nil, nil, false, iodine.New(err, errParams)
	}
	var donutObjects []string
	for objectName := range objectList {
		donutObjects = append(donutObjects, objectName)
	}
	if maxkeys <= 0 {
		maxkeys = 1000
	}
	if strings.TrimSpace(prefix) != "" {
		donutObjects = filterPrefix(donutObjects, prefix)
		donutObjects = removePrefix(donutObjects, prefix)
	}

	var actualObjects []string
	var actualPrefixes []string
	var isTruncated bool
	if strings.TrimSpace(delimiter) != "" {
		actualObjects = filterDelimited(donutObjects, delimiter)
		actualPrefixes = filterNotDelimited(donutObjects, delimiter)
		actualPrefixes = extractDir(actualPrefixes, delimiter)
		actualPrefixes = uniqueObjects(actualPrefixes)
	} else {
		actualObjects = donutObjects
	}

	var results []string
	var commonPrefixes []string
	for _, objectName := range actualObjects {
		if len(results) >= maxkeys {
			isTruncated = true
			break
		}
		results = appendUniq(results, prefix+objectName)
	}
	for _, commonPrefix := range actualPrefixes {
		commonPrefixes = appendUniq(commonPrefixes, prefix+commonPrefix)
	}
	sort.Strings(results)
	sort.Strings(commonPrefixes)
	return results, commonPrefixes, isTruncated, nil
}

// PutObject - put object
func (d donut) PutObject(bucket, object, expectedMD5Sum string, reader io.ReadCloser, metadata map[string]string) error {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return iodine.New(errors.New("invalid argument"), errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return iodine.New(errors.New("invalid argument"), errParams)
	}
	err := d.getDonutBuckets()
	if err != nil {
		return iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return iodine.New(errors.New("bucket does not exist"), nil)
	}
	err = d.buckets[bucket].PutObject(object, reader, expectedMD5Sum, metadata)
	if err != nil {
		return iodine.New(err, errParams)
	}
	return nil
}

// GetObject - get object
func (d donut) GetObject(bucket, object string) (reader io.ReadCloser, size int64, err error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return nil, 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return nil, 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	err = d.getDonutBuckets()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, 0, iodine.New(errors.New("bucket does not exist"), errParams)
	}
	return d.buckets[bucket].GetObject(object)
}

// GetObjectMetadata - get object metadata
func (d donut) GetObjectMetadata(bucket, object string) (map[string]string, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	err := d.getDonutBuckets()
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, iodine.New(errors.New("bucket does not exist"), errParams)
	}
	objectList, err := d.buckets[bucket].ListObjects()
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	donutObject, ok := objectList[object]
	if !ok {
		return nil, iodine.New(errors.New("object does not exist"), errParams)
	}
	return donutObject.GetObjectMetadata()
}
