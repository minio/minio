/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package fs

import (
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"io/ioutil"
	"path/filepath"

	"github.com/minio/minio-xl/pkg/probe"
)

// API - local variables
type API struct {
	path       string
	lock       *sync.Mutex
	multiparts *Multiparts
}

// MultipartSession holds active session information
type MultipartSession struct {
	TotalParts int
	UploadID   string
	Initiated  time.Time
	Parts      []*PartMetadata
}

// Multiparts collection of many parts
type Multiparts struct {
	Version       string                       `json:"version"`
	ActiveSession map[string]*MultipartSession `json:"activeSessions"`
}

// New instantiate a new donut
func New(path string) (CloudStorage, *probe.Error) {
	var err *probe.Error
	// load multiparts session from disk
	var multiparts *Multiparts
	multiparts, err = loadMultipartsSession()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			multiparts = &Multiparts{
				Version:       "1",
				ActiveSession: make(map[string]*MultipartSession),
			}
			if err := SaveMultipartsSession(multiparts); err != nil {
				return nil, err.Trace()
			}
		} else {
			return nil, err.Trace()
		}
	}
	a := API{
		path: path,
		lock: new(sync.Mutex),
	}
	a.multiparts = multiparts
	return a, nil
}

/// Bucket Operations

// DeleteBucket - delete bucket
func (fs API) DeleteBucket(bucket string) *probe.Error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// verify bucket path legal
	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucketDir := filepath.Join(fs.path, bucket)
	// check bucket exists
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		return probe.NewError(BucketNotFound{Bucket: bucket})
	}
	files, err := ioutil.ReadDir(bucketDir)
	if err != nil {
		return probe.NewError(err)
	}
	if len(files) > 0 {
		return probe.NewError(BucketNotEmpty{Bucket: bucket})
	}
	if err := os.Remove(bucketDir); err != nil {
		return probe.NewError(err)
	}
	return nil
}

// ListBuckets - Get service
func (fs API) ListBuckets() ([]BucketMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	files, err := ioutil.ReadDir(fs.path)
	if err != nil {
		return []BucketMetadata{}, probe.NewError(err)
	}

	var metadataList []BucketMetadata
	for _, file := range files {
		if !file.IsDir() {
			// if files found ignore them
			continue
		}
		if file.IsDir() {
			// if directories found with odd names, skip them too
			if !IsValidBucket(file.Name()) {
				continue
			}
		}

		metadata := BucketMetadata{
			Name:    file.Name(),
			Created: file.ModTime(),
		}
		metadataList = append(metadataList, metadata)
	}
	return metadataList, nil
}

// MakeBucket - PUT Bucket
func (fs API) MakeBucket(bucket, acl string) *probe.Error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// verify bucket path legal
	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)

	// check if bucket exists
	if _, err := os.Stat(bucketDir); err == nil {
		return probe.NewError(BucketExists{
			Bucket: bucket,
		})
	}

	// make bucket
	err := os.Mkdir(bucketDir, aclToPerm(acl))
	if err != nil {
		return probe.NewError(err)
	}
	return nil
}

// GetBucketMetadata -
func (fs API) GetBucketMetadata(bucket string) (BucketMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if !IsValidBucket(bucket) {
		return BucketMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	bucketMetadata := BucketMetadata{}
	fi, err := os.Stat(bucketDir)
	// check if bucket exists
	if os.IsNotExist(err) {
		return BucketMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	if err != nil {
		return BucketMetadata{}, probe.NewError(err)
	}

	bucketMetadata.Name = fi.Name()
	bucketMetadata.Created = fi.ModTime()
	bucketMetadata.ACL = permToACL(fi.Mode())
	return bucketMetadata, nil
}

// permToACL - convert perm to meaningful ACL
func permToACL(mode os.FileMode) BucketACL {
	switch mode.Perm() {
	case os.FileMode(0700):
		return BucketACL("private")
	case os.FileMode(0500):
		return BucketACL("public-read")
	case os.FileMode(0777):
		return BucketACL("public-read-write")
	default:
		return BucketACL("private")
	}
}

// aclToPerm - convert acl to filesystem mode
func aclToPerm(acl string) os.FileMode {
	switch acl {
	case "private":
		return os.FileMode(0700)
	case "public-read":
		return os.FileMode(0500)
	case "public-read-write":
		return os.FileMode(0777)
	default:
		return os.FileMode(0700)
	}
}

// SetBucketMetadata -
func (fs API) SetBucketMetadata(bucket string, metadata map[string]string) *probe.Error {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if !IsValidBucket(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	acl := metadata["acl"]
	if !IsValidBucketACL(acl) {
		return probe.NewError(InvalidACL{ACL: acl})
	}
	// get bucket path
	bucketDir := filepath.Join(fs.path, bucket)
	err := os.Chmod(bucketDir, aclToPerm(acl))
	if err != nil {
		return probe.NewError(err)
	}
	return nil
}

// ListObjects - GET bucket (list objects)
func (fs API) ListObjects(bucket string, resources BucketResourcesMetadata) ([]ObjectMetadata, BucketResourcesMetadata, *probe.Error) {
	if !IsValidBucket(bucket) {
		return nil, resources, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if resources.Prefix != "" && IsValidObjectName(resources.Prefix) == false {
		return nil, resources, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: resources.Prefix})
	}

	p := bucketDir{}
	rootPrefix := filepath.Join(fs.path, bucket)
	// check bucket exists
	if _, err := os.Stat(rootPrefix); os.IsNotExist(err) {
		return nil, resources, probe.NewError(BucketNotFound{Bucket: bucket})
	}

	p.root = rootPrefix
	/// automatically treat "/" delimiter as "\\" delimiter on windows due to its path constraints.
	if resources.Delimiter == "/" {
		if runtime.GOOS == "windows" {
			resources.Delimiter = "\\"
		}
	}

	// If delimiter is supplied make sure that paging doesn't go deep, treat it as simple directory listing.
	if resources.Delimiter != "" {
		files, err := ioutil.ReadDir(filepath.Join(rootPrefix, resources.Prefix))
		if err != nil {
			if os.IsNotExist(err) {
				return nil, resources, probe.NewError(ObjectNotFound{Bucket: bucket, Object: resources.Prefix})
			}
			return nil, resources, probe.NewError(err)
		}
		for _, fl := range files {
			prefix := fl.Name()
			if resources.Prefix != "" {
				prefix = filepath.Join(resources.Prefix, fl.Name())
			}
			p.files = append(p.files, contentInfo{
				Prefix:   prefix,
				Size:     fl.Size(),
				Mode:     fl.Mode(),
				ModTime:  fl.ModTime(),
				FileInfo: fl,
			})
		}
	} else {
		var files []contentInfo
		getAllFiles := func(fp string, fl os.FileInfo, err error) error {
			// If any error return back quickly
			if err != nil {
				return err
			}
			if strings.HasSuffix(fp, "$multiparts") {
				return nil
			}
			// if file pointer equals to rootPrefix - discard it
			if fp == p.root {
				return nil
			}
			if len(files) > resources.Maxkeys {
				return ErrSkipFile
			}
			// Split the root prefix from the incoming file pointer
			realFp := ""
			if runtime.GOOS == "windows" {
				if splits := strings.Split(fp, p.root+"\\"); len(splits) > 1 {
					realFp = splits[1]
				}
			} else {
				if splits := strings.Split(fp, p.root+"/"); len(splits) > 1 {
					realFp = splits[1]
				}
			}
			// If path is a directory and has a prefix verify if the file pointer
			// has the prefix if it does not skip the directory.
			if fl.Mode().IsDir() {
				if resources.Prefix != "" {
					if !strings.HasPrefix(fp, filepath.Join(p.root, resources.Prefix)) {
						return ErrSkipDir
					}
				}
			}
			// If path is a directory and has a marker verify if the file split file pointer
			// is lesser than the Marker top level directory if yes skip it.
			if fl.Mode().IsDir() {
				if resources.Marker != "" {
					if realFp != "" {
						if runtime.GOOS == "windows" {
							if realFp < strings.Split(resources.Marker, "\\")[0] {
								return ErrSkipDir
							}
						} else {
							if realFp < strings.Split(resources.Marker, "/")[0] {
								return ErrSkipDir
							}
						}
					}
				}
			}
			// If regular file verify
			if fl.Mode().IsRegular() {
				// If marker is present this will be used to check if filepointer is
				// lexically higher than then Marker
				if realFp != "" {
					if resources.Marker != "" {
						if realFp > resources.Marker {
							files = append(files, contentInfo{
								Prefix:   realFp,
								Size:     fl.Size(),
								Mode:     fl.Mode(),
								ModTime:  fl.ModTime(),
								FileInfo: fl,
							})
						}
					} else {
						files = append(files, contentInfo{
							Prefix:   realFp,
							Size:     fl.Size(),
							Mode:     fl.Mode(),
							ModTime:  fl.ModTime(),
							FileInfo: fl,
						})
					}
				}
			}
			// If file is a symlink follow it and populate values.
			if fl.Mode()&os.ModeSymlink == os.ModeSymlink {
				st, err := os.Stat(fp)
				if err != nil {
					return nil
				}
				// If marker is present this will be used to check if filepointer is
				// lexically higher than then Marker
				if realFp != "" {
					if resources.Marker != "" {
						if realFp > resources.Marker {
							files = append(files, contentInfo{
								Prefix:   realFp,
								Size:     st.Size(),
								Mode:     st.Mode(),
								ModTime:  st.ModTime(),
								FileInfo: st,
							})
						}
					} else {
						files = append(files, contentInfo{
							Prefix:   realFp,
							Size:     st.Size(),
							Mode:     st.Mode(),
							ModTime:  st.ModTime(),
							FileInfo: st,
						})
					}
				}
			}
			p.files = files
			return nil
		}
		// If no delimiter is specified, crawl through everything.
		err := Walk(rootPrefix, getAllFiles)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, resources, probe.NewError(ObjectNotFound{Bucket: bucket, Object: resources.Prefix})
			}
			return nil, resources, probe.NewError(err)
		}
	}

	var metadataList []ObjectMetadata
	var metadata ObjectMetadata

	// Filter objects
	for _, content := range p.files {
		if len(metadataList) == resources.Maxkeys {
			resources.IsTruncated = true
			if resources.IsTruncated && resources.Delimiter != "" {
				resources.NextMarker = metadataList[len(metadataList)-1].Object
			}
			break
		}
		if content.Prefix > resources.Marker {
			var err *probe.Error
			metadata, resources, err = fs.filterObjects(bucket, content, resources)
			if err != nil {
				return nil, resources, err.Trace()
			}
			if metadata.Bucket != "" {
				metadataList = append(metadataList, metadata)
			}
		}
	}
	return metadataList, resources, nil
}
