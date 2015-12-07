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
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/minio/minio-xl/pkg/probe"
)

// ListObjects - GET bucket (list objects)
func (fs Filesystem) ListObjects(bucket string, resources BucketResourcesMetadata) ([]ObjectMetadata, BucketResourcesMetadata, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	if !IsValidBucketName(bucket) {
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
	/// automatically treat incoming "/" as "\\" on windows due to its path constraints.
	if runtime.GOOS == "windows" {
		if resources.Prefix != "" {
			resources.Prefix = strings.Replace(resources.Prefix, "/", string(os.PathSeparator), -1)
		}
		if resources.Delimiter != "" {
			resources.Delimiter = strings.Replace(resources.Delimiter, "/", string(os.PathSeparator), -1)
		}
		if resources.Marker != "" {
			resources.Marker = strings.Replace(resources.Marker, "/", string(os.PathSeparator), -1)
		}
	}

	// if delimiter is supplied and not prefix then we are the very top level, list everything and move on.
	if resources.Delimiter != "" && resources.Prefix == "" {
		files, err := ioutil.ReadDir(rootPrefix)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, resources, probe.NewError(BucketNotFound{Bucket: bucket})
			}
			return nil, resources, probe.NewError(err)
		}
		for _, fl := range files {
			if strings.HasSuffix(fl.Name(), "$multiparts") {
				continue
			}
			p.files = append(p.files, contentInfo{
				Prefix:   fl.Name(),
				Size:     fl.Size(),
				Mode:     fl.Mode(),
				ModTime:  fl.ModTime(),
				FileInfo: fl,
			})
		}
	}

	// If delimiter and prefix is supplied make sure that paging doesn't go deep, treat it as simple directory listing.
	if resources.Delimiter != "" && resources.Prefix != "" {
		if !strings.HasSuffix(resources.Prefix, resources.Delimiter) {
			fl, err := os.Stat(filepath.Join(rootPrefix, resources.Prefix))
			if err != nil {
				if os.IsNotExist(err) {
					return nil, resources, probe.NewError(ObjectNotFound{Bucket: bucket, Object: resources.Prefix})
				}
				return nil, resources, probe.NewError(err)
			}
			p.files = append(p.files, contentInfo{
				Prefix:   resources.Prefix,
				Size:     fl.Size(),
				Mode:     os.ModeDir,
				ModTime:  fl.ModTime(),
				FileInfo: fl,
			})
		} else {
			files, err := ioutil.ReadDir(filepath.Join(rootPrefix, resources.Prefix))
			if err != nil {
				if os.IsNotExist(err) {
					return nil, resources, probe.NewError(ObjectNotFound{Bucket: bucket, Object: resources.Prefix})
				}
				return nil, resources, probe.NewError(err)
			}
			for _, fl := range files {
				if strings.HasSuffix(fl.Name(), "$multiparts") {
					continue
				}
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
		}
	}
	if resources.Delimiter == "" {
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
				if splits := strings.Split(fp, (p.root + string(os.PathSeparator))); len(splits) > 1 {
					realFp = splits[1]
				}
			} else {
				if splits := strings.Split(fp, (p.root + string(os.PathSeparator))); len(splits) > 1 {
					realFp = splits[1]
				}
			}
			// If path is a directory and has a prefix verify if the file pointer
			// has the prefix if it does not skip the directory.
			if fl.Mode().IsDir() {
				if resources.Prefix != "" {
					// Skip the directory on following situations
					// - when prefix is part of file pointer along with the root path
					// - when file pointer is part of the prefix along with root path
					if !strings.HasPrefix(fp, filepath.Join(p.root, resources.Prefix)) &&
						!strings.HasPrefix(filepath.Join(p.root, resources.Prefix), fp) {
						return ErrSkipDir
					}
				}
			}
			// If path is a directory and has a marker verify if the file split file pointer
			// is lesser than the Marker top level directory if yes skip it.
			if fl.Mode().IsDir() {
				if resources.Marker != "" {
					if realFp != "" {
						// For windows split with its own os.PathSeparator
						if runtime.GOOS == "windows" {
							if realFp < strings.Split(resources.Marker, string(os.PathSeparator))[0] {
								return ErrSkipDir
							}
						} else {
							if realFp < strings.Split(resources.Marker, string(os.PathSeparator))[0] {
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
			// If windows replace all the incoming paths to API compatible paths
			if runtime.GOOS == "windows" {
				metadata.Object = sanitizeWindowsPath(metadata.Object)
			}
			if metadata.Bucket != "" {
				metadataList = append(metadataList, metadata)
			}
		}
	}
	// Sanitize common prefixes back into API compatible paths
	if runtime.GOOS == "windows" {
		resources.CommonPrefixes = sanitizeWindowsPaths(resources.CommonPrefixes...)
	}
	return metadataList, resources, nil
}

func (fs Filesystem) filterObjects(bucket string, content contentInfo, resources BucketResourcesMetadata) (ObjectMetadata, BucketResourcesMetadata, *probe.Error) {
	var err *probe.Error
	var metadata ObjectMetadata

	name := content.Prefix
	switch true {
	// Both delimiter and Prefix is present
	case resources.Delimiter != "" && resources.Prefix != "":
		if strings.HasPrefix(name, resources.Prefix) {
			trimmedName := strings.TrimPrefix(name, resources.Prefix)
			delimitedName := delimiter(trimmedName, resources.Delimiter)
			switch true {
			case name == resources.Prefix:
				// Use resources.Prefix to filter out delimited file
				metadata, err = getMetadata(fs.path, bucket, name)
				if err != nil {
					return ObjectMetadata{}, resources, err.Trace()
				}
				if metadata.Mode.IsDir() {
					resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
					return ObjectMetadata{}, resources, nil
				}
			case delimitedName == content.FileInfo.Name():
				// Use resources.Prefix to filter out delimited files
				metadata, err = getMetadata(fs.path, bucket, name)
				if err != nil {
					return ObjectMetadata{}, resources, err.Trace()
				}
				if metadata.Mode.IsDir() {
					resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
					return ObjectMetadata{}, resources, nil
				}
			case delimitedName != "":
				resources.CommonPrefixes = append(resources.CommonPrefixes, resources.Prefix+delimitedName)
			}
		}
	// Delimiter present and Prefix is absent
	case resources.Delimiter != "" && resources.Prefix == "":
		delimitedName := delimiter(name, resources.Delimiter)
		switch true {
		case delimitedName == "":
			metadata, err = getMetadata(fs.path, bucket, name)
			if err != nil {
				return ObjectMetadata{}, resources, err.Trace()
			}
			if metadata.Mode.IsDir() {
				resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
				return ObjectMetadata{}, resources, nil
			}
		case delimitedName == content.FileInfo.Name():
			metadata, err = getMetadata(fs.path, bucket, name)
			if err != nil {
				return ObjectMetadata{}, resources, err.Trace()
			}
			if metadata.Mode.IsDir() {
				resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
				return ObjectMetadata{}, resources, nil
			}
		case delimitedName != "":
			resources.CommonPrefixes = append(resources.CommonPrefixes, delimitedName)
		}
	// Delimiter is absent and only Prefix is present
	case resources.Delimiter == "" && resources.Prefix != "":
		if strings.HasPrefix(name, resources.Prefix) {
			// Do not strip prefix object output
			metadata, err = getMetadata(fs.path, bucket, name)
			if err != nil {
				return ObjectMetadata{}, resources, err.Trace()
			}
		}
	default:
		metadata, err = getMetadata(fs.path, bucket, name)
		if err != nil {
			return ObjectMetadata{}, resources, err.Trace()
		}
	}
	sortUnique(sort.StringSlice(resources.CommonPrefixes))
	return metadata, resources, nil
}
