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
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/NebulousLabs/Sia/api"
)

// SiaDaemon represents the Sia Daemon between Minio and Sia network
type SiaDaemon struct {
	SiadAddress string // Address of siad daemon API. (e.g., "127.0.0.1:9980")
	CacheDir    string // Cache directory for downloads.
	SiaRootDir  string // Root directory to store files on Sia.
	DebugMode   bool   // Whether or not debug mode is enabled.
}

// SiaObjectInfo represents object info stored on Sia
type SiaObjectInfo struct {
	SiaPath        string
	Filesize       uint64
	Renewing       bool
	Available      bool
	Redundancy     float64
	UploadProgress float64
}

// SiaFileInfo implements os.FileInfo interface and provides file information
type SiaFileInfo struct {
	FileName    string
	FileSize    int64
	FileModTime time.Time
	FileMode    os.FileMode
	FileIsDir   bool
	FileSys     interface{}
}

// Name returns the name of the file
func (o SiaFileInfo) Name() string {
	return o.FileName
}

// Size returns the size of the file in bytes
func (o SiaFileInfo) Size() int64 {
	return o.FileSize
}

// ModTime returns the modification time
func (o SiaFileInfo) ModTime() time.Time {
	return o.FileModTime
}

// Mode returns the file permissions
func (o SiaFileInfo) Mode() os.FileMode {
	return o.FileMode
}

// IsDir returns whether the file is a directory
func (o SiaFileInfo) IsDir() bool {
	return o.FileIsDir
}

// Sys returns system interface
func (o SiaFileInfo) Sys() interface{} {
	return o.FileSys
}

// newSiaDaemon creates a new Sia Daemon
func newSiaDaemon(siadAddress string, cacheDir string, siaRootDir string, debug bool) (*SiaDaemon, error) {
	daemon := &SiaDaemon{
		SiadAddress: siadAddress,
		CacheDir:    cacheDir,
		SiaRootDir:  siaRootDir,
		DebugMode:   debug,
	}

	return daemon, nil
}

// Start will start running
func (daemon *SiaDaemon) Start() *SiaServiceError {
	daemon.debugmsg("SiaDaemon.Start")

	// Make sure cache dir exists
	os.Mkdir(daemon.CacheDir, 0744)

	return nil
}

// Stop will stop
func (daemon *SiaDaemon) Stop() {
	daemon.debugmsg("SiaDaemon.Stop")
}

// ApproveObjectName returns whether or not the objectName provided is suitable for Sia
func (daemon *SiaDaemon) ApproveObjectName(objectName string) bool {
	daemon.debugmsg(fmt.Sprintf("SiaDaemon.ApproveObjectName - object: %s", objectName))

	reg, _ := regexp.Compile("[^a-zA-Z0-9., _+-]+")

	filtered := reg.ReplaceAllString(objectName, "")
	daemon.debugmsg(fmt.Sprintf("    filtered: %s", filtered))
	if filtered == objectName {
		return true
	} else {
		return false
	}
}

// DeleteObject will attempt to delete the object from Sia
func (daemon *SiaDaemon) DeleteObject(bucket string, objectName string) *SiaServiceError {
	daemon.debugmsg("SiaDaemon.DeleteObject")

	// Tell Sia daemon to delete the object
	var siaObj = daemon.getSiaObjectName(bucket, objectName)

	derr := post(daemon.SiadAddress, "/renter/delete/"+siaObj, "")
	if derr != nil {
		return &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	return nil
}

// PutObject will attempt to put an object on Sia
func (daemon *SiaDaemon) PutObject(bucket string, objectName string, size int64, srcFile string) *SiaServiceError {
	daemon.debugmsg("SiaDaemon.PutObject")

	// Tell Sia daemon to upload the object
	siaObj := daemon.getSiaObjectName(bucket, objectName)
	derr := post(daemon.SiadAddress, "/renter/upload/"+siaObj, "source="+srcFile)
	if derr != nil {
		return &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	// Need to wait for upload to complete
	return daemon.waitTillSiaUploadCompletes(bucket, objectName)
}

// ListObjects will return a list of existing objects in the bucket provided
func (daemon *SiaDaemon) ListRenterFiles(bucket string) (siaObjs []SiaObjectInfo, e *SiaServiceError) {
	daemon.debugmsg("SiaDaemon.ListRenterFiles")

	// Get list of all renter files
	var rf api.RenterFiles
	derr := getAPI(daemon.SiadAddress, "/renter/files", &rf)
	if derr != nil {
		return siaObjs, &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	var prefix string
	var root string
	if daemon.SiaRootDir == "" {
		root = ""
	} else {
		root = daemon.SiaRootDir + "/"
	}
	if bucket == "" {
		prefix = root
	} else {
		prefix = root + bucket + "/"
	}

	// Filter list
	var sobjs []SiaObjectInfo

	for _, f := range rf.Files {
		daemon.debugmsg(fmt.Sprintf("    file: %s", f.SiaPath))
		if strings.HasPrefix(f.SiaPath, prefix) {
			daemon.debugmsg(fmt.Sprintf("        matches prefix: %s", prefix))
			sobjs = append(sobjs, SiaObjectInfo{
				SiaPath:        f.SiaPath,
				Filesize:       f.Filesize,
				Renewing:       f.Renewing,
				Available:      f.Available,
				Redundancy:     f.Redundancy,
				UploadProgress: f.UploadProgress,
			})
		}
	}

	return sobjs, nil
}

// ListBuckets will detect and return existing buckets on Sia.
func (daemon *SiaDaemon) ListBuckets() (buckets []BucketInfo, e *SiaServiceError) {
	daemon.debugmsg("SiaDaemon.ListBuckets")

	sObjs, serr := daemon.ListRenterFiles("")
	if serr != nil {
		return buckets, serr
	}

	var m map[string]int
	m = make(map[string]int)

	var prefix string
	if daemon.SiaRootDir == "" {
		prefix = ""
	} else {
		prefix = daemon.SiaRootDir + "/"
	}

	for _, sObj := range sObjs {
		if strings.HasPrefix(sObj.SiaPath, prefix) {
			trimmed := strings.TrimPrefix(sObj.SiaPath, prefix)
			idx := strings.Index(trimmed, "/")
			bkt := trimmed[0:idx]
			m[bkt] = 1
		}
	}

	for k := range m {
		buckets = append(buckets, BucketInfo{
			Name: k})
		daemon.debugmsg(fmt.Sprintf("    bucket: %s", k))
	}

	return buckets, nil
}

// GuaranteeObjectIsInCache will guarantee that the specified object exists in the local cache
func (daemon *SiaDaemon) GuaranteeObjectIsInCache(bucket string, objectName string) *SiaServiceError {
	defer daemon.timeTrack(time.Now(), "GuaranteeObjectIsInCache")
	daemon.debugmsg(fmt.Sprintf("SiaDaemon.GuaranteeObjectIsInCache - bucket: %s, object: %s", bucket, objectName))

	// Minio filesystem layer may request files from .minio.sys bucket
	// If we get a request for Minio, we'll pass back success and let Minio deal with it.
	if bucket == ".minio.sys" {
		return nil
	}

	// Is file already in cache?
	file_cache := filepath.Join(daemon.CacheDir, bucket, objectName)
	abs_file_cache, err := filepath.Abs(file_cache)
	if err != nil {
		return siaErrorUnknown
	}
	_, serr := os.Stat(abs_file_cache)
	if serr == nil {
		// File exists in cache
		daemon.debugmsg("    Object already stored locally.")
		return nil
	}

	// Object not in cache, must download from Sia.
	daemon.debugmsg("    Object not stored locally. Fetching from Sia.")

	var root string
	if daemon.SiaRootDir == "" {
		root = ""
	} else {
		root = daemon.SiaRootDir + "/"
	}
	file_sia := root + bucket + "/" + objectName

	// Make sure bucket path exists in cache directory
	os.Mkdir(filepath.Join(daemon.CacheDir, bucket), 0744)

	derr := get(daemon.SiadAddress, "/renter/download/"+file_sia+"?destination="+url.QueryEscape(abs_file_cache))
	if derr != nil {
		daemon.debugmsg(fmt.Sprintf("    Error: %s", derr))
		return &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	daemon.debugmsg("    Object fetched.")
	return nil
}

// GetObjectInfo will return object information for the object specified
func (daemon *SiaDaemon) GetObjectInfo(bucket string, objectName string) (soi SiaObjectInfo, e *SiaServiceError) {
	daemon.debugmsg(fmt.Sprintf("SiaDaemon.GetObjectInfo - bucket: %s, object: %s", bucket, objectName))

	sia_path := daemon.SiaRootDir + "/" + bucket + "/" + objectName

	sObjs, serr := daemon.ListRenterFiles(bucket)
	if serr != nil {
		return soi, serr
	}

	for _, sObj := range sObjs {
		if sObj.SiaPath == sia_path {
			// Object found
			return sObj, nil
		}
	}

	// Object not found
	return soi, siaErrorObjectDoesNotExistInBucket
}

func (daemon *SiaDaemon) waitTillSiaUploadCompletes(bucket string, objectName string) *SiaServiceError {
	daemon.debugmsg("SiaDaemon.waitTillSiaUploadCompletes")
	complete := false
	for !complete {
		avail, e := daemon.isSiaFileAvailable(bucket, objectName)
		if e != nil {
			return e
		}

		if avail {
			return nil
		}
		time.Sleep(time.Duration(3000) * time.Millisecond)
	}

	return nil
}

func (daemon *SiaDaemon) isSiaFileAvailable(bucket string, objectName string) (bool, *SiaServiceError) {
	daemon.debugmsg(fmt.Sprintf("SiaDaemon.isSiaFileAvailable - bucket: %s, object: %s", bucket, objectName))

	sia_path := daemon.getSiaObjectName(bucket, objectName)
	daemon.debugmsg(fmt.Sprintf("    sia_path: %s", sia_path))

	sObjs, serr := daemon.ListRenterFiles(bucket)
	if serr != nil {
		return false, serr
	}

	for _, sObj := range sObjs {
		if sObj.SiaPath == sia_path {
			// Object found
			return sObj.Available, nil
		}
	}

	// Object not found
	return false, &SiaServiceError{Code: "SiaErrorDaemon", Message: "File not in Sia renter list"}
}

func (daemon *SiaDaemon) getSiaObjectName(bucket string, objectName string) string {
	if daemon.SiaRootDir == "" {
		return bucket + "/" + objectName
	} else {
		return daemon.SiaRootDir + "/" + bucket + "/" + objectName
	}
}

func (daemon *SiaDaemon) debugmsg(str string) {
	if daemon.DebugMode {
		fmt.Println(str)
	}
}

func (daemon *SiaDaemon) timeTrack(start time.Time, name string) {
	if daemon.DebugMode {
		elapsed := time.Since(start)
		fmt.Printf("%s took %s\n", name, elapsed)
	}
}
