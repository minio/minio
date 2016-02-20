/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/dustin/go-humanize"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio-go"
	"github.com/minio/minio/pkg/disk"
)

// isJWTReqAuthenticated validates if any incoming request to be a
// valid JWT authenticated request.
func isJWTReqAuthenticated(req *http.Request) bool {
	jwt := initJWT()
	token, e := jwtgo.ParseFromRequest(req, func(token *jwtgo.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtgo.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return jwt.secretAccessKey, nil
	})
	if e != nil {
		return false
	}
	return token.Valid
}

// GenericRep - reply structure for calls for which reply is success/failure
// for ex. RemoveObject MakeBucket
type GenericRep struct {
	UIVersion string `json:"uiVersion"`
}

// GenericArgs - empty struct
type GenericArgs struct{}

// GetUIVersion - get UI version
func (web webAPI) GetUIVersion(r *http.Request, args *GenericArgs, reply *GenericRep) error {
	reply.UIVersion = uiVersion
	return nil
}

// ServerInfoRep - server info reply.
type ServerInfoRep struct {
	MinioVersion  string
	MinioMemory   string
	MinioPlatform string
	MinioRuntime  string
	UIVersion     string `json:"uiVersion"`
}

// ServerInfoArgs  - server info args.
type ServerInfoArgs struct{}

// ServerInfo - get server info.
func (web *webAPI) ServerInfo(r *http.Request, args *ServerInfoArgs, reply *ServerInfoRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	mem := fmt.Sprintf("Used: %s | Allocated: %s | Used-Heap: %s | Allocated-Heap: %s",
		humanize.Bytes(memstats.Alloc),
		humanize.Bytes(memstats.TotalAlloc),
		humanize.Bytes(memstats.HeapAlloc),
		humanize.Bytes(memstats.HeapSys))
	platform := fmt.Sprintf("Host: %s | OS: %s | Arch: %s",
		host,
		runtime.GOOS,
		runtime.GOARCH)
	goruntime := fmt.Sprintf("Version: %s | CPUs: %s", runtime.Version(), strconv.Itoa(runtime.NumCPU()))
	reply.MinioVersion = minioVersion
	reply.MinioMemory = mem
	reply.MinioPlatform = platform
	reply.MinioRuntime = goruntime
	reply.UIVersion = uiVersion
	return nil
}

// DiskInfoArgs - disk info args.
type DiskInfoArgs struct{}

// DiskInfoRep - disk info reply.
type DiskInfoRep struct {
	DiskInfo  disk.Info `json:"diskInfo"`
	UIVersion string    `json:"uiVersion"`
}

// DiskInfo - get disk statistics.
func (web *webAPI) DiskInfo(r *http.Request, args *DiskInfoArgs, reply *DiskInfoRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	info, e := disk.GetInfo(web.FSPath)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	reply.DiskInfo = info
	reply.UIVersion = uiVersion
	return nil
}

// MakeBucketArgs - make bucket args.
type MakeBucketArgs struct {
	BucketName string `json:"bucketName"`
}

// MakeBucket - make a bucket.
func (web *webAPI) MakeBucket(r *http.Request, args *MakeBucketArgs, reply *GenericRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	reply.UIVersion = uiVersion
	e := web.Client.MakeBucket(args.BucketName, "", "")
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	return nil
}

// ListBucketsArgs - list bucket args.
type ListBucketsArgs struct{}

// ListBucketsRep - list buckets response
type ListBucketsRep struct {
	Buckets   []BucketInfo `json:"buckets"`
	UIVersion string       `json:"uiVersion"`
}

// BucketInfo container for list buckets metadata.
type BucketInfo struct {
	// The name of the bucket.
	Name string `json:"name"`
	// Date the bucket was created.
	CreationDate time.Time `json:"creationDate"`
}

// ListBuckets - list buckets api.
func (web *webAPI) ListBuckets(r *http.Request, args *ListBucketsArgs, reply *ListBucketsRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	buckets, e := web.Client.ListBuckets()
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	for _, bucket := range buckets {
		reply.Buckets = append(reply.Buckets, BucketInfo{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate,
		})
	}
	reply.UIVersion = uiVersion
	return nil
}

// ListObjectsArgs - list object args.
type ListObjectsArgs struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// ListObjectsRep - list objects response.
type ListObjectsRep struct {
	Objects   []ObjectInfo `json:"objects"`
	UIVersion string       `json:"uiVersion"`
}

// ObjectInfo container for list objects metadata.
type ObjectInfo struct {
	// Name of the object
	Key string `json:"name"`
	// Date and time the object was last modified.
	LastModified time.Time `json:"lastModified"`
	// Size in bytes of the object.
	Size int64 `json:"size"`
	// ContentType is mime type of the object.
	ContentType string `json:"contentType"`
}

// ListObjects - list objects api.
func (web *webAPI) ListObjects(r *http.Request, args *ListObjectsArgs, reply *ListObjectsRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	doneCh := make(chan struct{})
	defer close(doneCh)

	for object := range web.Client.ListObjects(args.BucketName, args.Prefix, false, doneCh) {
		if object.Err != nil {
			return &json2.Error{Message: object.Err.Error()}
		}
		objectInfo := ObjectInfo{
			Key:          object.Key,
			LastModified: object.LastModified,
			Size:         object.Size,
		}
		// TODO - This can get slower for large directories, we can
		// perhaps extend the ListObjects XML to reply back
		// ContentType as well.
		if !strings.HasSuffix(object.Key, "/") && object.Size > 0 {
			objectStatInfo, e := web.Client.StatObject(args.BucketName, object.Key)
			if e != nil {
				return &json2.Error{Message: e.Error()}
			}
			objectInfo.ContentType = objectStatInfo.ContentType
		}
		reply.Objects = append(reply.Objects, objectInfo)
	}
	reply.UIVersion = uiVersion
	return nil
}

// PutObjectURLArgs - args to generate url for upload access.
type PutObjectURLArgs struct {
	TargetHost  string `json:"targetHost"`
	TargetProto string `json:"targetProto"`
	BucketName  string `json:"bucketName"`
	ObjectName  string `json:"objectName"`
}

// PutObjectURLRep - reply for presigned upload url request.
type PutObjectURLRep struct {
	URL       string `json:"url"`
	UIVersion string `json:"uiVersion"`
}

// PutObjectURL - generates url for upload access.
func (web *webAPI) PutObjectURL(r *http.Request, args *PutObjectURLArgs, reply *PutObjectURLRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}

	// disableSSL is true if no 'https:' proto is found.
	disableSSL := (args.TargetProto != "https:")

	client, e := minio.New(args.TargetHost, web.accessKeyID, web.secretAccessKey, disableSSL)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	signedURLStr, e := client.PresignedPutObject(args.BucketName, args.ObjectName, time.Duration(60*60)*time.Second)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	reply.URL = signedURLStr
	reply.UIVersion = uiVersion
	return nil
}

// GetObjectURLArgs - args to generate url for download access.
type GetObjectURLArgs struct {
	TargetHost  string `json:"targetHost"`
	TargetProto string `json:"targetProto"`
	BucketName  string `json:"bucketName"`
	ObjectName  string `json:"objectName"`
}

// GetObjectURLRep - reply for presigned download url request.
type GetObjectURLRep struct {
	URL       string `json:"url"`
	UIVersion string `json:"uiVersion"`
}

// GetObjectURL - generates url for download access.
func (web *webAPI) GetObjectURL(r *http.Request, args *GetObjectURLArgs, reply *GetObjectURLRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}

	// See if object exists.
	_, e := web.Client.StatObject(args.BucketName, args.ObjectName)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}

	// disableSSL is true if no 'https:' proto is found.
	disableSSL := (args.TargetProto != "https:")

	client, e := minio.New(args.TargetHost, web.accessKeyID, web.secretAccessKey, disableSSL)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}

	reqParams := make(url.Values)
	// Set content disposition for browser to download the file.
	reqParams.Set("response-content-disposition", fmt.Sprintf(`attachment; filename="%s"`, filepath.Base(args.ObjectName)))
	signedURLStr, e := client.PresignedGetObject(args.BucketName, args.ObjectName, time.Duration(60*60)*time.Second, reqParams)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	reply.URL = signedURLStr
	reply.UIVersion = uiVersion
	return nil
}

// RemoveObjectArgs - args to remove an object
type RemoveObjectArgs struct {
	TargetHost string `json:"targetHost"`
	BucketName string `json:"bucketName"`
	ObjectName string `json:"objectName"`
}

// RemoveObject - removes an object.
func (web *webAPI) RemoveObject(r *http.Request, args *RemoveObjectArgs, reply *GenericRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	reply.UIVersion = uiVersion
	e := web.Client.RemoveObject(args.BucketName, args.ObjectName)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	return nil
}

// LoginArgs - login arguments.
type LoginArgs struct {
	Username string `json:"username" form:"username"`
	Password string `json:"password" form:"password"`
}

// LoginRep - login reply.
type LoginRep struct {
	Token     string `json:"token"`
	UIVersion string `json:"uiVersion"`
}

// Login - user login handler.
func (web *webAPI) Login(r *http.Request, args *LoginArgs, reply *LoginRep) error {
	jwt := initJWT()
	if jwt.Authenticate(args.Username, args.Password) {
		token, err := jwt.GenerateToken(args.Username)
		if err != nil {
			return &json2.Error{Message: err.Cause.Error(), Data: err.String()}
		}
		reply.Token = token
		reply.UIVersion = uiVersion
		return nil
	}
	return &json2.Error{Message: "Invalid credentials"}
}
