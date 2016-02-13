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
	"net"
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
	"github.com/minio/minio/pkg/probe"
)

// isAuthenticated validates if any incoming request to be a valid JWT
// authenticated request.
func isAuthenticated(req *http.Request) bool {
	jwt := InitJWT()
	tokenRequest, e := jwtgo.ParseFromRequest(req, func(token *jwtgo.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtgo.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return jwt.secretAccessKey, nil
	})
	if e != nil {
		return false
	}
	return tokenRequest.Valid
}

// GetUIVersion - get UI version
func (web WebAPI) GetUIVersion(r *http.Request, args *GenericArgs, reply *GenericRep) error {
	reply.UIVersion = uiVersion
	return nil
}

// ServerInfo - get server info.
func (web *WebAPI) ServerInfo(r *http.Request, args *ServerInfoArgs, reply *ServerInfoRep) error {
	if !isAuthenticated(r) {
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

// DiskInfo - get disk statistics.
func (web *WebAPI) DiskInfo(r *http.Request, args *DiskInfoArgs, reply *DiskInfoRep) error {
	if !isAuthenticated(r) {
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

// MakeBucket - make a bucket.
func (web *WebAPI) MakeBucket(r *http.Request, args *MakeBucketArgs, reply *GenericRep) error {
	if !isAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	reply.UIVersion = uiVersion
	e := web.Client.MakeBucket(args.BucketName, "", "")
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	return nil
}

// ListBuckets - list buckets api.
func (web *WebAPI) ListBuckets(r *http.Request, args *ListBucketsArgs, reply *ListBucketsRep) error {
	if !isAuthenticated(r) {
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

// ListObjects - list objects api.
func (web *WebAPI) ListObjects(r *http.Request, args *ListObjectsArgs, reply *ListObjectsRep) error {
	if !isAuthenticated(r) {
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

func getTargetHost(apiAddress, targetHost string) (string, *probe.Error) {
	if targetHost != "" {
		_, port, e := net.SplitHostPort(apiAddress)
		if e != nil {
			return "", probe.NewError(e)
		}
		host, _, e := net.SplitHostPort(targetHost)
		if e != nil {
			return "", probe.NewError(e)
		}
		targetHost = net.JoinHostPort(host, port)
	}
	return targetHost, nil
}

// PutObjectURL - generates url for upload access.
func (web *WebAPI) PutObjectURL(r *http.Request, args *PutObjectURLArgs, reply *PutObjectURLRep) error {
	if !isAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	targetHost, err := getTargetHost(web.apiAddress, args.TargetHost)
	if err != nil {
		return &json2.Error{Message: err.Cause.Error(), Data: err.String()}
	}
	client, e := minio.NewV4(targetHost, web.accessKeyID, web.secretAccessKey, web.inSecure)
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

// GetObjectURL - generates url for download access.
func (web *WebAPI) GetObjectURL(r *http.Request, args *GetObjectURLArgs, reply *GetObjectURLRep) error {
	if !isAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}

	// See if object exists.
	_, e := web.Client.StatObject(args.BucketName, args.ObjectName)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}

	targetHost, err := getTargetHost(web.apiAddress, args.TargetHost)
	if err != nil {
		return &json2.Error{Message: err.Cause.Error(), Data: err.String()}
	}
	client, e := minio.NewV4(targetHost, web.accessKeyID, web.secretAccessKey, web.inSecure)
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

// RemoveObject - removes an object.
func (web *WebAPI) RemoveObject(r *http.Request, args *RemoveObjectArgs, reply *GenericRep) error {
	if !isAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	reply.UIVersion = uiVersion
	e := web.Client.RemoveObject(args.BucketName, args.ObjectName)
	if e != nil {
		return &json2.Error{Message: e.Error()}
	}
	return nil
}

// Login - user login handler.
func (web *WebAPI) Login(r *http.Request, args *LoginArgs, reply *LoginRep) error {
	jwt := InitJWT()
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
