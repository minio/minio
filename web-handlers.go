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
	"strings"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/minio/minio-go"
	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/disk"
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

// DiskInfo - get disk statistics.
func (web *WebAPI) DiskInfo(r *http.Request, args *DiskInfoArgs, reply *disk.Info) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	info, e := disk.GetInfo(web.FSPath)
	if e != nil {
		return e
	}
	*reply = info
	return nil
}

// MakeBucket - make a bucket.
func (web *WebAPI) MakeBucket(r *http.Request, args *MakeBucketArgs, reply *string) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	return web.Client.MakeBucket(args.BucketName, "", "")
}

// ListBuckets - list buckets api.
func (web *WebAPI) ListBuckets(r *http.Request, args *ListBucketsArgs, reply *[]BucketInfo) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	buckets, e := web.Client.ListBuckets()
	if e != nil {
		return e
	}
	for _, bucket := range buckets {
		*reply = append(*reply, BucketInfo{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate,
		})
	}
	return nil
}

// ListObjects - list objects api.
func (web *WebAPI) ListObjects(r *http.Request, args *ListObjectsArgs, reply *[]ObjectInfo) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	doneCh := make(chan struct{})
	defer close(doneCh)

	for object := range web.Client.ListObjects(args.BucketName, args.Prefix, false, doneCh) {
		if object.Err != nil {
			return object.Err
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
				return e
			}
			objectInfo.ContentType = objectStatInfo.ContentType
		}
		*reply = append(*reply, objectInfo)
	}
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
func (web *WebAPI) PutObjectURL(r *http.Request, args *PutObjectURLArgs, reply *string) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	targetHost, err := getTargetHost(web.apiAddress, args.TargetHost)
	if err != nil {
		return probe.WrapError(err)
	}
	client, e := minio.NewV4(targetHost, web.accessKeyID, web.secretAccessKey, web.inSecure)
	if e != nil {
		return e
	}
	signedURLStr, e := client.PresignedPutObject(args.BucketName, args.ObjectName, time.Duration(60*60)*time.Second)
	if e != nil {
		return e
	}
	*reply = signedURLStr
	return nil
}

// GetObjectURL - generates url for download access.
func (web *WebAPI) GetObjectURL(r *http.Request, args *GetObjectURLArgs, reply *string) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}

	// See if object exists.
	_, e := web.Client.StatObject(args.BucketName, args.ObjectName)
	if e != nil {
		return e
	}

	targetHost, err := getTargetHost(web.apiAddress, args.TargetHost)
	if err != nil {
		return probe.WrapError(err)
	}
	client, e := minio.NewV4(targetHost, web.accessKeyID, web.secretAccessKey, web.inSecure)
	if e != nil {
		return e
	}
	signedURLStr, e := client.PresignedGetObject(args.BucketName, args.ObjectName, time.Duration(60*60)*time.Second)
	if e != nil {
		return e
	}
	*reply = signedURLStr
	return nil
}

// Login - user login handler.
func (web *WebAPI) Login(r *http.Request, args *LoginArgs, reply *AuthToken) error {
	jwt := InitJWT()
	if jwt.Authenticate(args.Username, args.Password) {
		token, err := jwt.GenerateToken(args.Username)
		if err != nil {
			return probe.WrapError(err.Trace())
		}
		reply.Token = token
		return nil
	}
	return errUnAuthorizedRequest
}

// RefreshToken - refresh token handler.
func (web *WebAPI) RefreshToken(r *http.Request, args *LoginArgs, reply *AuthToken) error {
	if isAuthenticated(r) {
		jwt := InitJWT()
		token, err := jwt.GenerateToken(args.Username)
		if err != nil {
			return probe.WrapError(err.Trace())
		}
		reply.Token = token
		return nil
	}
	return errUnAuthorizedRequest
}

// Logout - user logout.
func (web *WebAPI) Logout(r *http.Request, arg *string, reply *string) error {
	if isAuthenticated(r) {
		return nil
	}
	return errUnAuthorizedRequest
}
