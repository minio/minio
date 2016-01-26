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
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
)

// isAuthenticated validates if any incoming request to be a valid JWT
// authenticated request.
func isAuthenticated(req *http.Request) bool {
	jwt := InitJWT()
	tokenRequest, err := jwtgo.ParseFromRequest(req, func(token *jwtgo.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtgo.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return jwt.PublicKey, nil
	})
	if err != nil {
		return false
	}
	return tokenRequest.Valid
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
	buckets, err := web.Client.ListBuckets()
	if err != nil {
		return err
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
		*reply = append(*reply, ObjectInfo{
			Key:          object.Key,
			LastModified: object.LastModified,
			Size:         object.Size,
		})
	}
	return nil
}

// GetObjectURL - get object url.
func (web *WebAPI) GetObjectURL(r *http.Request, args *GetObjectURLArgs, reply *string) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	urlStr, err := web.Client.PresignedGetObject(args.BucketName, args.ObjectName, time.Duration(60*60)*time.Second)
	if err != nil {
		return err
	}
	*reply = urlStr
	return nil
}

// Login - user login handler.
func (web *WebAPI) Login(r *http.Request, args *LoginArgs, reply *AuthToken) error {
	jwt := InitJWT()
	if jwt.Authenticate(args.Username, args.Password) {
		token, err := jwt.GenerateToken(args.Username)
		if err != nil {
			return err
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
			return err
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
