package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/minio/minio-go"
)

func isAuthenticated(req *http.Request) bool {
	authBackend := InitJWT()
	tokenRequest, err := jwt.ParseFromRequest(req, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return authBackend.PublicKey, nil
	})
	if err != nil {
		return false
	}
	return tokenRequest.Valid
}

// ListBuckets - list buckets api.
func (web *WebAPI) ListBuckets(r *http.Request, args *ListBucketsArgs, reply *[]minio.BucketInfo) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	client, err := minio.New("localhost:9000", web.AccessKeyID, web.SecretAccessKey, true)
	if err != nil {
		return err
	}
	buckets, err := client.ListBuckets()
	if err != nil {
		return err
	}
	*reply = buckets
	return nil
}

// ListObjects - list objects api.
func (web *WebAPI) ListObjects(r *http.Request, args *ListObjectsArgs, reply *[]minio.ObjectInfo) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	client, err := minio.New("localhost:9000", web.AccessKeyID, web.SecretAccessKey, true)
	if err != nil {
		return err
	}
	doneCh := make(chan struct{})
	defer close(doneCh)

	var objects []minio.ObjectInfo
	for object := range client.ListObjects(args.BucketName, args.Prefix, false, doneCh) {
		if object.Err != nil {
			return object.Err
		}
		objects = append(objects, object)
	}
	*reply = objects
	return nil
}

// GetObjectURL - get object url.
func (web *WebAPI) GetObjectURL(r *http.Request, args *GetObjectURLArgs, reply *string) error {
	if !isAuthenticated(r) {
		return errUnAuthorizedRequest
	}
	client, err := minio.New("localhost:9000", web.AccessKeyID, web.SecretAccessKey, true)
	if err != nil {
		return err
	}
	urlStr, err := client.PresignedGetObject(args.BucketName, args.ObjectName, time.Duration(60*60)*time.Second)
	if err != nil {
		return err
	}
	*reply = urlStr
	return nil
}

// Login - user login handler.
func (web *WebAPI) Login(r *http.Request, args *LoginArgs, reply *AuthToken) error {
	authBackend := InitJWT()
	if authBackend.Authenticate(args, web.AccessKeyID, web.SecretAccessKey) {
		token, err := authBackend.GenerateToken(args.Username)
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
		authBackend := InitJWT()
		token, err := authBackend.GenerateToken(args.Username)
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
		authBackend := InitJWT()
		tokenString := r.Header.Get("Authorization")
		if err := authBackend.Logout(tokenString); err != nil {
			return err
		}
		return nil
	}
	return errUnAuthorizedRequest
}
