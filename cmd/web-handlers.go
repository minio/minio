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

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	jwtreq "github.com/dgrijalva/jwt-go/request"
	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/miniobrowser"
)

// isJWTReqAuthenticated validates if any incoming request to be a
// valid JWT authenticated request.
func isJWTReqAuthenticated(req *http.Request) bool {
	jwt, err := newJWT()
	if err != nil {
		errorIf(err, "unable to initialize a new JWT")
		return false
	}

	var reqCallback jwtgo.Keyfunc
	reqCallback = func(token *jwtgo.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtgo.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(jwt.SecretAccessKey), nil
	}
	token, err := jwtreq.ParseFromRequest(req, jwtreq.AuthorizationHeaderExtractor, reqCallback)
	if err != nil {
		errorIf(err, "token parsing failed")
		return false
	}
	return token.Valid
}

// WebGenericArgs - empty struct for calls that don't accept arguments
// for ex. ServerInfo, GenerateAuth
type WebGenericArgs struct{}

// WebGenericRep - reply structure for calls for which reply is success/failure
// for ex. RemoveObject MakeBucket
type WebGenericRep struct {
	UIVersion string `json:"uiVersion"`
}

// ServerInfoRep - server info reply.
type ServerInfoRep struct {
	MinioVersion  string
	MinioMemory   string
	MinioPlatform string
	MinioRuntime  string
	MinioEnvVars  []string
	UIVersion     string `json:"uiVersion"`
}

// ServerInfo - get server info.
func (web *webAPIHandlers) ServerInfo(r *http.Request, args *WebGenericArgs, reply *ServerInfoRep) error {
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

	reply.MinioEnvVars = os.Environ()
	reply.MinioVersion = Version
	reply.MinioMemory = mem
	reply.MinioPlatform = platform
	reply.MinioRuntime = goruntime
	reply.UIVersion = miniobrowser.UIVersion
	return nil
}

// StorageInfoRep - contains storage usage statistics.
type StorageInfoRep struct {
	StorageInfo StorageInfo `json:"storageInfo"`
	UIVersion   string      `json:"uiVersion"`
}

// StorageInfo - web call to gather storage usage statistics.
func (web *webAPIHandlers) StorageInfo(r *http.Request, args *GenericArgs, reply *StorageInfoRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	reply.UIVersion = miniobrowser.UIVersion
	reply.StorageInfo = web.ObjectAPI.StorageInfo()
	return nil
}

// MakeBucketArgs - make bucket args.
type MakeBucketArgs struct {
	BucketName string `json:"bucketName"`
}

// MakeBucket - make a bucket.
func (web *webAPIHandlers) MakeBucket(r *http.Request, args *MakeBucketArgs, reply *WebGenericRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	reply.UIVersion = miniobrowser.UIVersion
	if err := web.ObjectAPI.MakeBucket(args.BucketName); err != nil {
		return &json2.Error{Message: err.Error()}
	}
	return nil
}

// ListBucketsRep - list buckets response
type ListBucketsRep struct {
	Buckets   []WebBucketInfo `json:"buckets"`
	UIVersion string          `json:"uiVersion"`
}

// WebBucketInfo container for list buckets metadata.
type WebBucketInfo struct {
	// The name of the bucket.
	Name string `json:"name"`
	// Date the bucket was created.
	CreationDate time.Time `json:"creationDate"`
}

// ListBuckets - list buckets api.
func (web *webAPIHandlers) ListBuckets(r *http.Request, args *WebGenericArgs, reply *ListBucketsRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	buckets, err := web.ObjectAPI.ListBuckets()
	if err != nil {
		return &json2.Error{Message: err.Error()}
	}
	for _, bucket := range buckets {
		// List all buckets which are not private.
		if bucket.Name != path.Base(reservedBucket) {
			reply.Buckets = append(reply.Buckets, WebBucketInfo{
				Name:         bucket.Name,
				CreationDate: bucket.Created,
			})
		}
	}
	reply.UIVersion = miniobrowser.UIVersion
	return nil
}

// ListObjectsArgs - list object args.
type ListObjectsArgs struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// ListObjectsRep - list objects response.
type ListObjectsRep struct {
	Objects   []WebObjectInfo `json:"objects"`
	UIVersion string          `json:"uiVersion"`
}

// WebObjectInfo container for list objects metadata.
type WebObjectInfo struct {
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
func (web *webAPIHandlers) ListObjects(r *http.Request, args *ListObjectsArgs, reply *ListObjectsRep) error {
	marker := ""
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	for {
		lo, err := web.ObjectAPI.ListObjects(args.BucketName, args.Prefix, marker, "/", 1000)
		if err != nil {
			return &json2.Error{Message: err.Error()}
		}
		marker = lo.NextMarker
		for _, obj := range lo.Objects {
			reply.Objects = append(reply.Objects, WebObjectInfo{
				Key:          obj.Name,
				LastModified: obj.ModTime,
				Size:         obj.Size,
			})
		}
		for _, prefix := range lo.Prefixes {
			reply.Objects = append(reply.Objects, WebObjectInfo{
				Key: prefix,
			})
		}
		if !lo.IsTruncated {
			break
		}
	}
	reply.UIVersion = miniobrowser.UIVersion
	return nil
}

// RemoveObjectArgs - args to remove an object
type RemoveObjectArgs struct {
	TargetHost string `json:"targetHost"`
	BucketName string `json:"bucketName"`
	ObjectName string `json:"objectName"`
}

// RemoveObject - removes an object.
func (web *webAPIHandlers) RemoveObject(r *http.Request, args *RemoveObjectArgs, reply *WebGenericRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	reply.UIVersion = miniobrowser.UIVersion
	if err := web.ObjectAPI.DeleteObject(args.BucketName, args.ObjectName); err != nil {
		return &json2.Error{Message: err.Error()}
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
func (web *webAPIHandlers) Login(r *http.Request, args *LoginArgs, reply *LoginRep) error {
	jwt, err := newJWT()
	if err != nil {
		return &json2.Error{Message: err.Error()}
	}

	if err = jwt.Authenticate(args.Username, args.Password); err != nil {
		return &json2.Error{Message: err.Error()}
	}

	token, err := jwt.GenerateToken(args.Username)
	if err != nil {
		return &json2.Error{Message: err.Error()}
	}
	reply.Token = token
	reply.UIVersion = miniobrowser.UIVersion
	return nil
}

// GenerateAuthReply - reply for GenerateAuth
type GenerateAuthReply struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	UIVersion string `json:"uiVersion"`
}

func (web webAPIHandlers) GenerateAuth(r *http.Request, args *WebGenericArgs, reply *GenerateAuthReply) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	cred := mustGenAccessKeys()
	reply.AccessKey = cred.AccessKeyID
	reply.SecretKey = cred.SecretAccessKey
	reply.UIVersion = miniobrowser.UIVersion
	return nil
}

// SetAuthArgs - argument for SetAuth
type SetAuthArgs struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

// SetAuthReply - reply for SetAuth
type SetAuthReply struct {
	Token     string `json:"token"`
	UIVersion string `json:"uiVersion"`
}

// SetAuth - Set accessKey and secretKey credentials.
func (web *webAPIHandlers) SetAuth(r *http.Request, args *SetAuthArgs, reply *SetAuthReply) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	if !isValidAccessKey.MatchString(args.AccessKey) {
		return &json2.Error{Message: "Invalid Access Key"}
	}
	if !isValidSecretKey.MatchString(args.SecretKey) {
		return &json2.Error{Message: "Invalid Secret Key"}
	}
	cred := credential{args.AccessKey, args.SecretKey}
	serverConfig.SetCredential(cred)
	if err := serverConfig.Save(); err != nil {
		return &json2.Error{Message: err.Error()}
	}

	jwt, err := newJWT()
	if err != nil {
		return &json2.Error{Message: err.Error()}
	}

	if err = jwt.Authenticate(args.AccessKey, args.SecretKey); err != nil {
		return &json2.Error{Message: err.Error()}
	}
	token, err := jwt.GenerateToken(args.AccessKey)
	if err != nil {
		return &json2.Error{Message: err.Error()}
	}
	reply.Token = token
	reply.UIVersion = miniobrowser.UIVersion
	return nil
}

// GetAuthReply - Reply current credentials.
type GetAuthReply struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	UIVersion string `json:"uiVersion"`
}

// GetAuth - return accessKey and secretKey credentials.
func (web *webAPIHandlers) GetAuth(r *http.Request, args *WebGenericArgs, reply *GetAuthReply) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	creds := serverConfig.GetCredential()
	reply.AccessKey = creds.AccessKeyID
	reply.SecretKey = creds.SecretAccessKey
	reply.UIVersion = miniobrowser.UIVersion
	return nil
}

// Upload - file upload handler.
func (web *webAPIHandlers) Upload(w http.ResponseWriter, r *http.Request) {
	if !isJWTReqAuthenticated(r) {
		writeWebErrorResponse(w, errInvalidToken)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Extract incoming metadata if any.
	metadata := extractMetadataFromHeader(r.Header)

	if _, err := web.ObjectAPI.PutObject(bucket, object, -1, r.Body, metadata); err != nil {
		writeWebErrorResponse(w, err)
		return
	}

	// Fetch object info for notifications.
	objInfo, err := web.ObjectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		errorIf(err, "Unable to fetch object info for \"%s\"", path.Join(bucket, object))
		return
	}

	if eventN.IsBucketNotificationSet(bucket) {
		// Notify object created event.
		eventNotify(eventData{
			Type:    ObjectCreatedPut,
			Bucket:  bucket,
			ObjInfo: objInfo,
			ReqParams: map[string]string{
				"sourceIPAddress": r.RemoteAddr,
			},
		})
	}
}

// Download - file download handler.
func (web *webAPIHandlers) Download(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	tokenStr := r.URL.Query().Get("token")

	jwt, err := newJWT()
	if err != nil {
		errorIf(err, "error in getting new JWT")
		return
	}

	token, e := jwtgo.Parse(tokenStr, func(token *jwtgo.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtgo.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(jwt.SecretAccessKey), nil
	})
	if e != nil || !token.Valid {
		writeWebErrorResponse(w, errInvalidToken)
		return
	}
	// Add content disposition.
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", path.Base(object)))

	objInfo, err := web.ObjectAPI.GetObjectInfo(bucket, object)
	if err != nil {
		writeWebErrorResponse(w, err)
		return
	}
	offset := int64(0)
	err = web.ObjectAPI.GetObject(bucket, object, offset, objInfo.Size, w)
	if err != nil {
		/// No need to print error, response writer already written to.
		return
	}
}

// writeWebErrorResponse - set HTTP status code and write error description to the body.
func writeWebErrorResponse(w http.ResponseWriter, err error) {
	// Handle invalid token as a special case.
	if err == errInvalidToken {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err.Error()))
		return
	}
	// Convert error type to api error code.
	var apiErrCode APIErrorCode
	switch err.(type) {
	case StorageFull:
		apiErrCode = ErrStorageFull
	case BucketNotFound:
		apiErrCode = ErrNoSuchBucket
	case BucketNameInvalid:
		apiErrCode = ErrInvalidBucketName
	case BadDigest:
		apiErrCode = ErrBadDigest
	case IncompleteBody:
		apiErrCode = ErrIncompleteBody
	case ObjectExistsAsDirectory:
		apiErrCode = ErrObjectExistsAsDirectory
	case ObjectNotFound:
		apiErrCode = ErrNoSuchKey
	case ObjectNameInvalid:
		apiErrCode = ErrNoSuchKey
	case InsufficientWriteQuorum:
		apiErrCode = ErrWriteQuorum
	case InsufficientReadQuorum:
		apiErrCode = ErrReadQuorum
	default:
		apiErrCode = ErrInternalError
	}
	apiErr := getAPIError(apiErrCode)
	w.WriteHeader(apiErr.HTTPStatusCode)
	w.Write([]byte(apiErr.Description))
}

// GetBucketPolicyArgs - get bucket policy args.
type GetBucketPolicyArgs struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// GetBucketPolicyRep - get bucket policy reply.
type GetBucketPolicyRep struct {
	UIVersion string              `json:"uiVersion"`
	Policy    policy.BucketPolicy `json:"policy"`
}

func readBucketAccessPolicy(objAPI ObjectLayer, bucketName string) (policy.BucketAccessPolicy, error) {
	bucketPolicyReader, err := readBucketPolicyJSON(bucketName, objAPI)
	if err != nil {
		if _, ok := err.(BucketPolicyNotFound); ok {
			return policy.BucketAccessPolicy{}, nil
		}
		return policy.BucketAccessPolicy{}, err
	}

	bucketPolicyBuf, err := ioutil.ReadAll(bucketPolicyReader)
	if err != nil {
		return policy.BucketAccessPolicy{}, err
	}

	policyInfo := policy.BucketAccessPolicy{}
	err = json.Unmarshal(bucketPolicyBuf, &policyInfo)
	if err != nil {
		return policy.BucketAccessPolicy{}, err
	}

	return policyInfo, nil

}

// GetBucketPolicy - get bucket policy.
func (web *webAPIHandlers) GetBucketPolicy(r *http.Request, args *GetBucketPolicyArgs, reply *GetBucketPolicyRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}

	policyInfo, err := readBucketAccessPolicy(web.ObjectAPI, args.BucketName)
	if err != nil {
		return &json2.Error{Message: err.Error()}
	}

	bucketPolicy := policy.GetPolicy(policyInfo.Statements, args.BucketName, args.Prefix)

	reply.UIVersion = miniobrowser.UIVersion
	reply.Policy = bucketPolicy

	return nil
}

// SetBucketPolicyArgs - set bucket policy args.
type SetBucketPolicyArgs struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
	Policy     string `json:"policy"`
}

// SetBucketPolicy - set bucket policy.
func (web *webAPIHandlers) SetBucketPolicy(r *http.Request, args *SetBucketPolicyArgs, reply *WebGenericRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}

	bucketPolicy := policy.BucketPolicy(args.Policy)
	if !bucketPolicy.IsValidBucketPolicy() {
		return &json2.Error{Message: "Invalid policy " + args.Policy}
	}

	policyInfo, err := readBucketAccessPolicy(web.ObjectAPI, args.BucketName)
	if err != nil {
		return &json2.Error{Message: err.Error()}
	}

	policyInfo.Statements = policy.SetPolicy(policyInfo.Statements, bucketPolicy, args.BucketName, args.Prefix)

	data, err := json.Marshal(policyInfo)
	if err != nil {
		return &json2.Error{Message: err.Error()}
	}

	// TODO: update policy statements according to bucket name, prefix and policy arguments.
	if err := writeBucketPolicy(args.BucketName, web.ObjectAPI, bytes.NewReader(data), int64(len(data))); err != nil {
		return &json2.Error{Message: err.Error()}
	}

	reply.UIVersion = miniobrowser.UIVersion

	return nil
}
