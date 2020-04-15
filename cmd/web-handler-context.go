/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/handlers"
)

const (
	kmBucket   = "BucketName"
	kmObject   = "ObjectName"
	kmObjects  = "Objects"
	kmPrefix   = "Prefix"
	kmMarker   = "Marker"
	kmUsername = "UserName"
	kmHostname = "HostName"
	kmPolicy   = "Policy"
)

// KeyValueMap extends builtin map to support setting and getting
// select fields like BucketName, ObjectName, Prefix, etc.
type KeyValueMap map[string]string

// Bucket returns the BucketName
func (km KeyValueMap) Bucket() string {
	return km[kmBucket]
}

// Object returns the ObjectName
func (km KeyValueMap) Object() string {
	return km[kmObject]
}

// Prefix returns the Prefix
func (km KeyValueMap) Prefix() string {
	return km[kmPrefix]
}

// Username returns the Username
func (km KeyValueMap) Username() string {
	return km[kmUsername]
}

// Hostname returns the Hostname
func (km KeyValueMap) Hostname() string {
	return km[kmHostname]
}

// Policy returns the Policy
func (km KeyValueMap) Policy() string {
	return km[kmPolicy]
}

// Objects returns the Objects
func (km KeyValueMap) Objects() []string {
	var objects []string
	_ = json.Unmarshal([]byte(km[kmObjects]), &objects)
	return objects
}

// SetBucket sets the given bucket to the KeyValueMap
func (km *KeyValueMap) SetBucket(bucket string) {
	(*km)[kmBucket] = bucket
}

// SetPrefix sets the given prefix to the KeyValueMap
func (km *KeyValueMap) SetPrefix(prefix string) {
	(*km)[kmPrefix] = prefix
}

// SetObject sets the given object to the KeyValueMap
func (km *KeyValueMap) SetObject(object string) {
	(*km)[kmObject] = object
}

// SetMarker sets the given marker to the KeyValueMap
func (km *KeyValueMap) SetMarker(marker string) {
	(*km)[kmMarker] = marker
}

// SetPolicy sets the given policy to the KeyValueMap
func (km *KeyValueMap) SetPolicy(policy string) {
	(*km)[kmPolicy] = policy
}

// SetExpiry sets the expiry to the KeyValueMap
func (km *KeyValueMap) SetExpiry(expiry int64) {
	(*km)[kmPolicy] = fmt.Sprintf("%d", expiry)
}

// SetObjects sets the list of objects to the KeyValueMap
func (km *KeyValueMap) SetObjects(objects []string) {
	objsVal, err := json.Marshal(objects)
	if err != nil {
		// NB this can only happen when we can't marshal a Go
		// slice to its json representation.
		objsVal = []byte("[]")
	}
	(*km)[kmObjects] = string(objsVal)
}

// SetUsername sets the username to the KeyValueMap
func (km *KeyValueMap) SetUsername(username string) {
	(*km)[kmUsername] = username
}

// SetHostname sets the hostname to the KeyValueMap
func (km *KeyValueMap) SetHostname(hostname string) {
	(*km)[kmHostname] = hostname
}

// ToKeyValuer interface wraps ToKeyValue method that allows types to
// marshal their values as a map of structure member names to their
// values, as strings
type ToKeyValuer interface {
	ToKeyValue() KeyValueMap
}

// ToKeyValue implementation for WebGenericArgs
func (args *WebGenericArgs) ToKeyValue() KeyValueMap {
	return KeyValueMap{}
}

// ToKeyValue implementation for MakeBucketArgs
func (args *MakeBucketArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetBucket(args.BucketName)
	return km
}

// ToKeyValue implementation for RemoveBucketArgs
func (args *RemoveBucketArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetBucket(args.BucketName)
	return km
}

// ToKeyValue implementation for ListObjectsArgs
func (args *ListObjectsArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetBucket(args.BucketName)
	km.SetPrefix(args.Prefix)
	km.SetMarker(args.Marker)
	return km
}

// ToKeyValue implementation for RemoveObjectArgs
func (args *RemoveObjectArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetBucket(args.BucketName)
	km.SetObjects(args.Objects)
	return km
}

// ToKeyValue implementation for LoginArgs
func (args *LoginArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetUsername(args.Username)
	return km
}

// ToKeyValue implementation for LoginSTSArgs
func (args *LoginSTSArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	return km
}

// ToKeyValue implementation for GetBucketPolicyArgs
func (args *GetBucketPolicyArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetBucket(args.BucketName)
	km.SetPrefix(args.Prefix)
	return km
}

// ToKeyValue implementation for ListAllBucketPoliciesArgs
func (args *ListAllBucketPoliciesArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetBucket(args.BucketName)
	return km
}

// ToKeyValue implementation for SetBucketPolicyWebArgs
func (args *SetBucketPolicyWebArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetBucket(args.BucketName)
	km.SetPrefix(args.Prefix)
	km.SetPolicy(args.Policy)
	return km
}

// ToKeyValue implementation for SetAuthArgs
// SetAuthArgs doesn't implement the ToKeyValue interface that will be
// used by logger subsystem down the line, to avoid leaking
// credentials to an external log target
func (args *SetAuthArgs) ToKeyValue() KeyValueMap {
	return KeyValueMap{}
}

// ToKeyValue implementation for PresignedGetArgs
func (args *PresignedGetArgs) ToKeyValue() KeyValueMap {
	km := KeyValueMap{}
	km.SetHostname(args.HostName)
	km.SetBucket(args.BucketName)
	km.SetObject(args.ObjectName)
	km.SetExpiry(args.Expiry)
	return km
}

// newWebContext creates a context with ReqInfo values from the given
// http request and api name.
func newWebContext(r *http.Request, args ToKeyValuer, api string) context.Context {
	argsMap := args.ToKeyValue()
	bucket := argsMap.Bucket()
	object := argsMap.Object()
	prefix := argsMap.Prefix()

	if prefix != "" {
		object = prefix
	}
	reqInfo := &logger.ReqInfo{
		DeploymentID: globalDeploymentID,
		RemoteHost:   handlers.GetSourceIP(r),
		Host:         getHostName(r),
		UserAgent:    r.UserAgent(),
		API:          api,
		BucketName:   bucket,
		ObjectName:   object,
	}
	return logger.SetReqInfo(GlobalContext, reqInfo)
}
