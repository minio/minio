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
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/auth"
)

// CustomCredentials extends standard with scope
type CustomCredentials struct {
	auth.Credentials `json:",inline"`
	Scope            string `json:"scope"`
}

// CredentialProvider indices credentials by keys
type CredentialProvider interface {
	Get(key string) (CustomCredentials, APIErrorCode)
}

// GlobalCredentialProvider is used to authenticate requests with custom provided credentials
var GlobalCredentialProvider CredentialProvider

// GetCredentials get current credentials.
func (s *serverConfig) GetCredentialByKey(key string) (CustomCredentials, APIErrorCode) {
	if GlobalCredentialProvider == nil || key == s.Credential.AccessKey {
		return CustomCredentials{Credentials: s.Credential, Scope: ""}, ErrNone
	}
	return GlobalCredentialProvider.Get(key)
}

// CredentialsMapProvider is a simple map provider
type CredentialsMapProvider map[string]CustomCredentials

// Get implements the CredentialProvider interface
func (cm CredentialsMapProvider) Get(key string) (cred CustomCredentials, err APIErrorCode) {
	cred, ok := cm[key]
	if !ok {
		err = ErrInvalidAccessKeyID
		return
	}
	return
}

// Bucket returns bucket component in Scope
func (cc *CustomCredentials) Bucket() string {
	if cc.Scope == "" {
		return ""
	}
	return strings.SplitN(cc.Scope[1:], slashSeparator, 2)[0]
}

// ObjectPrefix returns object prefix part in scope
func (cc *CustomCredentials) ObjectPrefix() string {
	if cc.Scope == "" {
		return ""
	}
	return strings.TrimLeft(cc.Scope[len(cc.Bucket())+1:], slashSeparator)
}

// Verified returns if /bucket/object has prefix of scope
func (cc *CustomCredentials) Verified(bucket, object string) bool {
	if cc.Scope == "" {
		return true
	}
	return strings.HasPrefix(filepath.Join("/", bucket, object), cc.Scope)
}

// GetValidFilterPrefix returns a prefix for bucket to filter out objects
func (cc *CustomCredentials) GetValidFilterPrefix(bucket, prefix string) (restrictedPrefix string, valid bool) {
	if cc.Scope == "" {
		return "", true
	}
	path := pathJoin("/", bucket, prefix)
	if prefix == "" {
		path += slashSeparator
	}
	if len(path) < len(cc.Scope) {
		if cc.Scope[:len(path)] != path {
			return "", false
		}
		// assume
		// scope: /foo/barz/folder/prefix
		// bucket: /foo
		// 1. prefix is ""
		//	first:    /foo/
		//	last:     barz/folder/prefix
		//	restrict: ""+barz/
		// 2. prefix is "barz/"
		//	first:    /foo/barz/
		//	last:     folder/prefix
		//	restrict: barz/folder/
		// 3. prefix is "barz/folder/"
		//	first:    /foo/barz/folder/
		//	last:     prefix
		//	restrict: barz/folder/prefix
		// 3. prefix is "barz/fold"
		//	first:    /foo/barz/fold
		//	last:     er/prefix
		//	restrict: barz/folder/
		last := cc.Scope[len(path):]
		if idx := strings.Index(last, "/"); idx >= 0 {
			restrictedPrefix = prefix + last[:idx+1]
		} else {
			restrictedPrefix = prefix + last
		}
	} else if !strings.HasPrefix(path, cc.Scope) {
		return "", false
	}
	valid = true
	return
}
