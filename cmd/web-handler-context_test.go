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
	"bytes"
	"net/http"
	"testing"

	"github.com/minio/minio/cmd/logger"
)

func TestKeyValueMap(t *testing.T) {
	bucket := "bucket"
	object := "object"
	prefix := "prefix"
	username := "username"
	policy := "policy"
	host := "min.io"
	objects := []string{object, object}

	km := KeyValueMap{}
	km.SetBucket(bucket)
	km.SetPrefix(prefix)
	km.SetUsername(username)
	km.SetHostname(host)
	km.SetObject(object)
	km.SetObjects(objects)
	km.SetPolicy(policy)

	if got := km.Bucket(); got != bucket {
		t.Errorf("Expected %s but got %s", bucket, got)
	}

	if got := km.Object(); got != object {
		t.Errorf("Expected %s but got %s", object, got)
	}

	areEqualObjects := func(as, bs []string) bool {
		if len(as) != len(bs) {
			return false
		}

		for i, a := range as {
			b := bs[i]
			if a != b {
				return false
			}
		}
		return true
	}

	if got := km.Objects(); !areEqualObjects(got, objects) {
		t.Errorf("Expected %s but got %s", objects, got)
	}

	if got := km.Policy(); got != policy {
		t.Errorf("Expected %s but got %s", policy, got)
	}

	if got := km.Prefix(); got != prefix {
		t.Errorf("Expected %s but got %s", prefix, got)
	}

	if got := km.Username(); got != username {
		t.Errorf("Expected %s but got %s", username, got)
	}

	if got := km.Hostname(); got != host {
		t.Errorf("Expected %s but got %s", host, got)
	}
}

func TestNewWebContext(t *testing.T) {
	api := "Test API"
	args := ListObjectsArgs{
		BucketName: "bucket",
		Prefix:     "prefix",
		Marker:     "marker",
	}

	req, err := http.NewRequest(http.MethodPost, "http://min.io", bytes.NewReader([]byte("nothing")))
	if err != nil {
		t.Fatal("Unexpected failure while creating a test request")
	}

	ctx := newWebContext(req, &args, api)
	reqInfo := logger.GetReqInfo(ctx)

	if reqInfo.API != api {
		t.Errorf("Expected %s got %s", api, reqInfo.API)
	}

	if reqInfo.BucketName != args.BucketName {
		t.Errorf("Expected %s got %s", args.BucketName, reqInfo.BucketName)
	}
}
