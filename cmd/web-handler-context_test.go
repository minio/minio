// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/minio/minio/internal/logger"
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
