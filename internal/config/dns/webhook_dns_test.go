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

package dns

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/mux"
)

var bucketMap sync.Map

func getBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	vi, ok := bucketMap.Load(bucketName)
	if !ok {
		http.Error(w, "bucket name does not exist", http.StatusNotFound)
		return
	}
	e := json.NewEncoder(w)
	if err := e.Encode(vi); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func putBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	if _, ok := bucketMap.Load(bucketName); ok {
		http.Error(w, "bucket name already exists", http.StatusConflict)
		return
	}
	var srvRecords []SrvRecord
	d := json.NewDecoder(r.Body)
	if err := d.Decode(&srvRecords); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	bucketMap.Store(bucketName, srvRecords)
}

func deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	if _, ok := bucketMap.Load(bucketName); !ok {
		http.Error(w, "bucket name does not exist", http.StatusNotFound)
		return
	}
	bucketMap.Delete(bucketName)
}

func listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	listBucketsRecords := make(map[string]interface{})

	bucketMap.Range(func(key, value interface{}) bool {
		bucket, ok := key.(string)
		if ok {
			listBucketsRecords[bucket] = value
		}
		return ok
	})
	enc := json.NewEncoder(w)
	if err := enc.Encode(listBucketsRecords); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func TestBucketDNS(t *testing.T) {
	rt := mux.NewRouter()
	rt.HandleFunc("/", deleteBucketHandler).Methods(http.MethodPost).Queries(
		"bucket", "{bucket:.+}",
		"delete", "true",
	)
	rt.HandleFunc("/", putBucketHandler).Methods(http.MethodPost).Queries("bucket", "{bucket:.+}")
	rt.HandleFunc("/", getBucketHandler).Methods(http.MethodGet).Queries("bucket", "{bucket:.+}")
	rt.HandleFunc("/", listBucketsHandler).Methods(http.MethodGet)

	svr := httptest.NewServer(rt)
	defer svr.Close()

	store, err := NewWebhookDNS(svr.URL,
		Authentication("test", "test"),
		PublicDomainNames("region1.example.com"),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err = store.Put("testbucket"); err != nil {
		t.Fatal(err)
	}

	srvRecords, err := store.Get("testbucket")
	if err != nil {
		t.Fatal(err)
	}

	if srvRecords[0].Host != "region1.example.com" {
		t.Fatalf("expected 'region1.example.com', got %s", srvRecords[0].Host)
	}

	if _, err = store.Get("testbucket1"); err != ErrNoEntriesFound {
		t.Fatalf("expected %v, got %v", ErrNoEntriesFound, err)
	}

	if err = store.Delete("testbucket"); err != nil {
		t.Fatal(err)
	}

	if err = store.Delete("testbucket1"); err != ErrNoEntriesFound {
		t.Fatalf("expected %v, got %v", ErrNoEntriesFound, err)
	}

	bucketRecords, err := store.List()
	if err != nil {
		t.Fatal(err)
	}

	_, ok := bucketRecords["testbucket"]
	if ok {
		t.Fatal("Expected bucket to not exist, bucket present instead")
	}
}
