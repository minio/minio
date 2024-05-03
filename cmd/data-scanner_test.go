// Copyright (c) 2015-2023 MinIO, Inc.
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
	"context"
	"encoding/xml"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/versioning"
)

func TestApplyNewerNoncurrentVersionsLimit(t *testing.T) {
	objAPI, disks, err := prepareErasure(context.Background(), 8)
	if err != nil {
		t.Fatalf("Failed to initialize object layer: %v", err)
	}
	defer removeRoots(disks)
	setObjectLayer(objAPI)
	globalBucketMetadataSys = NewBucketMetadataSys()
	globalBucketObjectLockSys = &BucketObjectLockSys{}
	globalBucketVersioningSys = &BucketVersioningSys{}
	es := newExpiryState(context.Background(), objAPI, 0)
	workers := []chan expiryOp{make(chan expiryOp)}
	es.workers.Store(&workers)
	globalExpiryState = es
	var wg sync.WaitGroup
	wg.Add(1)
	expired := make([]ObjectToDelete, 0, 5)
	go func() {
		defer wg.Done()
		workers := globalExpiryState.workers.Load()
		for t := range (*workers)[0] {
			if t, ok := t.(newerNoncurrentTask); ok {
				expired = append(expired, t.versions...)
			}
		}
	}()
	lc := lifecycle.Lifecycle{
		Rules: []lifecycle.Rule{
			{
				ID:     "max-versions",
				Status: "Enabled",
				NoncurrentVersionExpiration: lifecycle.NoncurrentVersionExpiration{
					NewerNoncurrentVersions: 1,
				},
			},
		},
	}
	lcXML, err := xml.Marshal(lc)
	if err != nil {
		t.Fatalf("Failed to marshal lifecycle config: %v", err)
	}
	vcfg := versioning.Versioning{
		Status: "Enabled",
	}
	vcfgXML, err := xml.Marshal(vcfg)
	if err != nil {
		t.Fatalf("Failed to marshal versioning config: %v", err)
	}

	bucket := "bucket"
	obj := "obj-1"
	now := time.Now()
	meta := BucketMetadata{
		Name:                      bucket,
		Created:                   now,
		LifecycleConfigXML:        lcXML,
		VersioningConfigXML:       vcfgXML,
		VersioningConfigUpdatedAt: now,
		LifecycleConfigUpdatedAt:  now,
		lifecycleConfig:           &lc,
		versioningConfig:          &vcfg,
	}
	globalBucketMetadataSys.Set(bucket, meta)
	item := scannerItem{
		Path:       obj,
		bucket:     bucket,
		prefix:     "",
		objectName: obj,
		lifeCycle:  &lc,
	}

	modTime := time.Now()
	uuids := make([]uuid.UUID, 5)
	for i := range uuids {
		uuids[i] = uuid.UUID([16]byte{15: uint8(i + 1)})
	}
	fivs := make([]FileInfo, 5)
	for i := 0; i < 5; i++ {
		fivs[i] = FileInfo{
			Volume:      bucket,
			Name:        obj,
			VersionID:   uuids[i].String(),
			IsLatest:    i == 0,
			ModTime:     modTime.Add(-1 * time.Duration(i) * time.Minute),
			Size:        1 << 10,
			NumVersions: 5,
		}
	}
	versioned := vcfg.Status == "Enabled"
	wants := make([]ObjectInfo, 2)
	for i, fi := range fivs[:2] {
		wants[i] = fi.ToObjectInfo(bucket, obj, versioned)
	}
	gots, err := item.applyNewerNoncurrentVersionLimit(context.TODO(), objAPI, fivs, es)
	if err != nil {
		t.Fatalf("Failed with err: %v", err)
	}
	if len(gots) != len(wants) {
		t.Fatalf("Expected %d objects but got %d", len(wants), len(gots))
	}

	// Close expiry state's channel to inspect object versions enqueued for expiration
	close(workers[0])
	wg.Wait()
	for _, obj := range expired {
		switch obj.ObjectV.VersionID {
		case uuids[2].String(), uuids[3].String(), uuids[4].String():
		default:
			t.Errorf("Unexpected versionID being expired: %#v\n", obj)
		}
	}
}

func TestEvalActionFromLifecycle(t *testing.T) {
	// Tests cover only ExpiredObjectDeleteAllVersions and DelMarkerExpiration actions
	obj := ObjectInfo{
		Name:        "foo",
		ModTime:     time.Now().Add(-31 * 24 * time.Hour),
		Size:        100 << 20,
		VersionID:   uuid.New().String(),
		IsLatest:    true,
		NumVersions: 4,
	}
	delMarker := ObjectInfo{
		Name:         "foo-deleted",
		ModTime:      time.Now().Add(-61 * 24 * time.Hour),
		Size:         0,
		VersionID:    uuid.New().String(),
		IsLatest:     true,
		DeleteMarker: true,
		NumVersions:  4,
	}
	deleteAllILM := `<LifecycleConfiguration>
			    <Rule>
		               <Expiration>
		                  <Days>30</Days>
	                          <ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>
	                       </Expiration>
	                       <Filter></Filter>
	                       <Status>Enabled</Status>
			       <ID>DeleteAllVersions</ID>
	                    </Rule>
	                 </LifecycleConfiguration>`
	delMarkerILM := `<LifecycleConfiguration>
                            <Rule>
                              <ID>DelMarkerExpiration</ID>
                              <Filter></Filter>
                              <Status>Enabled</Status>
                              <DelMarkerExpiration>
                                <Days>60</Days>
                              </DelMarkerExpiration>
                             </Rule>
                       </LifecycleConfiguration>`
	deleteAllLc, err := lifecycle.ParseLifecycleConfig(strings.NewReader(deleteAllILM))
	if err != nil {
		t.Fatalf("Failed to parse deleteAllILM test ILM policy %v", err)
	}
	delMarkerLc, err := lifecycle.ParseLifecycleConfig(strings.NewReader(delMarkerILM))
	if err != nil {
		t.Fatalf("Failed to parse delMarkerILM test ILM policy %v", err)
	}
	tests := []struct {
		ilm       lifecycle.Lifecycle
		retention lock.Retention
		obj       ObjectInfo
		want      lifecycle.Action
	}{
		{
			// with object locking
			ilm:       *deleteAllLc,
			retention: lock.Retention{LockEnabled: true},
			obj:       obj,
			want:      lifecycle.NoneAction,
		},
		{
			// without object locking
			ilm:       *deleteAllLc,
			retention: lock.Retention{},
			obj:       obj,
			want:      lifecycle.DeleteAllVersionsAction,
		},
		{
			// with object locking
			ilm:       *delMarkerLc,
			retention: lock.Retention{LockEnabled: true},
			obj:       delMarker,
			want:      lifecycle.NoneAction,
		},
		{
			// without object locking
			ilm:       *delMarkerLc,
			retention: lock.Retention{},
			obj:       delMarker,
			want:      lifecycle.DelMarkerDeleteAllVersionsAction,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("TestEvalAction-%d", i), func(t *testing.T) {
			if got := evalActionFromLifecycle(context.TODO(), test.ilm, test.retention, nil, test.obj); got.Action != test.want {
				t.Fatalf("Expected %v but got %v", test.want, got)
			}
		})
	}
}
