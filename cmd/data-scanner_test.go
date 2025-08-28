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
	"encoding/xml"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	xhttp "github.com/minio/minio/internal/http"
)

func TestApplyNewerNoncurrentVersionsLimit(t *testing.T) {
	// Prepare object layer
	objAPI, disks, err := prepareErasure(t.Context(), 8)
	if err != nil {
		t.Fatalf("Failed to initialize object layer: %v", err)
	}
	defer removeRoots(disks)
	setObjectLayer(objAPI)

	// Prepare bucket metadata
	globalBucketMetadataSys = NewBucketMetadataSys()
	globalBucketObjectLockSys = &BucketObjectLockSys{}
	globalBucketVersioningSys = &BucketVersioningSys{}

	lcXML := `
<LifecycleConfiguration>
       <Rule>
               <ID>max-versions</ID>
               <Status>Enabled</Status>
               <NoncurrentVersionExpiration>
                       <NewerNoncurrentVersions>2</NewerNoncurrentVersions>
               </NoncurrentVersionExpiration>
       </Rule>
       <Rule>
               <ID>delete-all-versions</ID>
               <Status>Enabled</Status>
               <Filter>
                       <Tag>
                       <Key>del-all</Key>
                       <Value>true</Value>
                       </Tag>
               </Filter>
               <Expiration>
                       <Days>1</Days>
	               <ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>
               </Expiration>
       </Rule>
</LifecycleConfiguration>
`
	lc, err := lifecycle.ParseLifecycleConfig(strings.NewReader(lcXML))
	if err != nil {
		t.Fatalf("Failed to unmarshal lifecycle config: %v", err)
	}

	vcfg := versioning.Versioning{
		Status: "Enabled",
	}
	vcfgXML, err := xml.Marshal(vcfg)
	if err != nil {
		t.Fatalf("Failed to marshal versioning config: %v", err)
	}

	bucket := "bucket"
	now := time.Now()
	meta := BucketMetadata{
		Name:                      bucket,
		Created:                   now,
		LifecycleConfigXML:        []byte(lcXML),
		VersioningConfigXML:       vcfgXML,
		VersioningConfigUpdatedAt: now,
		LifecycleConfigUpdatedAt:  now,
		lifecycleConfig:           lc,
		versioningConfig:          &vcfg,
	}
	globalBucketMetadataSys.Set(bucket, meta)
	// Prepare lifecycle expiration workers
	es := newExpiryState(t.Context(), objAPI, 0)
	globalExpiryState = es

	// Prepare object versions
	obj := "obj-1"
	// Simulate objects uploaded 30 hours ago
	modTime := now.Add(-48 * time.Hour)
	uuids := make([]uuid.UUID, 5)
	for i := range uuids {
		uuids[i] = uuid.UUID([16]byte{15: uint8(i + 1)})
	}
	fivs := make([]FileInfo, 5)
	objInfos := make([]ObjectInfo, 5)
	objRetentionMeta := make(map[string]string)
	objRetentionMeta[strings.ToLower(xhttp.AmzObjectLockMode)] = string(objectlock.RetCompliance)
	// Set retain until date 12 hours into the future
	objRetentionMeta[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)] = amztime.ISO8601Format(now.Add(12 * time.Hour))
	/*
		objInfos:
		version stack for obj-1
		v5 uuid-5 modTime
		v4 uuid-4 modTime -1m
		v3 uuid-3 modTime -2m
		v2 uuid-2 modTime -3m
		v1 uuid-1 modTime -4m
	*/
	for i := range 5 {
		fivs[i] = FileInfo{
			Volume:      bucket,
			Name:        obj,
			VersionID:   uuids[i].String(),
			IsLatest:    i == 0,
			ModTime:     modTime.Add(-1 * time.Duration(i) * time.Minute),
			Size:        1 << 10,
			NumVersions: 5,
		}
		objInfos[i] = fivs[i].ToObjectInfo(bucket, obj, true)
	}
	/*
		lrObjInfos: objInfos with following modifications
		version stack for obj-1
		v2 uuid-2 modTime -3m objRetentionMeta
	*/
	lrObjInfos := slices.Clone(objInfos)
	lrObjInfos[3].UserDefined = objRetentionMeta
	var lrWants []ObjectInfo
	lrWants = append(lrWants, lrObjInfos[:4]...)

	/*
		replObjInfos: objInfos with following modifications
		version stack for obj-1
		v1 uuid-1 modTime -4m	"VersionPurgeStatus: replication.VersionPurgePending"
	*/
	replObjInfos := slices.Clone(objInfos)
	replObjInfos[4].VersionPurgeStatus = replication.VersionPurgePending
	var replWants []ObjectInfo
	replWants = append(replWants, replObjInfos[:3]...)
	replWants = append(replWants, replObjInfos[4])

	allVersExpObjInfos := slices.Clone(objInfos)
	allVersExpObjInfos[0].UserTags = "del-all=true"

	replCfg := replication.Config{
		Rules: []replication.Rule{
			{
				ID:       "",
				Status:   "Enabled",
				Priority: 1,
				Destination: replication.Destination{
					ARN:    "arn:minio:replication:::dest-bucket",
					Bucket: "dest-bucket",
				},
			},
		},
	}
	lr := objectlock.Retention{
		Mode:        objectlock.RetCompliance,
		Validity:    12 * time.Hour,
		LockEnabled: true,
	}

	expiryWorker := func(wg *sync.WaitGroup, readyCh chan<- struct{}, taskCh <-chan expiryOp, gotExpired *[]ObjectToDelete) {
		defer wg.Done()
		// signal the calling goroutine that the worker is ready tor receive tasks
		close(readyCh)
		var expired []ObjectToDelete
		for t := range taskCh {
			switch v := t.(type) {
			case noncurrentVersionsTask:
				expired = append(expired, v.versions...)
			case expiryTask:
				expired = append(expired, ObjectToDelete{
					ObjectV: ObjectV{
						ObjectName: v.objInfo.Name,
						VersionID:  v.objInfo.VersionID,
					},
				})
			}
		}
		if len(expired) > 0 {
			*gotExpired = expired
		}
	}
	tests := []struct {
		replCfg     replicationConfig
		lr          objectlock.Retention
		objInfos    []ObjectInfo
		wants       []ObjectInfo
		wantExpired []ObjectToDelete
	}{
		{
			// With replication configured, version(s) with PENDING purge status
			replCfg:  replicationConfig{Config: &replCfg},
			objInfos: replObjInfos,
			wants:    replWants,
			wantExpired: []ObjectToDelete{
				{ObjectV: ObjectV{ObjectName: obj, VersionID: objInfos[3].VersionID}},
			},
		},
		{
			// With lock retention configured and version(s) with retention metadata
			lr:       lr,
			objInfos: lrObjInfos,
			wants:    lrWants,
			wantExpired: []ObjectToDelete{
				{ObjectV: ObjectV{ObjectName: obj, VersionID: objInfos[4].VersionID}},
			},
		},
		{
			// With replication configured, but no versions with PENDING purge status
			replCfg:  replicationConfig{Config: &replCfg},
			objInfos: objInfos,
			wants:    objInfos[:3],
			wantExpired: []ObjectToDelete{
				{ObjectV: ObjectV{ObjectName: obj, VersionID: objInfos[3].VersionID}},
				{ObjectV: ObjectV{ObjectName: obj, VersionID: objInfos[4].VersionID}},
			},
		},
		{
			objInfos:    allVersExpObjInfos,
			wants:       nil,
			wantExpired: []ObjectToDelete{{ObjectV: ObjectV{ObjectName: obj, VersionID: allVersExpObjInfos[0].VersionID}}},
		},
		{
			// When no versions are present, in practice this could be an object with only free versions
			objInfos:    nil,
			wants:       nil,
			wantExpired: nil,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("TestApplyNewerNoncurrentVersionsLimit-%d", i), func(t *testing.T) {
			workers := []chan expiryOp{make(chan expiryOp)}
			es.workers.Store(&workers)
			workerReady := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			var gotExpired []ObjectToDelete
			go expiryWorker(&wg, workerReady, workers[0], &gotExpired)
			<-workerReady

			item := scannerItem{
				Path:        obj,
				bucket:      bucket,
				prefix:      "",
				objectName:  obj,
				lifeCycle:   lc,
				replication: test.replCfg,
			}

			var (
				sizeS sizeSummary
				gots  []ObjectInfo
			)
			item.applyActions(t.Context(), objAPI, test.objInfos, test.lr, &sizeS, func(oi ObjectInfo, sz, _ int64, _ *sizeSummary) {
				if sz != 0 {
					gots = append(gots, oi)
				}
			})

			if len(gots) != len(test.wants) {
				t.Fatalf("Expected %d objects but got %d", len(test.wants), len(gots))
			}
			if slices.CompareFunc(gots, test.wants, func(g, w ObjectInfo) int {
				if g.VersionID == w.VersionID {
					return 0
				}
				return -1
			}) != 0 {
				t.Fatalf("Expected %v but got %v", test.wants, gots)
			}
			// verify the objects to be deleted
			close(workers[0])
			wg.Wait()
			if len(gotExpired) != len(test.wantExpired) {
				t.Fatalf("Expected expiry of %d objects but got %d", len(test.wantExpired), len(gotExpired))
			}
			if slices.CompareFunc(gotExpired, test.wantExpired, func(g, w ObjectToDelete) int {
				if g.VersionID == w.VersionID {
					return 0
				}
				return -1
			}) != 0 {
				t.Fatalf("Expected %v but got %v", test.wantExpired, gotExpired)
			}
		})
	}
}

func TestEvalActionFromLifecycle(t *testing.T) {
	// Tests cover only ExpiredObjectDeleteAllVersions and DelMarkerExpiration actions
	numVersions := 4
	obj := ObjectInfo{
		Name:        "foo",
		ModTime:     time.Now().Add(-31 * 24 * time.Hour),
		Size:        100 << 20,
		VersionID:   uuid.New().String(),
		IsLatest:    true,
		NumVersions: numVersions,
	}
	delMarker := ObjectInfo{
		Name:         "foo-deleted",
		ModTime:      time.Now().Add(-61 * 24 * time.Hour),
		Size:         0,
		VersionID:    uuid.New().String(),
		IsLatest:     true,
		DeleteMarker: true,
		NumVersions:  numVersions,
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
		retention *objectlock.Retention
		obj       ObjectInfo
		want      lifecycle.Action
	}{
		{
			// with object locking
			ilm:       *deleteAllLc,
			retention: &objectlock.Retention{LockEnabled: true},
			obj:       obj,
			want:      lifecycle.NoneAction,
		},
		{
			// without object locking
			ilm:       *deleteAllLc,
			retention: &objectlock.Retention{},
			obj:       obj,
			want:      lifecycle.DeleteAllVersionsAction,
		},
		{
			// with object locking
			ilm:       *delMarkerLc,
			retention: &objectlock.Retention{LockEnabled: true},
			obj:       delMarker,
			want:      lifecycle.NoneAction,
		},
		{
			// without object locking
			ilm:       *delMarkerLc,
			retention: &objectlock.Retention{},
			obj:       delMarker,
			want:      lifecycle.DelMarkerDeleteAllVersionsAction,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("TestEvalAction-%d", i), func(t *testing.T) {
			gotEvent := evalActionFromLifecycle(t.Context(), test.ilm, *test.retention, nil, test.obj)
			if gotEvent.Action != test.want {
				t.Fatalf("Expected %v but got %v", test.want, gotEvent.Action)
			}
		})
	}
}
