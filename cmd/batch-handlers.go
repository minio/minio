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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/config/batch"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/policy"
	"github.com/minio/pkg/v3/workers"
	"gopkg.in/yaml.v3"
)

var globalBatchConfig batch.Config

const (
	// Keep the completed/failed job stats 3 days before removing it
	oldJobsExpiration = 3 * 24 * time.Hour

	redactedText = "**REDACTED**"
)

// BatchJobRequest this is an internal data structure not for external consumption.
type BatchJobRequest struct {
	ID        string               `yaml:"-" json:"name"`
	User      string               `yaml:"-" json:"user"`
	Started   time.Time            `yaml:"-" json:"started"`
	Replicate *BatchJobReplicateV1 `yaml:"replicate" json:"replicate"`
	KeyRotate *BatchJobKeyRotateV1 `yaml:"keyrotate" json:"keyrotate"`
	Expire    *BatchJobExpire      `yaml:"expire" json:"expire"`
	ctx       context.Context      `msg:"-"`
}

// RedactSensitive will redact any sensitive information in b.
func (j *BatchJobRequest) RedactSensitive() {
	j.Replicate.RedactSensitive()
	j.Expire.RedactSensitive()
	j.KeyRotate.RedactSensitive()
}

// RedactSensitive will redact any sensitive information in b.
func (r *BatchJobReplicateV1) RedactSensitive() {
	if r == nil {
		return
	}
	if r.Target.Creds.SecretKey != "" {
		r.Target.Creds.SecretKey = redactedText
	}
	if r.Target.Creds.SessionToken != "" {
		r.Target.Creds.SessionToken = redactedText
	}
}

// RedactSensitive will redact any sensitive information in b.
func (r *BatchJobKeyRotateV1) RedactSensitive() {}

func notifyEndpoint(ctx context.Context, ri *batchJobInfo, endpoint, token string) error {
	if endpoint == "" {
		return nil
	}

	buf, err := json.Marshal(ri)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(buf))
	if err != nil {
		return err
	}

	if token != "" {
		req.Header.Set("Authorization", token)
	}
	req.Header.Set("Content-Type", "application/json")

	clnt := http.Client{Transport: getRemoteInstanceTransport()}
	resp, err := clnt.Do(req)
	if err != nil {
		return err
	}

	xhttp.DrainBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	return nil
}

// Notify notifies notification endpoint if configured regarding job failure or success.
func (r BatchJobReplicateV1) Notify(ctx context.Context, ri *batchJobInfo) error {
	return notifyEndpoint(ctx, ri, r.Flags.Notify.Endpoint, r.Flags.Notify.Token)
}

// ReplicateFromSource - this is not implemented yet where source is 'remote' and target is local.
func (r *BatchJobReplicateV1) ReplicateFromSource(ctx context.Context, api ObjectLayer, core *minio.Core, srcObjInfo ObjectInfo, retry bool) error {
	srcBucket := r.Source.Bucket
	tgtBucket := r.Target.Bucket
	srcObject := srcObjInfo.Name
	tgtObject := srcObjInfo.Name
	if r.Target.Prefix != "" {
		tgtObject = pathJoin(r.Target.Prefix, srcObjInfo.Name)
	}

	versionID := srcObjInfo.VersionID
	if r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3 {
		versionID = ""
	}
	if srcObjInfo.DeleteMarker {
		_, err := api.DeleteObject(ctx, tgtBucket, tgtObject, ObjectOptions{
			VersionID: versionID,
			// Since we are preserving a delete marker, we have to make sure this is always true.
			// regardless of the current configuration of the bucket we must preserve all versions
			// on the pool being batch replicated from source.
			Versioned:          true,
			MTime:              srcObjInfo.ModTime,
			DeleteMarker:       srcObjInfo.DeleteMarker,
			ReplicationRequest: true,
		})
		return err
	}

	opts := ObjectOptions{
		VersionID:    srcObjInfo.VersionID,
		MTime:        srcObjInfo.ModTime,
		PreserveETag: srcObjInfo.ETag,
		UserDefined:  srcObjInfo.UserDefined,
	}
	if r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3 {
		opts.VersionID = ""
	}
	if crypto.S3.IsEncrypted(srcObjInfo.UserDefined) {
		opts.ServerSideEncryption = encrypt.NewSSE()
	}
	slc := strings.Split(srcObjInfo.ETag, "-")
	if len(slc) == 2 {
		partsCount, err := strconv.Atoi(slc[1])
		if err != nil {
			return err
		}
		return r.copyWithMultipartfromSource(ctx, api, core, srcObjInfo, opts, partsCount)
	}
	gopts := minio.GetObjectOptions{
		VersionID: srcObjInfo.VersionID,
	}
	if err := gopts.SetMatchETag(srcObjInfo.ETag); err != nil {
		return err
	}
	rd, objInfo, _, err := core.GetObject(ctx, srcBucket, srcObject, gopts)
	if err != nil {
		return ErrorRespToObjectError(err, srcBucket, srcObject, srcObjInfo.VersionID)
	}
	defer rd.Close()

	hr, err := hash.NewReader(ctx, rd, objInfo.Size, "", "", objInfo.Size)
	if err != nil {
		return err
	}
	pReader := NewPutObjReader(hr)
	_, err = api.PutObject(ctx, tgtBucket, tgtObject, pReader, opts)
	return err
}

func (r *BatchJobReplicateV1) copyWithMultipartfromSource(ctx context.Context, api ObjectLayer, c *minio.Core, srcObjInfo ObjectInfo, opts ObjectOptions, partsCount int) (err error) {
	srcBucket := r.Source.Bucket
	tgtBucket := r.Target.Bucket
	srcObject := srcObjInfo.Name
	tgtObject := srcObjInfo.Name
	if r.Target.Prefix != "" {
		tgtObject = pathJoin(r.Target.Prefix, srcObjInfo.Name)
	}
	if r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3 {
		opts.VersionID = ""
	}
	var uploadedParts []CompletePart
	res, err := api.NewMultipartUpload(context.Background(), tgtBucket, tgtObject, opts)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			// block and abort remote upload upon failure.
			attempts := 1
			for attempts <= 3 {
				aerr := api.AbortMultipartUpload(ctx, tgtBucket, tgtObject, res.UploadID, ObjectOptions{})
				if aerr == nil {
					return
				}
				batchLogIf(ctx,
					fmt.Errorf("trying %s: Unable to cleanup failed multipart replication %s on remote %s/%s: %w - this may consume space on remote cluster",
						humanize.Ordinal(attempts), res.UploadID, tgtBucket, tgtObject, aerr))
				attempts++
				time.Sleep(time.Second)
			}
		}
	}()

	var (
		hr    *hash.Reader
		pInfo PartInfo
	)

	for i := range partsCount {
		gopts := minio.GetObjectOptions{
			VersionID:  srcObjInfo.VersionID,
			PartNumber: i + 1,
		}
		if err := gopts.SetMatchETag(srcObjInfo.ETag); err != nil {
			return err
		}
		rd, objInfo, _, err := c.GetObject(ctx, srcBucket, srcObject, gopts)
		if err != nil {
			return ErrorRespToObjectError(err, srcBucket, srcObject, srcObjInfo.VersionID)
		}
		defer rd.Close()

		hr, err = hash.NewReader(ctx, io.LimitReader(rd, objInfo.Size), objInfo.Size, "", "", objInfo.Size)
		if err != nil {
			return err
		}
		pReader := NewPutObjReader(hr)
		opts.PreserveETag = ""
		pInfo, err = api.PutObjectPart(ctx, tgtBucket, tgtObject, res.UploadID, i+1, pReader, opts)
		if err != nil {
			return err
		}
		if pInfo.Size != objInfo.Size {
			return fmt.Errorf("Part size mismatch: got %d, want %d", pInfo.Size, objInfo.Size)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}
	_, err = api.CompleteMultipartUpload(ctx, tgtBucket, tgtObject, res.UploadID, uploadedParts, opts)
	return err
}

// StartFromSource starts the batch replication job from remote source, resumes if there was a pending job via "job.ID"
func (r *BatchJobReplicateV1) StartFromSource(ctx context.Context, api ObjectLayer, job BatchJobRequest) error {
	ri := &batchJobInfo{
		JobID:     job.ID,
		JobType:   string(job.Type()),
		StartTime: job.Started,
	}
	if err := ri.loadOrInit(ctx, api, job); err != nil {
		return err
	}
	if ri.Complete {
		return nil
	}
	globalBatchJobsMetrics.save(job.ID, ri)

	retryAttempts := job.Replicate.Flags.Retry.Attempts
	if retryAttempts <= 0 {
		retryAttempts = batchReplJobDefaultRetries
	}
	delay := job.Replicate.Flags.Retry.Delay
	if delay <= 0 {
		delay = batchReplJobDefaultRetryDelay
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	hasTags := len(r.Flags.Filter.Tags) != 0
	isMetadata := len(r.Flags.Filter.Metadata) != 0
	isStorageClassOnly := len(r.Flags.Filter.Metadata) == 1 && strings.EqualFold(r.Flags.Filter.Metadata[0].Key, xhttp.AmzStorageClass)

	skip := func(oi ObjectInfo) (ok bool) {
		if r.Flags.Filter.OlderThan > 0 && time.Since(oi.ModTime) < r.Flags.Filter.OlderThan.D() {
			// skip all objects that are newer than specified older duration
			return true
		}

		if r.Flags.Filter.NewerThan > 0 && time.Since(oi.ModTime) >= r.Flags.Filter.NewerThan.D() {
			// skip all objects that are older than specified newer duration
			return true
		}

		if !r.Flags.Filter.CreatedAfter.IsZero() && r.Flags.Filter.CreatedAfter.After(oi.ModTime) {
			// skip all objects that are created before the specified time.
			return true
		}

		if !r.Flags.Filter.CreatedBefore.IsZero() && r.Flags.Filter.CreatedBefore.Before(oi.ModTime) {
			// skip all objects that are created after the specified time.
			return true
		}

		if hasTags {
			// Only parse object tags if tags filter is specified.
			tagMap := map[string]string{}
			tagStr := oi.UserTags
			if len(tagStr) != 0 {
				t, err := tags.ParseObjectTags(tagStr)
				if err != nil {
					return false
				}
				tagMap = t.ToMap()
			}
			for _, kv := range r.Flags.Filter.Tags {
				for t, v := range tagMap {
					if kv.Match(BatchJobKV{Key: t, Value: v}) {
						return true
					}
				}
			}

			// None of the provided tags filter match skip the object
			return false
		}

		for _, kv := range r.Flags.Filter.Metadata {
			for k, v := range oi.UserDefined {
				if !stringsHasPrefixFold(k, "x-amz-meta-") && !isStandardHeader(k) {
					continue
				}
				// We only need to match x-amz-meta or standardHeaders
				if kv.Match(BatchJobKV{Key: k, Value: v}) {
					return true
				}
			}
		}

		// None of the provided filters match
		return false
	}

	u, err := url.Parse(r.Source.Endpoint)
	if err != nil {
		return err
	}

	cred := r.Source.Creds

	c, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:       u.Scheme == "https",
		Transport:    getRemoteInstanceTransport(),
		BucketLookup: lookupStyle(r.Source.Path),
	})
	if err != nil {
		return err
	}

	c.SetAppInfo("minio-"+batchJobPrefix, r.APIVersion+" "+job.ID)
	core := &minio.Core{Client: c}

	workerSize, err := strconv.Atoi(env.Get("_MINIO_BATCH_REPLICATION_WORKERS", strconv.Itoa(runtime.GOMAXPROCS(0)/2)))
	if err != nil {
		return err
	}

	wk, err := workers.New(workerSize)
	if err != nil {
		// invalid worker size.
		return err
	}

	retry := false
	for attempts := 1; attempts <= retryAttempts; attempts++ {
		attempts := attempts
		// one of source/target is s3, skip delete marker and all versions under the same object name.
		s3Type := r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3
		minioSrc := r.Source.Type == BatchJobReplicateResourceMinIO
		ctx, cancel := context.WithCancel(ctx)

		objInfoCh := make(chan minio.ObjectInfo, 1)
		go func() {
			prefixes := r.Source.Prefix.F()
			if len(prefixes) == 0 {
				prefixes = []string{""}
			}
			for _, prefix := range prefixes {
				prefixObjInfoCh := c.ListObjects(ctx, r.Source.Bucket, minio.ListObjectsOptions{
					Prefix:       prefix,
					WithVersions: minioSrc,
					Recursive:    true,
					WithMetadata: true,
				})
				for obj := range prefixObjInfoCh {
					objInfoCh <- obj
				}
			}
			xioutil.SafeClose(objInfoCh)
		}()

		prevObj := ""
		skipReplicate := false

		for obj := range objInfoCh {
			oi := toObjectInfo(r.Source.Bucket, obj.Key, obj)
			if !minioSrc {
				// Check if metadata filter was requested and it is expected to have
				// all user metadata or just storageClass. If its only storageClass
				// List() already returns relevant information for filter to be applied.
				if isMetadata && !isStorageClassOnly {
					oi2, err := c.StatObject(ctx, r.Source.Bucket, obj.Key, minio.StatObjectOptions{})
					if err == nil {
						oi = toObjectInfo(r.Source.Bucket, obj.Key, oi2)
					} else {
						if !isErrMethodNotAllowed(ErrorRespToObjectError(err, r.Source.Bucket, obj.Key)) &&
							!isErrObjectNotFound(ErrorRespToObjectError(err, r.Source.Bucket, obj.Key)) {
							batchLogIf(ctx, err)
						}
						continue
					}
				}
				if hasTags {
					tags, err := c.GetObjectTagging(ctx, r.Source.Bucket, obj.Key, minio.GetObjectTaggingOptions{})
					if err == nil {
						oi.UserTags = tags.String()
					} else {
						if !isErrMethodNotAllowed(ErrorRespToObjectError(err, r.Source.Bucket, obj.Key)) &&
							!isErrObjectNotFound(ErrorRespToObjectError(err, r.Source.Bucket, obj.Key)) {
							batchLogIf(ctx, err)
						}
						continue
					}
				}
			}
			if skip(oi) {
				continue
			}
			if obj.Key != prevObj {
				prevObj = obj.Key
				// skip replication of delete marker and all versions under the same object name if one of source or target is s3.
				skipReplicate = obj.IsDeleteMarker && s3Type
			}
			if skipReplicate {
				continue
			}

			wk.Take()
			go func() {
				defer wk.Give()
				stopFn := globalBatchJobsMetrics.trace(batchJobMetricReplication, job.ID, attempts)
				success := true
				if err := r.ReplicateFromSource(ctx, api, core, oi, retry); err != nil {
					// object must be deleted concurrently, allow these failures but do not count them
					if isErrVersionNotFound(err) || isErrObjectNotFound(err) {
						return
					}
					stopFn(oi, err)
					batchLogIf(ctx, err)
					success = false
				} else {
					stopFn(oi, nil)
				}
				ri.trackCurrentBucketObject(r.Target.Bucket, oi, success, attempts)
				globalBatchJobsMetrics.save(job.ID, ri)
				// persist in-memory state to disk after every 10secs.
				batchLogIf(ctx, ri.updateAfter(ctx, api, 10*time.Second, job))

				if wait := globalBatchConfig.ReplicationWait(); wait > 0 {
					time.Sleep(wait)
				}
			}()
		}
		wk.Wait()

		ri.RetryAttempts = attempts
		ri.Complete = ri.ObjectsFailed == 0
		ri.Failed = ri.ObjectsFailed > 0

		globalBatchJobsMetrics.save(job.ID, ri)
		// persist in-memory state to disk.
		batchLogIf(ctx, ri.updateAfter(ctx, api, 0, job))

		if err := r.Notify(ctx, ri); err != nil {
			batchLogIf(ctx, fmt.Errorf("unable to notify %v", err))
		}

		cancel()
		if ri.Failed {
			ri.ObjectsFailed = 0
			ri.Bucket = ""
			ri.Object = ""
			ri.Objects = 0
			ri.BytesFailed = 0
			ri.BytesTransferred = 0
			retry = true // indicate we are retrying..
			time.Sleep(delay + time.Duration(rnd.Float64()*float64(delay)))
			continue
		}

		break
	}

	return nil
}

// toObjectInfo converts minio.ObjectInfo to ObjectInfo
func toObjectInfo(bucket, object string, objInfo minio.ObjectInfo) ObjectInfo {
	tags, _ := tags.MapToObjectTags(objInfo.UserTags)
	oi := ObjectInfo{
		Bucket:                    bucket,
		Name:                      object,
		ModTime:                   objInfo.LastModified,
		Size:                      objInfo.Size,
		ETag:                      objInfo.ETag,
		VersionID:                 objInfo.VersionID,
		IsLatest:                  objInfo.IsLatest,
		DeleteMarker:              objInfo.IsDeleteMarker,
		ContentType:               objInfo.ContentType,
		Expires:                   objInfo.Expires,
		StorageClass:              objInfo.StorageClass,
		ReplicationStatusInternal: objInfo.ReplicationStatus,
		UserTags:                  tags.String(),
	}

	oi.UserDefined = make(map[string]string, len(objInfo.Metadata))
	for k, v := range objInfo.Metadata {
		oi.UserDefined[k] = v[0]
	}

	ce, ok := oi.UserDefined[xhttp.ContentEncoding]
	if !ok {
		ce, ok = oi.UserDefined[strings.ToLower(xhttp.ContentEncoding)]
	}
	if ok {
		oi.ContentEncoding = ce
	}

	_, ok = oi.UserDefined[xhttp.AmzStorageClass]
	if !ok {
		oi.UserDefined[xhttp.AmzStorageClass] = objInfo.StorageClass
	}

	maps.Copy(oi.UserDefined, objInfo.UserMetadata)

	return oi
}

func (r BatchJobReplicateV1) writeAsArchive(ctx context.Context, objAPI ObjectLayer, remoteClnt *minio.Client, entries []ObjectInfo, prefix string) error {
	input := make(chan minio.SnowballObject, 1)
	opts := minio.SnowballOptions{
		Opts:     minio.PutObjectOptions{},
		InMemory: *r.Source.Snowball.InMemory,
		Compress: *r.Source.Snowball.Compress,
		SkipErrs: *r.Source.Snowball.SkipErrs,
	}

	go func() {
		defer xioutil.SafeClose(input)

		for _, entry := range entries {
			gr, err := objAPI.GetObjectNInfo(ctx, r.Source.Bucket,
				entry.Name, nil, nil, ObjectOptions{
					VersionID: entry.VersionID,
				})
			if err != nil {
				batchLogIf(ctx, err)
				continue
			}

			if prefix != "" {
				entry.Name = pathJoin(prefix, entry.Name)
			}

			snowballObj := minio.SnowballObject{
				// Create path to store objects within the bucket.
				Key:       entry.Name,
				Size:      entry.Size,
				ModTime:   entry.ModTime,
				VersionID: entry.VersionID,
				Content:   gr,
				Headers:   make(http.Header),
				Close: func() {
					gr.Close()
				},
			}

			opts, _, err := batchReplicationOpts(ctx, "", gr.ObjInfo)
			if err != nil {
				batchLogIf(ctx, err)
				continue
			}
			// TODO: I am not sure we read it back, but we aren't sending whether checksums are single/multipart.
			for k, vals := range opts.Header() {
				for _, v := range vals {
					snowballObj.Headers.Add(k, v)
				}
			}

			input <- snowballObj
		}
	}()

	// Collect and upload all entries.
	return remoteClnt.PutObjectsSnowball(ctx, r.Target.Bucket, opts, input)
}

// ReplicateToTarget read from source and replicate to configured target
func (r *BatchJobReplicateV1) ReplicateToTarget(ctx context.Context, api ObjectLayer, c *minio.Core, srcObjInfo ObjectInfo, retry bool) error {
	srcBucket := r.Source.Bucket
	tgtBucket := r.Target.Bucket
	tgtPrefix := r.Target.Prefix
	srcObject := srcObjInfo.Name
	s3Type := r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3

	if srcObjInfo.DeleteMarker || !srcObjInfo.VersionPurgeStatus.Empty() {
		if retry && !s3Type {
			if _, err := c.StatObject(ctx, tgtBucket, pathJoin(tgtPrefix, srcObject), minio.StatObjectOptions{
				VersionID: srcObjInfo.VersionID,
				Internal: minio.AdvancedGetOptions{
					ReplicationProxyRequest: "false",
				},
			}); isErrMethodNotAllowed(ErrorRespToObjectError(err, tgtBucket, pathJoin(tgtPrefix, srcObject))) {
				return nil
			}
		}

		versionID := srcObjInfo.VersionID
		dmVersionID := ""
		if srcObjInfo.VersionPurgeStatus.Empty() {
			dmVersionID = srcObjInfo.VersionID
		}
		if r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3 {
			dmVersionID = ""
			versionID = ""
		}
		return c.RemoveObject(ctx, tgtBucket, pathJoin(tgtPrefix, srcObject), minio.RemoveObjectOptions{
			VersionID: versionID,
			Internal: minio.AdvancedRemoveOptions{
				ReplicationDeleteMarker: dmVersionID != "",
				ReplicationMTime:        srcObjInfo.ModTime,
				ReplicationStatus:       minio.ReplicationStatusReplica,
				ReplicationRequest:      true, // always set this to distinguish between `mc mirror` replication and serverside
			},
		})
	}

	if retry && !s3Type { // when we are retrying avoid copying if necessary.
		gopts := minio.GetObjectOptions{}
		if err := gopts.SetMatchETag(srcObjInfo.ETag); err != nil {
			return err
		}
		if _, err := c.StatObject(ctx, tgtBucket, pathJoin(tgtPrefix, srcObject), gopts); err == nil {
			return nil
		}
	}

	versioned := globalBucketVersioningSys.PrefixEnabled(srcBucket, srcObject)
	versionSuspended := globalBucketVersioningSys.PrefixSuspended(srcBucket, srcObject)

	opts := ObjectOptions{
		VersionID:        srcObjInfo.VersionID,
		Versioned:        versioned,
		VersionSuspended: versionSuspended,
	}
	rd, err := api.GetObjectNInfo(ctx, srcBucket, srcObject, nil, http.Header{}, opts)
	if err != nil {
		return err
	}
	defer rd.Close()
	objInfo := rd.ObjInfo

	size, err := objInfo.GetActualSize()
	if err != nil {
		return err
	}

	putOpts, isMP, err := batchReplicationOpts(ctx, "", objInfo)
	if err != nil {
		return err
	}
	if r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3 {
		putOpts.Internal = minio.AdvancedPutOptions{}
	}
	if isMP {
		if err := replicateObjectWithMultipart(ctx, c, tgtBucket, pathJoin(tgtPrefix, objInfo.Name), rd, objInfo, putOpts); err != nil {
			return err
		}
	} else {
		if _, err = c.PutObject(ctx, tgtBucket, pathJoin(tgtPrefix, objInfo.Name), rd, size, "", "", putOpts); err != nil {
			return err
		}
	}
	return nil
}

//go:generate msgp -file $GOFILE -unexported

// batchJobInfo current batch replication information
type batchJobInfo struct {
	mu sync.RWMutex `json:"-" msg:"-"`

	Version       int       `json:"-" msg:"v"`
	JobID         string    `json:"jobID" msg:"jid"`
	JobType       string    `json:"jobType" msg:"jt"`
	StartTime     time.Time `json:"startTime" msg:"st"`
	LastUpdate    time.Time `json:"lastUpdate" msg:"lu"`
	RetryAttempts int       `json:"retryAttempts" msg:"ra"`
	Attempts      int       `json:"attempts" msg:"at"`

	Complete bool `json:"complete" msg:"cmp"`
	Failed   bool `json:"failed" msg:"fld"`

	// Last bucket/object batch replicated
	Bucket string `json:"-" msg:"lbkt"`
	Object string `json:"-" msg:"lobj"`

	// Verbose information
	Objects             int64 `json:"objects" msg:"ob"`
	DeleteMarkers       int64 `json:"deleteMarkers" msg:"dm"`
	ObjectsFailed       int64 `json:"objectsFailed" msg:"obf"`
	DeleteMarkersFailed int64 `json:"deleteMarkersFailed" msg:"dmf"`
	BytesTransferred    int64 `json:"bytesTransferred" msg:"bt"`
	BytesFailed         int64 `json:"bytesFailed" msg:"bf"`
}

const (
	batchReplName         = "batch-replicate.bin"
	batchReplFormat       = 1
	batchReplVersionV1    = 1
	batchReplVersion      = batchReplVersionV1
	batchJobName          = "job.bin"
	batchJobPrefix        = "batch-jobs"
	batchJobReportsPrefix = batchJobPrefix + "/reports"

	batchReplJobAPIVersion        = "v1"
	batchReplJobDefaultRetries    = 3
	batchReplJobDefaultRetryDelay = time.Second
)

func getJobPath(job BatchJobRequest) string {
	return pathJoin(batchJobPrefix, job.ID)
}

func (ri *batchJobInfo) getJobReportPath() (string, error) {
	var fileName string
	switch madmin.BatchJobType(ri.JobType) {
	case madmin.BatchJobReplicate:
		fileName = batchReplName
	case madmin.BatchJobKeyRotate:
		fileName = batchKeyRotationName
	case madmin.BatchJobExpire:
		fileName = batchExpireName
	default:
		return "", fmt.Errorf("unknown job type: %v", ri.JobType)
	}
	return pathJoin(batchJobReportsPrefix, ri.JobID, fileName), nil
}

func (ri *batchJobInfo) loadOrInit(ctx context.Context, api ObjectLayer, job BatchJobRequest) error {
	err := ri.load(ctx, api, job)
	if errors.Is(err, errNoSuchJob) {
		switch {
		case job.Replicate != nil:
			ri.Version = batchReplVersionV1
		case job.KeyRotate != nil:
			ri.Version = batchKeyRotateVersionV1
		case job.Expire != nil:
			ri.Version = batchExpireVersionV1
		}
		return nil
	}
	return err
}

func (ri *batchJobInfo) load(ctx context.Context, api ObjectLayer, job BatchJobRequest) error {
	path, err := job.getJobReportPath()
	if err != nil {
		batchLogIf(ctx, err)
		return err
	}
	return ri.loadByPath(ctx, api, path)
}

func (ri *batchJobInfo) loadByPath(ctx context.Context, api ObjectLayer, path string) error {
	var format, version uint16
	switch filepath.Base(path) {
	case batchReplName:
		version = batchReplVersionV1
		format = batchReplFormat
	case batchKeyRotationName:
		version = batchKeyRotateVersionV1
		format = batchKeyRotationFormat
	case batchExpireName:
		version = batchExpireVersionV1
		format = batchExpireFormat
	default:
		return errors.New("no supported batch job request specified")
	}

	data, err := readConfig(ctx, api, path)
	if err != nil {
		if errors.Is(err, errConfigNotFound) || isErrObjectNotFound(err) {
			return errNoSuchJob
		}
		return err
	}
	if len(data) == 0 {
		// Seems to be empty create a new batchRepl object.
		return nil
	}
	if len(data) <= 4 {
		return fmt.Errorf("%s: no data", ri.JobType)
	}
	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case format:
	default:
		return fmt.Errorf("%s: unknown format: %d", ri.JobType, binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case version:
	default:
		return fmt.Errorf("%s: unknown version: %d", ri.JobType, binary.LittleEndian.Uint16(data[2:4]))
	}

	ri.mu.Lock()
	defer ri.mu.Unlock()

	// OK, parse data.
	if _, err = ri.UnmarshalMsg(data[4:]); err != nil {
		return err
	}

	switch ri.Version {
	case batchReplVersionV1:
	default:
		return fmt.Errorf("unexpected batch %s meta version: %d", ri.JobType, ri.Version)
	}

	return nil
}

func (ri *batchJobInfo) clone() *batchJobInfo {
	ri.mu.RLock()
	defer ri.mu.RUnlock()

	return &batchJobInfo{
		Version:             ri.Version,
		JobID:               ri.JobID,
		JobType:             ri.JobType,
		RetryAttempts:       ri.RetryAttempts,
		Complete:            ri.Complete,
		Failed:              ri.Failed,
		StartTime:           ri.StartTime,
		LastUpdate:          ri.LastUpdate,
		Bucket:              ri.Bucket,
		Object:              ri.Object,
		Objects:             ri.Objects,
		ObjectsFailed:       ri.ObjectsFailed,
		DeleteMarkers:       ri.DeleteMarkers,
		DeleteMarkersFailed: ri.DeleteMarkersFailed,
		BytesTransferred:    ri.BytesTransferred,
		BytesFailed:         ri.BytesFailed,
		Attempts:            ri.Attempts,
	}
}

func (ri *batchJobInfo) countItem(size int64, dmarker, success bool, attempt int) {
	if ri == nil {
		return
	}
	ri.Attempts++
	if success {
		if dmarker {
			ri.DeleteMarkers++
		} else {
			ri.Objects++
			ri.BytesTransferred += size
		}
		if attempt > 1 {
			if dmarker {
				ri.DeleteMarkersFailed--
			} else {
				ri.ObjectsFailed--
				ri.BytesFailed += size
			}
		}
	} else {
		if attempt > 1 {
			// Only count first attempt
			return
		}
		if dmarker {
			ri.DeleteMarkersFailed++
		} else {
			ri.ObjectsFailed++
			ri.BytesFailed += size
		}
	}
}

func (ri *batchJobInfo) updateAfter(ctx context.Context, api ObjectLayer, duration time.Duration, job BatchJobRequest) error {
	if ri == nil {
		return errInvalidArgument
	}
	now := UTCNow()
	ri.mu.Lock()
	var (
		format, version uint16
		jobTyp          string
	)

	if now.Sub(ri.LastUpdate) >= duration {
		switch job.Type() {
		case madmin.BatchJobReplicate:
			format = batchReplFormat
			version = batchReplVersion
			jobTyp = string(job.Type())
			ri.Version = batchReplVersionV1
		case madmin.BatchJobKeyRotate:
			format = batchKeyRotationFormat
			version = batchKeyRotateVersion
			jobTyp = string(job.Type())
			ri.Version = batchKeyRotateVersionV1
		case madmin.BatchJobExpire:
			format = batchExpireFormat
			version = batchExpireVersion
			jobTyp = string(job.Type())
			ri.Version = batchExpireVersionV1
		default:
			return errInvalidArgument
		}
		if serverDebugLog {
			console.Debugf("%s: persisting info on drive: threshold:%s, %s:%#v\n", jobTyp, now.Sub(ri.LastUpdate), jobTyp, ri)
		}
		ri.LastUpdate = now

		data := make([]byte, 4, ri.Msgsize()+4)

		// Initialize the header.
		binary.LittleEndian.PutUint16(data[0:2], format)
		binary.LittleEndian.PutUint16(data[2:4], version)

		buf, err := ri.MarshalMsg(data)
		ri.mu.Unlock()
		if err != nil {
			return err
		}
		path, err := ri.getJobReportPath()
		if err != nil {
			batchLogIf(ctx, err)
			return err
		}
		return saveConfig(ctx, api, path, buf)
	}
	ri.mu.Unlock()
	return nil
}

// Note: to be used only with batch jobs that affect multiple versions through
// a single action. e.g batch-expire has an option to expire all versions of an
// object which matches the given filters.
func (ri *batchJobInfo) trackMultipleObjectVersions(info expireObjInfo, success bool) {
	if ri == nil {
		return
	}

	ri.mu.Lock()
	defer ri.mu.Unlock()

	if success {
		ri.Bucket = info.Bucket
		ri.Object = info.Name
		ri.Objects += int64(info.NumVersions) - info.DeleteMarkerCount
		ri.DeleteMarkers += info.DeleteMarkerCount
	} else {
		ri.ObjectsFailed += int64(info.NumVersions) - info.DeleteMarkerCount
		ri.DeleteMarkersFailed += info.DeleteMarkerCount
	}
}

func (ri *batchJobInfo) trackCurrentBucketObject(bucket string, info ObjectInfo, success bool, attempt int) {
	if ri == nil {
		return
	}

	ri.mu.Lock()
	defer ri.mu.Unlock()

	if success {
		ri.Bucket = bucket
		ri.Object = info.Name
	}
	ri.countItem(info.Size, info.DeleteMarker, success, attempt)
}

func (ri *batchJobInfo) trackCurrentBucketBatch(bucket string, batch []ObjectInfo) {
	if ri == nil {
		return
	}

	ri.mu.Lock()
	defer ri.mu.Unlock()

	ri.Bucket = bucket
	for i := range batch {
		ri.Object = batch[i].Name
		ri.countItem(batch[i].Size, batch[i].DeleteMarker, true, 1)
	}
}

// Start start the batch replication job, resumes if there was a pending job via "job.ID"
func (r *BatchJobReplicateV1) Start(ctx context.Context, api ObjectLayer, job BatchJobRequest) error {
	ri := &batchJobInfo{
		JobID:     job.ID,
		JobType:   string(job.Type()),
		StartTime: job.Started,
	}
	if err := ri.loadOrInit(ctx, api, job); err != nil {
		return err
	}
	if ri.Complete {
		return nil
	}
	globalBatchJobsMetrics.save(job.ID, ri)
	lastObject := ri.Object

	retryAttempts := job.Replicate.Flags.Retry.Attempts
	if retryAttempts <= 0 {
		retryAttempts = batchReplJobDefaultRetries
	}
	delay := job.Replicate.Flags.Retry.Delay
	if delay <= 0 {
		delay = batchReplJobDefaultRetryDelay
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	selectObj := func(info FileInfo) (ok bool) {
		if r.Flags.Filter.OlderThan > 0 && time.Since(info.ModTime) < r.Flags.Filter.OlderThan.D() {
			// skip all objects that are newer than specified older duration
			return false
		}

		if r.Flags.Filter.NewerThan > 0 && time.Since(info.ModTime) >= r.Flags.Filter.NewerThan.D() {
			// skip all objects that are older than specified newer duration
			return false
		}

		if !r.Flags.Filter.CreatedAfter.IsZero() && r.Flags.Filter.CreatedAfter.After(info.ModTime) {
			// skip all objects that are created before the specified time.
			return false
		}

		if !r.Flags.Filter.CreatedBefore.IsZero() && r.Flags.Filter.CreatedBefore.Before(info.ModTime) {
			// skip all objects that are created after the specified time.
			return false
		}

		if len(r.Flags.Filter.Tags) > 0 {
			// Only parse object tags if tags filter is specified.
			tagMap := map[string]string{}
			tagStr := info.Metadata[xhttp.AmzObjectTagging]
			if len(tagStr) != 0 {
				t, err := tags.ParseObjectTags(tagStr)
				if err != nil {
					return false
				}
				tagMap = t.ToMap()
			}

			for _, kv := range r.Flags.Filter.Tags {
				for t, v := range tagMap {
					if kv.Match(BatchJobKV{Key: t, Value: v}) {
						return true
					}
				}
			}

			// None of the provided tags filter match skip the object
			return false
		}

		if len(r.Flags.Filter.Metadata) > 0 {
			for _, kv := range r.Flags.Filter.Metadata {
				for k, v := range info.Metadata {
					if !stringsHasPrefixFold(k, "x-amz-meta-") && !isStandardHeader(k) {
						continue
					}
					// We only need to match x-amz-meta or standardHeaders
					if kv.Match(BatchJobKV{Key: k, Value: v}) {
						return true
					}
				}
			}

			// None of the provided metadata filters match skip the object.
			return false
		}

		// if one of source or target is non MinIO, just replicate the top most version like `mc mirror`
		isSourceOrTargetS3 := r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3
		return !isSourceOrTargetS3 || info.IsLatest
	}

	u, err := url.Parse(r.Target.Endpoint)
	if err != nil {
		return err
	}

	cred := r.Target.Creds

	c, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:       u.Scheme == "https",
		Transport:    getRemoteInstanceTransport(),
		BucketLookup: lookupStyle(r.Target.Path),
	})
	if err != nil {
		return err
	}

	c.SetAppInfo("minio-"+batchJobPrefix, r.APIVersion+" "+job.ID)

	retry := false
	for attempts := 1; attempts <= retryAttempts; attempts++ {
		attempts := attempts
		var (
			walkCh = make(chan itemOrErr[ObjectInfo], 100)
			slowCh = make(chan itemOrErr[ObjectInfo], 100)
		)

		if r.Source.Snowball.Disable != nil && !*r.Source.Snowball.Disable && r.Source.Type.isMinio() && r.Target.Type.isMinio() {
			go func() {
				// Snowball currently needs the high level minio-go Client, not the Core one
				cl, err := minio.New(u.Host, &minio.Options{
					Creds:        credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
					Secure:       u.Scheme == "https",
					Transport:    getRemoteInstanceTransport(),
					BucketLookup: lookupStyle(r.Target.Path),
				})
				if err != nil {
					batchLogOnceIf(ctx, err, job.ID+"minio.New")
					return
				}

				// Already validated before arriving here
				smallerThan, _ := humanize.ParseBytes(*r.Source.Snowball.SmallerThan)

				batch := make([]ObjectInfo, 0, *r.Source.Snowball.Batch)
				writeFn := func(batch []ObjectInfo) {
					if len(batch) > 0 {
						if err := r.writeAsArchive(ctx, api, cl, batch, r.Target.Prefix); err != nil {
							batchLogOnceIf(ctx, err, job.ID+"writeAsArchive")
							for _, b := range batch {
								slowCh <- itemOrErr[ObjectInfo]{Item: b}
							}
						} else {
							ri.trackCurrentBucketBatch(r.Source.Bucket, batch)
							globalBatchJobsMetrics.save(job.ID, ri)
							// persist in-memory state to disk after every 10secs.
							batchLogOnceIf(ctx, ri.updateAfter(ctx, api, 10*time.Second, job), job.ID+"updateAfter")
						}
					}
				}
				for obj := range walkCh {
					if obj.Item.DeleteMarker || !obj.Item.VersionPurgeStatus.Empty() || obj.Item.Size >= int64(smallerThan) {
						slowCh <- obj
						continue
					}

					batch = append(batch, obj.Item)

					if len(batch) < *r.Source.Snowball.Batch {
						continue
					}
					writeFn(batch)
					batch = batch[:0]
				}
				writeFn(batch)
				xioutil.SafeClose(slowCh)
			}()
		} else {
			slowCh = walkCh
		}

		workerSize, err := strconv.Atoi(env.Get("_MINIO_BATCH_REPLICATION_WORKERS", strconv.Itoa(runtime.GOMAXPROCS(0)/2)))
		if err != nil {
			return err
		}

		wk, err := workers.New(workerSize)
		if err != nil {
			// invalid worker size.
			return err
		}

		walkQuorum := env.Get("_MINIO_BATCH_REPLICATION_WALK_QUORUM", "strict")
		if walkQuorum == "" {
			walkQuorum = "strict"
		}
		ctx, cancelCause := context.WithCancelCause(ctx)
		// one of source/target is s3, skip delete marker and all versions under the same object name.
		s3Type := r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3

		go func() {
			prefixes := r.Source.Prefix.F()
			if len(prefixes) == 0 {
				prefixes = []string{""}
			}
			for _, prefix := range prefixes {
				prefixWalkCh := make(chan itemOrErr[ObjectInfo], 100)
				if err := api.Walk(ctx, r.Source.Bucket, prefix, prefixWalkCh, WalkOptions{
					Marker:   lastObject,
					Filter:   selectObj,
					AskDisks: walkQuorum,
				}); err != nil {
					cancelCause(err)
					xioutil.SafeClose(walkCh)
					return
				}
				for obj := range prefixWalkCh {
					walkCh <- obj
				}
			}
			xioutil.SafeClose(walkCh)
		}()

		prevObj := ""

		skipReplicate := false
		for res := range slowCh {
			if res.Err != nil {
				ri.Failed = true
				batchLogOnceIf(ctx, res.Err, job.ID+"res.Err")
				continue
			}
			result := res.Item
			if result.Name != prevObj {
				prevObj = result.Name
				skipReplicate = result.DeleteMarker && s3Type
			}
			if skipReplicate {
				continue
			}
			wk.Take()
			go func() {
				defer wk.Give()

				stopFn := globalBatchJobsMetrics.trace(batchJobMetricReplication, job.ID, attempts)
				success := true
				if err := r.ReplicateToTarget(ctx, api, c, result, retry); err != nil {
					if minio.ToErrorResponse(err).Code == "PreconditionFailed" {
						// pre-condition failed means we already have the object copied over.
						return
					}
					// object must be deleted concurrently, allow these failures but do not count them
					if isErrVersionNotFound(err) || isErrObjectNotFound(err) {
						return
					}
					stopFn(result, err)
					batchLogOnceIf(ctx, err, job.ID+"ReplicateToTarget")
					success = false
				} else {
					stopFn(result, nil)
				}
				ri.trackCurrentBucketObject(r.Source.Bucket, result, success, attempts)
				globalBatchJobsMetrics.save(job.ID, ri)
				// persist in-memory state to disk after every 10secs.
				batchLogOnceIf(ctx, ri.updateAfter(ctx, api, 10*time.Second, job), job.ID+"updateAfter2")

				if wait := globalBatchConfig.ReplicationWait(); wait > 0 {
					time.Sleep(wait)
				}
			}()
		}
		wk.Wait()
		// Do not need to retry if we can't list objects on source.
		if context.Cause(ctx) != nil {
			return context.Cause(ctx)
		}
		ri.RetryAttempts = attempts
		ri.Complete = ri.ObjectsFailed == 0
		ri.Failed = ri.ObjectsFailed > 0

		globalBatchJobsMetrics.save(job.ID, ri)
		// persist in-memory state to disk.
		batchLogOnceIf(ctx, ri.updateAfter(ctx, api, 0, job), job.ID+"updateAfter3")

		if err := r.Notify(ctx, ri); err != nil {
			batchLogOnceIf(ctx, fmt.Errorf("unable to notify %v", err), job.ID+"notify")
		}

		cancelCause(nil)
		if ri.Failed {
			ri.ObjectsFailed = 0
			ri.Bucket = ""
			ri.Object = ""
			ri.Objects = 0
			ri.BytesFailed = 0
			ri.BytesTransferred = 0
			retry = true // indicate we are retrying..
			time.Sleep(delay + time.Duration(rnd.Float64()*float64(delay)))
			continue
		}

		break
	}

	return nil
}

//msgp:ignore batchReplicationJobError
type batchReplicationJobError struct {
	Code           string
	Description    string
	HTTPStatusCode int
	ObjectSize     int64
}

func (e batchReplicationJobError) Error() string {
	return e.Description
}

// Validate validates the job definition input
func (r *BatchJobReplicateV1) Validate(ctx context.Context, job BatchJobRequest, o ObjectLayer) error {
	if r == nil {
		return nil
	}

	if r.APIVersion != batchReplJobAPIVersion {
		return errInvalidArgument
	}

	if r.Source.Endpoint != "" && r.Target.Endpoint != "" {
		return errInvalidArgument
	}

	if r.Source.Creds.Empty() && r.Target.Creds.Empty() {
		return errInvalidArgument
	}

	if r.Source.Bucket == "" || r.Target.Bucket == "" {
		return errInvalidArgument
	}

	var isRemoteToLocal bool
	localBkt := r.Source.Bucket
	if r.Source.Endpoint != "" {
		localBkt = r.Target.Bucket
		isRemoteToLocal = true
	}
	info, err := o.GetBucketInfo(ctx, localBkt, BucketOptions{})
	if err != nil {
		if isErrBucketNotFound(err) {
			return batchReplicationJobError{
				Code:           "NoSuchSourceBucket",
				Description:    fmt.Sprintf("The specified bucket %s does not exist", localBkt),
				HTTPStatusCode: http.StatusNotFound,
			}
		}
		return err
	}

	if err := r.Source.Type.Validate(); err != nil {
		return err
	}
	if err := r.Source.Snowball.Validate(); err != nil {
		return err
	}

	if !r.Source.Creds.Empty() {
		if err := r.Source.Creds.Validate(); err != nil {
			return err
		}
	}
	if r.Target.Endpoint == "" && !r.Target.Creds.Empty() {
		return errInvalidArgument
	}

	if r.Source.Endpoint == "" && !r.Source.Creds.Empty() {
		return errInvalidArgument
	}

	if r.Source.Endpoint != "" && !r.Source.Type.isMinio() && !r.Source.ValidPath() {
		return errInvalidArgument
	}

	if r.Target.Endpoint != "" && !r.Target.Type.isMinio() && !r.Target.ValidPath() {
		return errInvalidArgument
	}

	if !r.Target.Creds.Empty() {
		if err := r.Target.Creds.Validate(); err != nil {
			return err
		}
	}

	if err := r.Target.Type.Validate(); err != nil {
		return err
	}

	for _, tag := range r.Flags.Filter.Tags {
		if err := tag.Validate(); err != nil {
			return err
		}
	}

	for _, meta := range r.Flags.Filter.Metadata {
		if err := meta.Validate(); err != nil {
			return err
		}
	}

	if err := r.Flags.Retry.Validate(); err != nil {
		return err
	}

	remoteEp := r.Target.Endpoint
	remoteBkt := r.Target.Bucket
	cred := r.Target.Creds
	pathStyle := r.Target.Path

	if r.Source.Endpoint != "" {
		remoteEp = r.Source.Endpoint
		cred = r.Source.Creds
		remoteBkt = r.Source.Bucket
		pathStyle = r.Source.Path
	}

	u, err := url.Parse(remoteEp)
	if err != nil {
		return err
	}

	c, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:       u.Scheme == "https",
		Transport:    getRemoteInstanceTransport(),
		BucketLookup: lookupStyle(pathStyle),
	})
	if err != nil {
		return err
	}
	c.SetAppInfo("minio-"+batchJobPrefix, r.APIVersion+" "+job.ID)

	vcfg, err := c.GetBucketVersioning(ctx, remoteBkt)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchBucket" {
			return batchReplicationJobError{
				Code:           "NoSuchTargetBucket",
				Description:    "The specified target bucket does not exist",
				HTTPStatusCode: http.StatusNotFound,
			}
		}
		return err
	}
	// if both source and target are minio instances
	minioType := r.Target.Type == BatchJobReplicateResourceMinIO && r.Source.Type == BatchJobReplicateResourceMinIO
	// If source has versioning enabled, target must have versioning enabled
	if minioType && ((info.Versioning && !vcfg.Enabled() && !isRemoteToLocal) || (!info.Versioning && vcfg.Enabled() && isRemoteToLocal)) {
		return batchReplicationJobError{
			Code: "InvalidBucketState",
			Description: fmt.Sprintf("The source '%s' has versioning enabled, target '%s' must have versioning enabled",
				r.Source.Bucket, r.Target.Bucket),
			HTTPStatusCode: http.StatusBadRequest,
		}
	}

	r.clnt = c
	return nil
}

// Type returns type of batch job, currently only supports 'replicate'
func (j BatchJobRequest) Type() madmin.BatchJobType {
	switch {
	case j.Replicate != nil:
		return madmin.BatchJobReplicate
	case j.KeyRotate != nil:
		return madmin.BatchJobKeyRotate
	case j.Expire != nil:
		return madmin.BatchJobExpire
	}
	return madmin.BatchJobType("unknown")
}

// Validate validates the current job, used by 'save()' before
// persisting the job request
func (j BatchJobRequest) Validate(ctx context.Context, o ObjectLayer) error {
	switch {
	case j.Replicate != nil:
		return j.Replicate.Validate(ctx, j, o)
	case j.KeyRotate != nil:
		return j.KeyRotate.Validate(ctx, j, o)
	case j.Expire != nil:
		return j.Expire.Validate(ctx, j, o)
	}
	return errInvalidArgument
}

func (j BatchJobRequest) delete(ctx context.Context, api ObjectLayer) {
	deleteConfig(ctx, api, getJobPath(j))
}

func (j BatchJobRequest) getJobReportPath() (string, error) {
	var fileName string
	switch {
	case j.Replicate != nil:
		fileName = batchReplName
	case j.KeyRotate != nil:
		fileName = batchKeyRotationName
	case j.Expire != nil:
		fileName = batchExpireName
	default:
		return "", errors.New("unknown job type")
	}
	return pathJoin(batchJobReportsPrefix, j.ID, fileName), nil
}

func (j *BatchJobRequest) save(ctx context.Context, api ObjectLayer) error {
	if j.Replicate == nil && j.KeyRotate == nil && j.Expire == nil {
		return errInvalidArgument
	}

	if err := j.Validate(ctx, api); err != nil {
		return err
	}

	job, err := j.MarshalMsg(nil)
	if err != nil {
		return err
	}

	return saveConfig(ctx, api, getJobPath(*j), job)
}

func (j *BatchJobRequest) load(ctx context.Context, api ObjectLayer, name string) error {
	if j == nil {
		return nil
	}

	job, err := readConfig(ctx, api, name)
	if err != nil {
		if errors.Is(err, errConfigNotFound) || isErrObjectNotFound(err) {
			err = errNoSuchJob
		}
		return err
	}

	_, err = j.UnmarshalMsg(job)
	return err
}

func batchReplicationOpts(ctx context.Context, sc string, objInfo ObjectInfo) (putOpts minio.PutObjectOptions, isMP bool, err error) {
	// TODO: support custom storage class for remote replication
	putOpts, isMP, err = putReplicationOpts(ctx, "", objInfo)
	if err != nil {
		return putOpts, isMP, err
	}
	putOpts.Internal = minio.AdvancedPutOptions{
		SourceVersionID:    objInfo.VersionID,
		SourceMTime:        objInfo.ModTime,
		SourceETag:         objInfo.ETag,
		ReplicationRequest: true,
	}
	return putOpts, isMP, nil
}

// ListBatchJobs - lists all currently active batch jobs, optionally takes {jobType}
// input to list only active batch jobs of 'jobType'
func (a adminAPIHandlers) ListBatchJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ListBatchJobsAction)
	if objectAPI == nil {
		return
	}

	jobType := r.Form.Get("jobType")

	resultCh := make(chan itemOrErr[ObjectInfo])

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := objectAPI.Walk(ctx, minioMetaBucket, batchJobPrefix, resultCh, WalkOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	listResult := madmin.ListBatchJobsResult{}
	for result := range resultCh {
		if result.Err != nil {
			writeErrorResponseJSON(ctx, w, toAPIError(ctx, result.Err), r.URL)
			return
		}
		if strings.HasPrefix(result.Item.Name, batchJobReportsPrefix+slashSeparator) {
			continue
		}
		req := &BatchJobRequest{}
		if err := req.load(ctx, objectAPI, result.Item.Name); err != nil {
			if !errors.Is(err, errNoSuchJob) {
				batchLogIf(ctx, err)
			}
			continue
		}

		if jobType == string(req.Type()) || jobType == "" {
			listResult.Jobs = append(listResult.Jobs, madmin.BatchJobResult{
				ID:      req.ID,
				Type:    req.Type(),
				Started: req.Started,
				User:    req.User,
				Elapsed: time.Since(req.Started),
			})
		}
	}

	batchLogIf(ctx, json.NewEncoder(w).Encode(&listResult))
}

// BatchJobStatus - returns the status of a batch job saved in the disk
func (a adminAPIHandlers) BatchJobStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ListBatchJobsAction)
	if objectAPI == nil {
		return
	}

	jobID := r.Form.Get("jobId")
	if jobID == "" {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, errInvalidArgument), r.URL)
		return
	}

	req := BatchJobRequest{ID: jobID}
	if i := strings.Index(jobID, "-"); i > 0 {
		switch madmin.BatchJobType(jobID[:i]) {
		case madmin.BatchJobReplicate:
			req.Replicate = &BatchJobReplicateV1{}
		case madmin.BatchJobKeyRotate:
			req.KeyRotate = &BatchJobKeyRotateV1{}
		case madmin.BatchJobExpire:
			req.Expire = &BatchJobExpire{}
		default:
			writeErrorResponseJSON(ctx, w, toAPIError(ctx, errors.New("job ID format unrecognized")), r.URL)
			return
		}
	}

	ri := &batchJobInfo{}
	if err := ri.load(ctx, objectAPI, req); err != nil {
		if !errors.Is(err, errNoSuchJob) {
			batchLogIf(ctx, err)
		}
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	buf, err := json.Marshal(madmin.BatchJobStatus{LastMetric: ri.metric()})
	if err != nil {
		batchLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	w.Write(buf)
}

var errNoSuchJob = errors.New("no such job")

// DescribeBatchJob returns the currently active batch job definition
func (a adminAPIHandlers) DescribeBatchJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.DescribeBatchJobAction)
	if objectAPI == nil {
		return
	}

	jobID := r.Form.Get("jobId")
	if jobID == "" {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, errInvalidArgument), r.URL)
		return
	}

	req := &BatchJobRequest{}
	if err := req.load(ctx, objectAPI, pathJoin(batchJobPrefix, jobID)); err != nil {
		if !errors.Is(err, errNoSuchJob) {
			batchLogIf(ctx, err)
		}

		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Remove sensitive fields.
	req.RedactSensitive()
	buf, err := yaml.Marshal(req)
	if err != nil {
		batchLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	w.Write(buf)
}

// StartBatchJob queue a new job for execution
func (a adminAPIHandlers) StartBatchJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, creds := validateAdminReq(ctx, w, r, policy.StartBatchJobAction)
	if objectAPI == nil {
		return
	}

	buf, err := io.ReadAll(xioutil.HardLimitReader(r.Body, humanize.MiByte*4))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	user := creds.AccessKey
	if creds.ParentUser != "" {
		user = creds.ParentUser
	}

	job := &BatchJobRequest{}
	if err = yaml.Unmarshal(buf, job); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Fill with default values
	if job.Replicate != nil {
		if job.Replicate.Source.Snowball.Disable == nil {
			job.Replicate.Source.Snowball.Disable = ptr(false)
		}
		if job.Replicate.Source.Snowball.Batch == nil {
			job.Replicate.Source.Snowball.Batch = ptr(100)
		}
		if job.Replicate.Source.Snowball.InMemory == nil {
			job.Replicate.Source.Snowball.InMemory = ptr(true)
		}
		if job.Replicate.Source.Snowball.Compress == nil {
			job.Replicate.Source.Snowball.Compress = ptr(false)
		}
		if job.Replicate.Source.Snowball.SmallerThan == nil {
			job.Replicate.Source.Snowball.SmallerThan = ptr("5MiB")
		}
		if job.Replicate.Source.Snowball.SkipErrs == nil {
			job.Replicate.Source.Snowball.SkipErrs = ptr(true)
		}
	}

	//  Validate the incoming job request
	if err := job.Validate(ctx, objectAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	job.ID = fmt.Sprintf("%s-%s%s%d", job.Type(), shortuuid.New(), getKeySeparator(), GetProxyEndpointLocalIndex(globalProxyEndpoints))
	job.User = user
	job.Started = time.Now()

	if err := job.save(ctx, objectAPI); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if err = globalBatchJobPool.queueJob(job); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	buf, err = json.Marshal(&madmin.BatchJobResult{
		ID:      job.ID,
		Type:    job.Type(),
		Started: job.Started,
		User:    job.User,
	})
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, buf)
}

// CancelBatchJob cancels a job in progress
func (a adminAPIHandlers) CancelBatchJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.CancelBatchJobAction)
	if objectAPI == nil {
		return
	}

	jobID := r.Form.Get("id")
	if jobID == "" {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, errInvalidArgument), r.URL)
		return
	}

	if _, proxied, _ := proxyRequestByToken(ctx, w, r, jobID, true); proxied {
		return
	}

	if err := globalBatchJobPool.canceler(jobID, true); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrInvalidRequest, err), r.URL)
		return
	}

	j := BatchJobRequest{
		ID: jobID,
	}

	j.delete(ctx, objectAPI)

	writeSuccessNoContent(w)
}

//msgp:ignore BatchJobPool

// BatchJobPool batch job pool
type BatchJobPool struct {
	ctx          context.Context
	objLayer     ObjectLayer
	once         sync.Once
	mu           sync.Mutex
	jobCh        chan *BatchJobRequest
	jmu          sync.Mutex // protects jobCancelers
	jobCancelers map[string]context.CancelFunc
	workerKillCh chan struct{}
	workerSize   int
}

var globalBatchJobPool *BatchJobPool

// newBatchJobPool creates a pool of job manifest workers of specified size
func newBatchJobPool(ctx context.Context, o ObjectLayer, workers int) *BatchJobPool {
	jpool := &BatchJobPool{
		ctx:          ctx,
		objLayer:     o,
		jobCh:        make(chan *BatchJobRequest, 10000),
		workerKillCh: make(chan struct{}, workers),
		jobCancelers: make(map[string]context.CancelFunc),
	}
	jpool.ResizeWorkers(workers)

	randomWait := func() time.Duration {
		// randomWait depends on the number of nodes to avoid triggering resume and cleanups at the same time.
		return time.Duration(rand.Float64() * float64(time.Duration(globalEndpoints.NEndpoints())*time.Hour))
	}

	go func() {
		jpool.resume(randomWait)
		jpool.cleanupReports(randomWait)
	}()

	return jpool
}

func (j *BatchJobPool) cleanupReports(randomWait func() time.Duration) {
	t := time.NewTimer(randomWait())
	defer t.Stop()

	for {
		select {
		case <-GlobalContext.Done():
			return
		case <-t.C:
			results := make(chan itemOrErr[ObjectInfo], 100)
			ctx, cancel := context.WithCancel(j.ctx)
			defer cancel()
			if err := j.objLayer.Walk(ctx, minioMetaBucket, batchJobReportsPrefix, results, WalkOptions{}); err != nil {
				batchLogIf(j.ctx, err)
				t.Reset(randomWait())
				continue
			}
			for result := range results {
				if result.Err != nil {
					batchLogIf(j.ctx, result.Err)
					continue
				}
				ri := &batchJobInfo{}
				if err := ri.loadByPath(ctx, j.objLayer, result.Item.Name); err != nil {
					batchLogIf(ctx, err)
					continue
				}
				if (ri.Complete || ri.Failed) && time.Since(ri.LastUpdate) > oldJobsExpiration {
					deleteConfig(ctx, j.objLayer, result.Item.Name)
				}
			}

			t.Reset(randomWait())
		}
	}
}

func (j *BatchJobPool) resume(randomWait func() time.Duration) {
	time.Sleep(randomWait())

	results := make(chan itemOrErr[ObjectInfo], 100)
	ctx, cancel := context.WithCancel(j.ctx)
	defer cancel()
	if err := j.objLayer.Walk(ctx, minioMetaBucket, batchJobPrefix, results, WalkOptions{}); err != nil {
		batchLogIf(j.ctx, err)
		return
	}
	for result := range results {
		if result.Err != nil {
			batchLogIf(j.ctx, result.Err)
			continue
		}
		if strings.HasPrefix(result.Item.Name, batchJobReportsPrefix+slashSeparator) {
			continue
		}
		// ignore batch-replicate.bin and batch-rotate.bin entries
		if strings.HasSuffix(result.Item.Name, slashSeparator) {
			continue
		}
		req := &BatchJobRequest{}
		if err := req.load(ctx, j.objLayer, result.Item.Name); err != nil {
			batchLogIf(ctx, err)
			continue
		}
		_, nodeIdx := parseRequestToken(req.ID)
		if nodeIdx > -1 && GetProxyEndpointLocalIndex(globalProxyEndpoints) != nodeIdx {
			// This job doesn't belong on this node.
			continue
		}
		if err := j.queueJob(req); err != nil {
			batchLogIf(ctx, err)
			continue
		}
	}
}

// AddWorker adds a replication worker to the pool
func (j *BatchJobPool) AddWorker() {
	if j == nil {
		return
	}
	for {
		select {
		case <-j.ctx.Done():
			return
		case job, ok := <-j.jobCh:
			if !ok {
				return
			}
			switch {
			case job.Replicate != nil:
				if job.Replicate.RemoteToLocal() {
					if err := job.Replicate.StartFromSource(job.ctx, j.objLayer, *job); err != nil {
						if !isErrBucketNotFound(err) {
							batchLogIf(j.ctx, err)
							j.canceler(job.ID, false)
							continue
						}
						// Bucket not found proceed to delete such a job.
					}
				} else {
					if err := job.Replicate.Start(job.ctx, j.objLayer, *job); err != nil {
						if !isErrBucketNotFound(err) {
							batchLogIf(j.ctx, err)
							j.canceler(job.ID, false)
							continue
						}
						// Bucket not found proceed to delete such a job.
					}
				}
			case job.KeyRotate != nil:
				if err := job.KeyRotate.Start(job.ctx, j.objLayer, *job); err != nil {
					if !isErrBucketNotFound(err) {
						batchLogIf(j.ctx, err)
						continue
					}
				}
			case job.Expire != nil:
				if err := job.Expire.Start(job.ctx, j.objLayer, *job); err != nil {
					if !isErrBucketNotFound(err) {
						batchLogIf(j.ctx, err)
						continue
					}
				}
			}
			j.canceler(job.ID, false)
		case <-j.workerKillCh:
			return
		}
	}
}

// ResizeWorkers sets replication workers pool to new size
func (j *BatchJobPool) ResizeWorkers(n int) {
	if j == nil {
		return
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	for j.workerSize < n {
		j.workerSize++
		go j.AddWorker()
	}
	for j.workerSize > n {
		j.workerSize--
		go func() { j.workerKillCh <- struct{}{} }()
	}
}

func (j *BatchJobPool) queueJob(req *BatchJobRequest) error {
	if j == nil {
		return errInvalidArgument
	}
	jctx, jcancel := context.WithCancel(j.ctx)
	j.jmu.Lock()
	j.jobCancelers[req.ID] = jcancel
	j.jmu.Unlock()
	req.ctx = jctx

	select {
	case <-j.ctx.Done():
		j.once.Do(func() {
			xioutil.SafeClose(j.jobCh)
		})
	case j.jobCh <- req:
	default:
		return fmt.Errorf("batch job queue is currently full please try again later %#v", req)
	}
	return nil
}

// delete canceler from the map, cancel job if requested
func (j *BatchJobPool) canceler(jobID string, cancel bool) error {
	if j == nil {
		return errInvalidArgument
	}
	j.jmu.Lock()
	defer j.jmu.Unlock()
	if canceler, ok := j.jobCancelers[jobID]; ok {
		if cancel {
			canceler()
		}
	}
	if cancel {
		delete(j.jobCancelers, jobID)
	}
	return nil
}

//msgp:ignore batchJobMetrics
type batchJobMetrics struct {
	sync.RWMutex
	metrics map[string]*batchJobInfo
}

//msgp:ignore batchJobMetric
//go:generate stringer -type=batchJobMetric -trimprefix=batchJobMetric $GOFILE
type batchJobMetric uint8

const (
	batchJobMetricReplication batchJobMetric = iota
	batchJobMetricKeyRotation
	batchJobMetricExpire
)

func batchJobTrace(d batchJobMetric, job string, startTime time.Time, duration time.Duration, info objTraceInfoer, attempts int, err error) madmin.TraceInfo {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	traceType := madmin.TraceBatchReplication
	switch d {
	case batchJobMetricKeyRotation:
		traceType = madmin.TraceBatchKeyRotation
	case batchJobMetricExpire:
		traceType = madmin.TraceBatchExpire
	}
	funcName := fmt.Sprintf("%s() (job-name=%s)", d.String(), job)
	if attempts > 0 {
		funcName = fmt.Sprintf("%s() (job-name=%s,attempts=%s)", d.String(), job, humanize.Ordinal(attempts))
	}
	return madmin.TraceInfo{
		TraceType: traceType,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  funcName,
		Duration:  duration,
		Path:      fmt.Sprintf("%s (versionID=%s)", info.TraceObjName(), info.TraceVersionID()),
		Error:     errStr,
	}
}

func (ri *batchJobInfo) metric() madmin.JobMetric {
	m := madmin.JobMetric{
		JobID:         ri.JobID,
		JobType:       ri.JobType,
		StartTime:     ri.StartTime,
		LastUpdate:    ri.LastUpdate,
		RetryAttempts: ri.RetryAttempts,
		Complete:      ri.Complete,
		Failed:        ri.Failed,
	}

	switch ri.JobType {
	case string(madmin.BatchJobReplicate):
		m.Replicate = &madmin.ReplicateInfo{
			Bucket:              ri.Bucket,
			Object:              ri.Object,
			Objects:             ri.Objects,
			DeleteMarkers:       ri.DeleteMarkers,
			ObjectsFailed:       ri.ObjectsFailed,
			DeleteMarkersFailed: ri.DeleteMarkersFailed,
			BytesTransferred:    ri.BytesTransferred,
			BytesFailed:         ri.BytesFailed,
		}
	case string(madmin.BatchJobKeyRotate):
		m.KeyRotate = &madmin.KeyRotationInfo{
			Bucket:        ri.Bucket,
			Object:        ri.Object,
			Objects:       ri.Objects,
			ObjectsFailed: ri.ObjectsFailed,
		}
	case string(madmin.BatchJobExpire):
		m.Expired = &madmin.ExpirationInfo{
			Bucket:              ri.Bucket,
			Object:              ri.Object,
			Objects:             ri.Objects,
			DeleteMarkers:       ri.DeleteMarkers,
			ObjectsFailed:       ri.ObjectsFailed,
			DeleteMarkersFailed: ri.DeleteMarkersFailed,
		}
	}

	return m
}

func (m *batchJobMetrics) report(jobID string) (metrics *madmin.BatchJobMetrics) {
	metrics = &madmin.BatchJobMetrics{CollectedAt: time.Now(), Jobs: make(map[string]madmin.JobMetric)}
	m.RLock()
	defer m.RUnlock()

	if jobID != "" {
		if job, ok := m.metrics[jobID]; ok {
			metrics.Jobs[jobID] = job.metric()
		}
		return metrics
	}

	for id, job := range m.metrics {
		metrics.Jobs[id] = job.metric()
	}
	return metrics
}

// keep job metrics for some time after the job is completed
// in-case some one wants to look at the older results.
func (m *batchJobMetrics) purgeJobMetrics() {
	t := time.NewTicker(6 * time.Hour)
	defer t.Stop()

	for {
		select {
		case <-GlobalContext.Done():
			return
		case <-t.C:
			var toDeleteJobMetrics []string
			m.RLock()
			for id, metrics := range m.metrics {
				if time.Since(metrics.LastUpdate) > oldJobsExpiration && (metrics.Complete || metrics.Failed) {
					toDeleteJobMetrics = append(toDeleteJobMetrics, id)
				}
			}
			m.RUnlock()
			for _, jobID := range toDeleteJobMetrics {
				m.delete(jobID)
				j := BatchJobRequest{
					ID: jobID,
				}
				j.delete(GlobalContext, newObjectLayerFn())
			}
		}
	}
}

// load metrics from disk on startup
func (m *batchJobMetrics) init(ctx context.Context, objectAPI ObjectLayer) error {
	resultCh := make(chan itemOrErr[ObjectInfo])

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := objectAPI.Walk(ctx, minioMetaBucket, batchJobReportsPrefix, resultCh, WalkOptions{}); err != nil {
		return err
	}

	for result := range resultCh {
		if result.Err != nil {
			return result.Err
		}
		ri := &batchJobInfo{}
		if err := ri.loadByPath(ctx, objectAPI, result.Item.Name); err != nil {
			if !errors.Is(err, errNoSuchJob) {
				batchLogIf(ctx, err)
			}
			continue
		}
		m.metrics[ri.JobID] = ri
	}
	return nil
}

func (m *batchJobMetrics) delete(jobID string) {
	m.Lock()
	defer m.Unlock()

	delete(m.metrics, jobID)
}

func (m *batchJobMetrics) save(jobID string, ri *batchJobInfo) {
	m.Lock()
	defer m.Unlock()

	m.metrics[jobID] = ri.clone()
}

type objTraceInfoer interface {
	TraceObjName() string
	TraceVersionID() string
}

// TraceObjName returns name of object being traced
func (td ObjectToDelete) TraceObjName() string {
	return td.ObjectName
}

// TraceVersionID returns version-id of object being traced
func (td ObjectToDelete) TraceVersionID() string {
	return td.VersionID
}

// TraceObjName returns name of object being traced
func (oi ObjectInfo) TraceObjName() string {
	return oi.Name
}

// TraceVersionID returns version-id of object being traced
func (oi ObjectInfo) TraceVersionID() string {
	return oi.VersionID
}

func (m *batchJobMetrics) trace(d batchJobMetric, job string, attempts int) func(info objTraceInfoer, err error) {
	startTime := time.Now()
	return func(info objTraceInfoer, err error) {
		duration := time.Since(startTime)
		if globalTrace.NumSubscribers(madmin.TraceBatch) > 0 {
			globalTrace.Publish(batchJobTrace(d, job, startTime, duration, info, attempts, err))
			return
		}
		switch d {
		case batchJobMetricReplication:
			if globalTrace.NumSubscribers(madmin.TraceBatchReplication) > 0 {
				globalTrace.Publish(batchJobTrace(d, job, startTime, duration, info, attempts, err))
			}
		case batchJobMetricKeyRotation:
			if globalTrace.NumSubscribers(madmin.TraceBatchKeyRotation) > 0 {
				globalTrace.Publish(batchJobTrace(d, job, startTime, duration, info, attempts, err))
			}
		case batchJobMetricExpire:
			if globalTrace.NumSubscribers(madmin.TraceBatchExpire) > 0 {
				globalTrace.Publish(batchJobTrace(d, job, startTime, duration, info, attempts, err))
			}
		}
	}
}

func lookupStyle(s string) minio.BucketLookupType {
	var lookup minio.BucketLookupType
	switch s {
	case "on":
		lookup = minio.BucketLookupPath
	case "off":
		lookup = minio.BucketLookupDNS
	default:
		lookup = minio.BucketLookupAuto
	}
	return lookup
}

// BatchJobPrefix - to support prefix field yaml unmarshalling with string or slice of strings
type BatchJobPrefix []string

var _ yaml.Unmarshaler = &BatchJobPrefix{}

// UnmarshalYAML - to support prefix field yaml unmarshalling with string or slice of strings
func (b *BatchJobPrefix) UnmarshalYAML(value *yaml.Node) error {
	// try slice first
	tmpSlice := []string{}
	if err := value.Decode(&tmpSlice); err == nil {
		*b = tmpSlice
		return nil
	}
	// try string
	tmpStr := ""
	if err := value.Decode(&tmpStr); err == nil {
		*b = []string{tmpStr}
		return nil
	}
	return fmt.Errorf("unable to decode %s", value.Value)
}

// F - return prefix(es) as slice
func (b *BatchJobPrefix) F() []string {
	return *b
}
