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
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/config/batch"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/pkg/v2/env"
	"github.com/minio/pkg/v2/policy"
	"github.com/minio/pkg/v2/workers"
	"gopkg.in/yaml.v2"
)

var globalBatchConfig batch.Config

// BatchJobRequest this is an internal data structure not for external consumption.
type BatchJobRequest struct {
	ID        string               `yaml:"-" json:"name"`
	User      string               `yaml:"-" json:"user"`
	Started   time.Time            `yaml:"-" json:"started"`
	Location  string               `yaml:"-" json:"location"`
	Replicate *BatchJobReplicateV1 `yaml:"replicate" json:"replicate"`
	KeyRotate *BatchJobKeyRotateV1 `yaml:"keyrotate" json:"keyrotate"`
	Expire    *BatchJobExpireV1    `yaml:"expire" json:"expire"`
	ctx       context.Context      `msg:"-"`
}

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

	clnt := http.Client{Transport: getRemoteInstanceTransport}
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
func (r *BatchJobReplicateV1) ReplicateFromSource(ctx context.Context, api ObjectLayer, core *miniogo.Core, srcObjInfo ObjectInfo, retry bool) error {
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
	gopts := miniogo.GetObjectOptions{
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

func (r *BatchJobReplicateV1) copyWithMultipartfromSource(ctx context.Context, api ObjectLayer, c *miniogo.Core, srcObjInfo ObjectInfo, opts ObjectOptions, partsCount int) (err error) {
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
				logger.LogIf(ctx,
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

	for i := 0; i < partsCount; i++ {
		gopts := miniogo.GetObjectOptions{
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
	if err := ri.load(ctx, api, job); err != nil {
		return err
	}
	if ri.Complete {
		return nil
	}
	globalBatchJobsMetrics.save(job.ID, ri)

	delay := job.Replicate.Flags.Retry.Delay
	if delay == 0 {
		delay = batchReplJobDefaultRetryDelay
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	isTags := len(r.Flags.Filter.Tags) != 0
	isMetadata := len(r.Flags.Filter.Metadata) != 0
	isStorageClassOnly := len(r.Flags.Filter.Metadata) == 1 && strings.EqualFold(r.Flags.Filter.Metadata[0].Key, xhttp.AmzStorageClass)

	skip := func(oi ObjectInfo) (ok bool) {
		if r.Flags.Filter.OlderThan > 0 && time.Since(oi.ModTime) < r.Flags.Filter.OlderThan {
			// skip all objects that are newer than specified older duration
			return true
		}

		if r.Flags.Filter.NewerThan > 0 && time.Since(oi.ModTime) >= r.Flags.Filter.NewerThan {
			// skip all objects that are older than specified newer duration
			return true
		}

		if !r.Flags.Filter.CreatedAfter.IsZero() && r.Flags.Filter.CreatedAfter.Before(oi.ModTime) {
			// skip all objects that are created before the specified time.
			return true
		}

		if !r.Flags.Filter.CreatedBefore.IsZero() && r.Flags.Filter.CreatedBefore.After(oi.ModTime) {
			// skip all objects that are created after the specified time.
			return true
		}

		if isTags {
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

	c, err := miniogo.New(u.Host, &miniogo.Options{
		Creds:        credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:       u.Scheme == "https",
		Transport:    getRemoteInstanceTransport,
		BucketLookup: lookupStyle(r.Source.Path),
	})
	if err != nil {
		return err
	}

	c.SetAppInfo("minio-"+batchJobPrefix, r.APIVersion+" "+job.ID)
	core := &miniogo.Core{Client: c}

	workerSize, err := strconv.Atoi(env.Get("_MINIO_BATCH_REPLICATION_WORKERS", strconv.Itoa(runtime.GOMAXPROCS(0)/2)))
	if err != nil {
		return err
	}

	wk, err := workers.New(workerSize)
	if err != nil {
		// invalid worker size.
		return err
	}

	retryAttempts := ri.RetryAttempts
	retry := false
	for attempts := 1; attempts <= retryAttempts; attempts++ {
		attempts := attempts
		// one of source/target is s3, skip delete marker and all versions under the same object name.
		s3Type := r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3
		minioSrc := r.Source.Type == BatchJobReplicateResourceMinIO
		ctx, cancel := context.WithCancel(ctx)
		objInfoCh := c.ListObjects(ctx, r.Source.Bucket, miniogo.ListObjectsOptions{
			Prefix:       r.Source.Prefix,
			WithVersions: minioSrc,
			Recursive:    true,
			WithMetadata: true,
		})
		prevObj := ""
		skipReplicate := false

		for obj := range objInfoCh {
			oi := toObjectInfo(r.Source.Bucket, obj.Key, obj)
			if !minioSrc {
				// Check if metadata filter was requested and it is expected to have
				// all user metadata or just storageClass. If its only storageClass
				// List() already returns relevant information for filter to be applied.
				if isMetadata && !isStorageClassOnly {
					oi2, err := c.StatObject(ctx, r.Source.Bucket, obj.Key, miniogo.StatObjectOptions{})
					if err == nil {
						oi = toObjectInfo(r.Source.Bucket, obj.Key, oi2)
					} else {
						if !isErrMethodNotAllowed(ErrorRespToObjectError(err, r.Source.Bucket, obj.Key)) &&
							!isErrObjectNotFound(ErrorRespToObjectError(err, r.Source.Bucket, obj.Key)) {
							logger.LogIf(ctx, err)
						}
						continue
					}
				}
				if isTags {
					tags, err := c.GetObjectTagging(ctx, r.Source.Bucket, obj.Key, minio.GetObjectTaggingOptions{})
					if err == nil {
						oi.UserTags = tags.String()
					} else {
						if !isErrMethodNotAllowed(ErrorRespToObjectError(err, r.Source.Bucket, obj.Key)) &&
							!isErrObjectNotFound(ErrorRespToObjectError(err, r.Source.Bucket, obj.Key)) {
							logger.LogIf(ctx, err)
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
					logger.LogIf(ctx, err)
					success = false
				} else {
					stopFn(oi, nil)
				}
				ri.trackCurrentBucketObject(r.Target.Bucket, oi, success)
				globalBatchJobsMetrics.save(job.ID, ri)
				// persist in-memory state to disk after every 10secs.
				logger.LogIf(ctx, ri.updateAfter(ctx, api, 10*time.Second, job))

				if wait := globalBatchConfig.Clone().ReplicationWorkersWait; wait > 0 {
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
		logger.LogIf(ctx, ri.updateAfter(ctx, api, 0, job))

		if err := r.Notify(ctx, ri); err != nil {
			logger.LogIf(ctx, fmt.Errorf("unable to notify %v", err))
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
func toObjectInfo(bucket, object string, objInfo miniogo.ObjectInfo) ObjectInfo {
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

	for k, v := range objInfo.UserMetadata {
		oi.UserDefined[k] = v
	}

	return oi
}

func (r BatchJobReplicateV1) writeAsArchive(ctx context.Context, objAPI ObjectLayer, remoteClnt *minio.Client, entries []ObjectInfo) error {
	input := make(chan minio.SnowballObject, 1)
	opts := minio.SnowballOptions{
		Opts:     minio.PutObjectOptions{},
		InMemory: *r.Source.Snowball.InMemory,
		Compress: *r.Source.Snowball.Compress,
		SkipErrs: *r.Source.Snowball.SkipErrs,
	}

	go func() {
		defer close(input)

		for _, entry := range entries {
			gr, err := objAPI.GetObjectNInfo(ctx, r.Source.Bucket,
				entry.Name, nil, nil, ObjectOptions{
					VersionID: entry.VersionID,
				})
			if err != nil {
				logger.LogIf(ctx, err)
				continue
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

			opts, err := batchReplicationOpts(ctx, "", gr.ObjInfo)
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}

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
func (r *BatchJobReplicateV1) ReplicateToTarget(ctx context.Context, api ObjectLayer, c *miniogo.Core, srcObjInfo ObjectInfo, retry bool) error {
	srcBucket := r.Source.Bucket
	tgtBucket := r.Target.Bucket
	tgtPrefix := r.Target.Prefix
	srcObject := srcObjInfo.Name
	s3Type := r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3

	if srcObjInfo.DeleteMarker || !srcObjInfo.VersionPurgeStatus.Empty() {
		if retry && !s3Type {
			if _, err := c.StatObject(ctx, tgtBucket, pathJoin(tgtPrefix, srcObject), miniogo.StatObjectOptions{
				VersionID: srcObjInfo.VersionID,
				Internal: miniogo.AdvancedGetOptions{
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
		return c.RemoveObject(ctx, tgtBucket, pathJoin(tgtPrefix, srcObject), miniogo.RemoveObjectOptions{
			VersionID: versionID,
			Internal: miniogo.AdvancedRemoveOptions{
				ReplicationDeleteMarker: dmVersionID != "",
				ReplicationMTime:        srcObjInfo.ModTime,
				ReplicationStatus:       miniogo.ReplicationStatusReplica,
				ReplicationRequest:      true, // always set this to distinguish between `mc mirror` replication and serverside
			},
		})
	}

	if retry && !s3Type { // when we are retrying avoid copying if necessary.
		gopts := miniogo.GetObjectOptions{}
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

	putOpts, err := batchReplicationOpts(ctx, "", objInfo)
	if err != nil {
		return err
	}
	if r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3 {
		putOpts.Internal = miniogo.AdvancedPutOptions{}
	}
	if objInfo.isMultipart() {
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
	batchReplName      = "batch-replicate.bin"
	batchReplFormat    = 1
	batchReplVersionV1 = 1
	batchReplVersion   = batchReplVersionV1
	batchJobName       = "job.bin"
	batchJobPrefix     = "batch-jobs"

	batchReplJobAPIVersion        = "v1"
	batchReplJobDefaultRetries    = 3
	batchReplJobDefaultRetryDelay = 250 * time.Millisecond
)

func (ri *batchJobInfo) load(ctx context.Context, api ObjectLayer, job BatchJobRequest) error {
	var fileName string
	var format, version uint16
	switch {
	case job.Replicate != nil:
		fileName = batchReplName
		version = batchReplVersionV1
		format = batchReplFormat
	case job.KeyRotate != nil:
		fileName = batchKeyRotationName
		version = batchKeyRotateVersionV1
		format = batchKeyRotationFormat
	case job.Expire != nil:
		fileName = batchExpireName
		version = batchExpireVersionV1
		format = batchExpireFormat
	default:
		return errors.New("no supported batch job request specified")
	}
	data, err := readConfig(ctx, api, pathJoin(job.Location, fileName))
	if err != nil {
		if errors.Is(err, errConfigNotFound) || isErrObjectNotFound(err) {
			ri.Version = int(version)
			switch {
			case job.Replicate != nil:
				ri.RetryAttempts = batchReplJobDefaultRetries
				if job.Replicate.Flags.Retry.Attempts > 0 {
					ri.RetryAttempts = job.Replicate.Flags.Retry.Attempts
				}
			case job.KeyRotate != nil:
				ri.RetryAttempts = batchKeyRotateJobDefaultRetries
				if job.KeyRotate.Flags.Retry.Attempts > 0 {
					ri.RetryAttempts = job.KeyRotate.Flags.Retry.Attempts
				}
			case job.Expire != nil:
				ri.RetryAttempts = batchExpireJobDefaultRetries
				if job.Expire.Retry.Attempts > 0 {
					ri.RetryAttempts = job.Expire.Retry.Attempts
				}
			}
			return nil
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
		Version:          ri.Version,
		JobID:            ri.JobID,
		JobType:          ri.JobType,
		RetryAttempts:    ri.RetryAttempts,
		Complete:         ri.Complete,
		Failed:           ri.Failed,
		StartTime:        ri.StartTime,
		LastUpdate:       ri.LastUpdate,
		Bucket:           ri.Bucket,
		Object:           ri.Object,
		Objects:          ri.Objects,
		ObjectsFailed:    ri.ObjectsFailed,
		BytesTransferred: ri.BytesTransferred,
		BytesFailed:      ri.BytesFailed,
	}
}

func (ri *batchJobInfo) countItem(size int64, dmarker, success bool) {
	if ri == nil {
		return
	}
	if success {
		if dmarker {
			ri.DeleteMarkers++
		} else {
			ri.Objects++
			ri.BytesTransferred += size
		}
	} else {
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
		format, version  uint16
		jobTyp, fileName string
	)

	if now.Sub(ri.LastUpdate) >= duration {
		switch job.Type() {
		case madmin.BatchJobReplicate:
			format = batchReplFormat
			version = batchReplVersion
			jobTyp = string(job.Type())
			fileName = batchReplName
			ri.Version = batchReplVersionV1
		case madmin.BatchJobKeyRotate:
			format = batchKeyRotationFormat
			version = batchKeyRotateVersion
			jobTyp = string(job.Type())
			fileName = batchKeyRotationName
			ri.Version = batchKeyRotateVersionV1
		case madmin.BatchJobExpire:
			format = batchExpireFormat
			version = batchExpireVersion
			jobTyp = string(job.Type())
			fileName = batchExpireName
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
		return saveConfig(ctx, api, pathJoin(job.Location, fileName), buf)
	}
	ri.mu.Unlock()
	return nil
}

func (ri *batchJobInfo) trackCurrentBucketObject(bucket string, info ObjectInfo, failed bool) {
	if ri == nil {
		return
	}

	ri.mu.Lock()
	defer ri.mu.Unlock()

	ri.Bucket = bucket
	ri.Object = info.Name
	ri.countItem(info.Size, info.DeleteMarker, failed)
}

// Start start the batch replication job, resumes if there was a pending job via "job.ID"
func (r *BatchJobReplicateV1) Start(ctx context.Context, api ObjectLayer, job BatchJobRequest) error {
	ri := &batchJobInfo{
		JobID:     job.ID,
		JobType:   string(job.Type()),
		StartTime: job.Started,
	}
	if err := ri.load(ctx, api, job); err != nil {
		return err
	}
	if ri.Complete {
		return nil
	}
	globalBatchJobsMetrics.save(job.ID, ri)
	lastObject := ri.Object

	delay := job.Replicate.Flags.Retry.Delay
	if delay == 0 {
		delay = batchReplJobDefaultRetryDelay
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	selectObj := func(info FileInfo) (ok bool) {
		if r.Flags.Filter.OlderThan > 0 && time.Since(info.ModTime) < r.Flags.Filter.OlderThan {
			// skip all objects that are newer than specified older duration
			return false
		}

		if r.Flags.Filter.NewerThan > 0 && time.Since(info.ModTime) >= r.Flags.Filter.NewerThan {
			// skip all objects that are older than specified newer duration
			return false
		}

		if !r.Flags.Filter.CreatedAfter.IsZero() && r.Flags.Filter.CreatedAfter.Before(info.ModTime) {
			// skip all objects that are created before the specified time.
			return false
		}

		if !r.Flags.Filter.CreatedBefore.IsZero() && r.Flags.Filter.CreatedBefore.After(info.ModTime) {
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
		if (r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3) && !info.IsLatest {
			return false
		}

		return true
	}

	u, err := url.Parse(r.Target.Endpoint)
	if err != nil {
		return err
	}

	cred := r.Target.Creds

	c, err := miniogo.NewCore(u.Host, &miniogo.Options{
		Creds:        credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:       u.Scheme == "https",
		Transport:    getRemoteInstanceTransport,
		BucketLookup: lookupStyle(r.Target.Path),
	})
	if err != nil {
		return err
	}

	c.SetAppInfo("minio-"+batchJobPrefix, r.APIVersion+" "+job.ID)

	var (
		walkCh = make(chan ObjectInfo, 100)
		slowCh = make(chan ObjectInfo, 100)
	)

	if !*r.Source.Snowball.Disable && r.Source.Type.isMinio() && r.Target.Type.isMinio() {
		go func() {
			defer close(slowCh)

			// Snowball currently needs the high level minio-go Client, not the Core one
			cl, err := miniogo.New(u.Host, &miniogo.Options{
				Creds:        credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
				Secure:       u.Scheme == "https",
				Transport:    getRemoteInstanceTransport,
				BucketLookup: lookupStyle(r.Target.Path),
			})
			if err != nil {
				logger.LogIf(ctx, err)
				return
			}

			// Already validated before arriving here
			smallerThan, _ := humanize.ParseBytes(*r.Source.Snowball.SmallerThan)

			var (
				obj   = ObjectInfo{}
				batch = make([]ObjectInfo, 0, *r.Source.Snowball.Batch)
				valid = true
			)

			for valid {
				obj, valid = <-walkCh

				if !valid {
					goto write
				}

				if obj.DeleteMarker || !obj.VersionPurgeStatus.Empty() || obj.Size >= int64(smallerThan) {
					slowCh <- obj
					continue
				}

				batch = append(batch, obj)

				if len(batch) < *r.Source.Snowball.Batch {
					continue
				}

			write:
				if len(batch) > 0 {
					if err := r.writeAsArchive(ctx, api, cl, batch); err != nil {
						logger.LogIf(ctx, err)
						for _, b := range batch {
							slowCh <- b
						}
					}
					batch = batch[:0]
				}
			}
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

	retryAttempts := ri.RetryAttempts
	retry := false
	for attempts := 1; attempts <= retryAttempts; attempts++ {
		attempts := attempts

		ctx, cancel := context.WithCancel(ctx)
		// one of source/target is s3, skip delete marker and all versions under the same object name.
		s3Type := r.Target.Type == BatchJobReplicateResourceS3 || r.Source.Type == BatchJobReplicateResourceS3

		results := make(chan ObjectInfo, 100)
		if err := api.Walk(ctx, r.Source.Bucket, r.Source.Prefix, results, WalkOptions{
			Marker:   lastObject,
			Filter:   selectObj,
			AskDisks: walkQuorum,
		}); err != nil {
			cancel()
			// Do not need to retry if we can't list objects on source.
			return err
		}

		prevObj := ""

		skipReplicate := false
		for result := range slowCh {
			result := result
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
					if miniogo.ToErrorResponse(err).Code == "PreconditionFailed" {
						// pre-condition failed means we already have the object copied over.
						return
					}
					// object must be deleted concurrently, allow these failures but do not count them
					if isErrVersionNotFound(err) || isErrObjectNotFound(err) {
						return
					}
					stopFn(result, err)
					logger.LogIf(ctx, err)
					success = false
				} else {
					stopFn(result, nil)
				}
				ri.trackCurrentBucketObject(r.Source.Bucket, result, success)
				globalBatchJobsMetrics.save(job.ID, ri)
				// persist in-memory state to disk after every 10secs.
				logger.LogIf(ctx, ri.updateAfter(ctx, api, 10*time.Second, job))

				if wait := globalBatchConfig.Clone().ReplicationWorkersWait; wait > 0 {
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
		logger.LogIf(ctx, ri.updateAfter(ctx, api, 0, job))

		if err := r.Notify(ctx, ri); err != nil {
			logger.LogIf(ctx, fmt.Errorf("unable to notify %v", err))
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

//msgp:ignore batchReplicationJobError
type batchReplicationJobError struct {
	Code           string
	Description    string
	HTTPStatusCode int
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

	if r.Source.Bucket == "" {
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
	if r.Source.Creds.Empty() && r.Target.Creds.Empty() {
		return errInvalidArgument
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
	if r.Target.Bucket == "" {
		return errInvalidArgument
	}

	if !r.Target.Creds.Empty() {
		if err := r.Target.Creds.Validate(); err != nil {
			return err
		}
	}

	if r.Source.Creds.Empty() && r.Target.Creds.Empty() {
		return errInvalidArgument
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

	c, err := miniogo.NewCore(u.Host, &miniogo.Options{
		Creds:        credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:       u.Scheme == "https",
		Transport:    getRemoteInstanceTransport,
		BucketLookup: lookupStyle(pathStyle),
	})
	if err != nil {
		return err
	}
	c.SetAppInfo("minio-"+batchJobPrefix, r.APIVersion+" "+job.ID)

	vcfg, err := c.GetBucketVersioning(ctx, remoteBkt)
	if err != nil {
		if miniogo.ToErrorResponse(err).Code == "NoSuchBucket" {
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
	switch {
	case j.Replicate != nil:
		deleteConfig(ctx, api, pathJoin(j.Location, batchReplName))
	case j.KeyRotate != nil:
		deleteConfig(ctx, api, pathJoin(j.Location, batchKeyRotationName))
	case j.Expire != nil:
		deleteConfig(ctx, api, pathJoin(j.Location, batchExpireName))
	}
	deleteConfig(ctx, api, j.Location)
}

func (j *BatchJobRequest) save(ctx context.Context, api ObjectLayer) error {
	if j.Replicate == nil && j.KeyRotate == nil && j.Expire == nil {
		return errInvalidArgument
	}

	if err := j.Validate(ctx, api); err != nil {
		return err
	}

	j.Location = pathJoin(batchJobPrefix, j.ID)
	job, err := j.MarshalMsg(nil)
	if err != nil {
		return err
	}

	return saveConfig(ctx, api, j.Location, job)
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

func batchReplicationOpts(ctx context.Context, sc string, objInfo ObjectInfo) (putOpts miniogo.PutObjectOptions, err error) {
	// TODO: support custom storage class for remote replication
	putOpts, err = putReplicationOpts(ctx, "", objInfo)
	if err != nil {
		return putOpts, err
	}
	putOpts.Internal = miniogo.AdvancedPutOptions{
		SourceVersionID:    objInfo.VersionID,
		SourceMTime:        objInfo.ModTime,
		SourceETag:         objInfo.ETag,
		ReplicationRequest: true,
	}
	return putOpts, nil
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
	if jobType == "" {
		jobType = string(madmin.BatchJobReplicate)
	}

	resultCh := make(chan ObjectInfo)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := objectAPI.Walk(ctx, minioMetaBucket, batchJobPrefix, resultCh, WalkOptions{}); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	listResult := madmin.ListBatchJobsResult{}
	for result := range resultCh {
		req := &BatchJobRequest{}
		if err := req.load(ctx, objectAPI, result.Name); err != nil {
			if !errors.Is(err, errNoSuchJob) {
				logger.LogIf(ctx, err)
			}
			continue
		}

		if jobType == string(req.Type()) {
			listResult.Jobs = append(listResult.Jobs, madmin.BatchJobResult{
				ID:      req.ID,
				Type:    req.Type(),
				Started: req.Started,
				User:    req.User,
				Elapsed: time.Since(req.Started),
			})
		}
	}

	logger.LogIf(ctx, json.NewEncoder(w).Encode(&listResult))
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
			logger.LogIf(ctx, err)
		}

		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	buf, err := yaml.Marshal(req)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	w.Write(buf)
}

// StarBatchJob queue a new job for execution
func (a adminAPIHandlers) StartBatchJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, creds := validateAdminReq(ctx, w, r, policy.StartBatchJobAction)
	if objectAPI == nil {
		return
	}

	buf, err := io.ReadAll(ioutil.HardLimitReader(r.Body, humanize.MiByte*4))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	user := creds.AccessKey
	if creds.ParentUser != "" {
		user = creds.ParentUser
	}

	job := &BatchJobRequest{}
	if err = yaml.UnmarshalStrict(buf, job); err != nil {
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

	job.ID = fmt.Sprintf("%s:%d", shortuuid.New(), GetProxyEndpointLocalIndex(globalProxyEndpoints))
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

	if _, success := proxyRequestByToken(ctx, w, r, jobID); success {
		return
	}

	if err := globalBatchJobPool.canceler(jobID, true); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrInvalidRequest, err), r.URL)
		return
	}

	j := BatchJobRequest{
		ID:       jobID,
		Location: pathJoin(batchJobPrefix, jobID),
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
	jpool.resume()
	return jpool
}

func (j *BatchJobPool) resume() {
	results := make(chan ObjectInfo, 100)
	ctx, cancel := context.WithCancel(j.ctx)
	defer cancel()
	if err := j.objLayer.Walk(ctx, minioMetaBucket, batchJobPrefix, results, WalkOptions{}); err != nil {
		logger.LogIf(j.ctx, err)
		return
	}
	for result := range results {
		// ignore batch-replicate.bin and batch-rotate.bin entries
		if strings.HasSuffix(result.Name, slashSeparator) {
			continue
		}
		req := &BatchJobRequest{}
		if err := req.load(ctx, j.objLayer, result.Name); err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		_, nodeIdx := parseRequestToken(req.ID)
		if nodeIdx > -1 && GetProxyEndpointLocalIndex(globalProxyEndpoints) != nodeIdx {
			// This job doesn't belong on this node.
			continue
		}
		if err := j.queueJob(req); err != nil {
			logger.LogIf(ctx, err)
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
							logger.LogIf(j.ctx, err)
							j.canceler(job.ID, false)
							continue
						}
						// Bucket not found proceed to delete such a job.
					}
				} else {
					if err := job.Replicate.Start(job.ctx, j.objLayer, *job); err != nil {
						if !isErrBucketNotFound(err) {
							logger.LogIf(j.ctx, err)
							j.canceler(job.ID, false)
							continue
						}
						// Bucket not found proceed to delete such a job.
					}
				}
			case job.KeyRotate != nil:
				if err := job.KeyRotate.Start(job.ctx, j.objLayer, *job); err != nil {
					if !isErrBucketNotFound(err) {
						logger.LogIf(j.ctx, err)
						continue
					}
				}
			case job.Expire != nil:
				if err := job.Expire.Start(job.ctx, j.objLayer, *job); err != nil {
					if !isErrBucketNotFound(err) {
						logger.LogIf(j.ctx, err)
						continue
					}
				}
			}
			job.delete(j.ctx, j.objLayer)
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
			close(j.jobCh)
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
	delete(j.jobCancelers, jobID)
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
			Bucket:           ri.Bucket,
			Object:           ri.Object,
			Objects:          ri.Objects,
			ObjectsFailed:    ri.ObjectsFailed,
			BytesTransferred: ri.BytesTransferred,
			BytesFailed:      ri.BytesFailed,
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
			Bucket:        ri.Bucket,
			Object:        ri.Object,
			Objects:       ri.Objects,
			ObjectsFailed: ri.ObjectsFailed,
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
				if time.Since(metrics.LastUpdate) > 24*time.Hour && (metrics.Complete || metrics.Failed) {
					toDeleteJobMetrics = append(toDeleteJobMetrics, id)
				}
			}
			m.RUnlock()
			for _, jobID := range toDeleteJobMetrics {
				m.delete(jobID)
			}
		}
	}
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

func lookupStyle(s string) miniogo.BucketLookupType {
	var lookup miniogo.BucketLookupType
	switch s {
	case "on":
		lookup = miniogo.BucketLookupPath
	case "off":
		lookup = miniogo.BucketLookupDNS
	default:
		lookup = miniogo.BucketLookupAuto

	}
	return lookup
}
