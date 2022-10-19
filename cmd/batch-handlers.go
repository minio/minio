// Copyright (c) 2015-2022 MinIO, Inc.
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
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/madmin-go"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/console"
	iampolicy "github.com/minio/pkg/iam/policy"
	"github.com/minio/pkg/wildcard"
	"gopkg.in/yaml.v2"
)

// replicate:
//   # source of the objects to be replicated
//   source:
//     type: "minio"
//     bucket: "testbucket"
//     prefix: "spark/"
//
//   # optional flags based filtering criteria
//   # for source objects
//   flags:
//     filter:
//       newerThan: "7d"
//       olderThan: "7d"
//       createdAfter: "date"
//       createdBefore: "date"
//       tags:
//         - key: "name"
//           value: "value*"
//       metadata:
//         - key: "content-type"
//           value: "image/*"
//     notify:
//       endpoint: "https://splunk-hec.dev.com"
//       token: "Splunk ..." # e.g. "Bearer token"
//
//   # target where the objects must be replicated
//   target:
//     type: "minio"
//     bucket: "testbucket1"
//     endpoint: "https://play.min.io"
//     credentials:
//       accessKey: "minioadmin"
//       secretKey: "minioadmin"
//       sessionToken: ""

// BatchJobReplicateKV is a datatype that holds key and values for filtering of objects
// used by metadata filter as well as tags based filtering.
type BatchJobReplicateKV struct {
	Key   string `yaml:"key" json:"key"`
	Value string `yaml:"value" json:"value"`
}

// Validate returns an error if key is empty
func (kv BatchJobReplicateKV) Validate() error {
	if kv.Key == "" {
		return errInvalidArgument
	}
	return nil
}

// Empty indicates if kv is not set
func (kv BatchJobReplicateKV) Empty() bool {
	return kv.Key == "" && kv.Value == ""
}

// Match matches input kv with kv, value will be wildcard matched depending on the user input
func (kv BatchJobReplicateKV) Match(ikv BatchJobReplicateKV) bool {
	if kv.Empty() {
		return true
	}
	if strings.EqualFold(kv.Key, ikv.Key) {
		return wildcard.Match(kv.Value, ikv.Value)
	}
	return false
}

// BatchReplicateRetry datatype represents total retry attempts and delay between each retries.
type BatchReplicateRetry struct {
	Attempts int           `yaml:"attempts" json:"attempts"` // number of retry attempts
	Delay    time.Duration `yaml:"delay" json:"delay"`       // delay between each retries
}

// Validate validates input replicate retries.
func (r BatchReplicateRetry) Validate() error {
	if r.Attempts < 0 {
		return errInvalidArgument
	}

	if r.Delay < 0 {
		return errInvalidArgument
	}

	return nil
}

// BatchReplicateFilter holds all the filters currently supported for batch replication
type BatchReplicateFilter struct {
	NewerThan     time.Duration         `yaml:"newerThan,omitempty" json:"newerThan"`
	OlderThan     time.Duration         `yaml:"olderThan,omitempty" json:"olderThan"`
	CreatedAfter  time.Time             `yaml:"createdAfter,omitempty" json:"createdAfter"`
	CreatedBefore time.Time             `yaml:"createdBefore,omitempty" json:"createdBefore"`
	Tags          []BatchJobReplicateKV `yaml:"tags,omitempty" json:"tags"`
	Metadata      []BatchJobReplicateKV `yaml:"metadata,omitempty" json:"metadata"`
}

// BatchReplicateNotification success or failure notification endpoint for each job attempts
type BatchReplicateNotification struct {
	Endpoint string `yaml:"endpoint" json:"endpoint"`
	Token    string `yaml:"token" json:"token"`
}

// BatchJobReplicateFlags various configurations for replication job definition currently includes
// - filter
// - notify
// - retry
type BatchJobReplicateFlags struct {
	Filter BatchReplicateFilter       `yaml:"filter" json:"filter"`
	Notify BatchReplicateNotification `yaml:"notify" json:"notify"`
	Retry  BatchReplicateRetry        `yaml:"retry" json:"retry"`
}

// BatchJobReplicateResourceType defines the type of batch jobs
type BatchJobReplicateResourceType string

// Validate validates if the replicate resource type is recognized and supported
func (t BatchJobReplicateResourceType) Validate() error {
	switch t {
	case BatchJobReplicateResourceMinIO:
	default:
		return errInvalidArgument
	}
	return nil
}

// Different types of batch jobs..
const (
	BatchJobReplicateResourceMinIO BatchJobReplicateResourceType = "minio"
	// add future targets
)

// BatchJobReplicateCredentials access credentials for batch replication it may
// be either for target or source.
type BatchJobReplicateCredentials struct {
	AccessKey    string `xml:"AccessKeyId" json:"accessKey,omitempty" yaml:"accessKey"`
	SecretKey    string `xml:"SecretAccessKey" json:"secretKey,omitempty" yaml:"secretKey"`
	SessionToken string `xml:"SessionToken" json:"sessionToken,omitempty" yaml:"sessionToken"`
}

// Validate validates if credentials are valid
func (c BatchJobReplicateCredentials) Validate() error {
	if !auth.IsAccessKeyValid(c.AccessKey) || !auth.IsSecretKeyValid(c.SecretKey) {
		return errInvalidArgument
	}
	return nil
}

// BatchJobReplicateTarget describes target element of the replication job that receives
// the filtered data from source
type BatchJobReplicateTarget struct {
	Type     BatchJobReplicateResourceType `yaml:"type" json:"type"`
	Bucket   string                        `yaml:"bucket" json:"bucket"`
	Prefix   string                        `yaml:"prefix" json:"prefix"`
	Endpoint string                        `yaml:"endpoint" json:"endpoint"`
	Creds    BatchJobReplicateCredentials  `yaml:"credentials" json:"credentials"`
}

// BatchJobReplicateSource describes source element of the replication job that is
// the source of the data for the target
type BatchJobReplicateSource struct {
	Type     BatchJobReplicateResourceType `yaml:"type" json:"type"`
	Bucket   string                        `yaml:"bucket" json:"bucket"`
	Prefix   string                        `yaml:"prefix" json:"prefix"`
	Endpoint string                        `yaml:"endpoint" json:"endpoint"`
	Creds    BatchJobReplicateCredentials  `yaml:"credentials" json:"credentials"`
}

// BatchJobReplicateV1 v1 of batch job replication
type BatchJobReplicateV1 struct {
	APIVersion string                  `yaml:"apiVersion" json:"apiVersion"`
	Flags      BatchJobReplicateFlags  `yaml:"flags" json:"flags"`
	Target     BatchJobReplicateTarget `yaml:"target" json:"target"`
	Source     BatchJobReplicateSource `yaml:"source" json:"source"`

	clnt *miniogo.Core `msg:"-"`
}

// BatchJobRequest this is an internal data structure not for external consumption.
type BatchJobRequest struct {
	ID        string               `yaml:"-" json:"name"`
	User      string               `yaml:"-" json:"user"`
	Started   time.Time            `yaml:"-" json:"started"`
	Location  string               `yaml:"-" json:"location"`
	Replicate *BatchJobReplicateV1 `yaml:"replicate" json:"replicate"`
}

// Notify notifies notification endpoint if configured regarding job failure or success.
func (r BatchJobReplicateV1) Notify(ctx context.Context, body io.Reader) error {
	if r.Flags.Notify.Endpoint == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Flags.Notify.Endpoint, body)
	if err != nil {
		return err
	}

	if r.Flags.Notify.Token != "" {
		req.Header.Set("Authorization", r.Flags.Notify.Token)
	}

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

// ReplicateFromSource - this is not implemented yet where source is 'remote' and target is local.
func (r *BatchJobReplicateV1) ReplicateFromSource(ctx context.Context, api ObjectLayer, c *miniogo.Core, srcObject string) error {
	return nil
}

// ReplicateToTarget read from source and replicate to configured target
func (r *BatchJobReplicateV1) ReplicateToTarget(ctx context.Context, api ObjectLayer, c *miniogo.Core, srcObjInfo ObjectInfo, retry bool) error {
	srcBucket := r.Source.Bucket
	tgtBucket := r.Target.Bucket
	tgtPrefix := r.Target.Prefix
	srcObject := srcObjInfo.Name

	if srcObjInfo.DeleteMarker || !srcObjInfo.VersionPurgeStatus.Empty() {
		if retry {
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

	if retry { // when we are retrying avoid copying if necessary.
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
	rd, err := api.GetObjectNInfo(ctx, srcBucket, srcObject, nil, http.Header{}, readLock, opts)
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
	data, err := readConfig(ctx, api, pathJoin(job.Location, batchReplName))
	if err != nil {
		if errors.Is(err, errConfigNotFound) || isErrObjectNotFound(err) {
			ri.Version = batchReplVersionV1
			if job.Replicate.Flags.Retry.Attempts > 0 {
				ri.RetryAttempts = job.Replicate.Flags.Retry.Attempts
			} else {
				ri.RetryAttempts = batchReplJobDefaultRetries
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
		return fmt.Errorf("batchRepl: no data")
	}
	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case batchReplFormat:
	default:
		return fmt.Errorf("batchRepl: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case batchReplVersion:
	default:
		return fmt.Errorf("batchRepl: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}

	// OK, parse data.
	if _, err = ri.UnmarshalMsg(data[4:]); err != nil {
		return err
	}

	switch ri.Version {
	case batchReplVersionV1:
	default:
		return fmt.Errorf("unexpected batch repl meta version: %d", ri.Version)
	}

	return nil
}

func (ri batchJobInfo) clone() batchJobInfo {
	return batchJobInfo{
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

func (ri batchJobInfo) save(ctx context.Context, api ObjectLayer, jobLocation string) error {
	data := make([]byte, 4, ri.Msgsize()+4)

	// Initialize the header.
	binary.LittleEndian.PutUint16(data[0:2], batchReplFormat)
	binary.LittleEndian.PutUint16(data[2:4], batchReplVersion)

	buf, err := ri.MarshalMsg(data)
	if err != nil {
		return err
	}

	return saveConfig(ctx, api, pathJoin(jobLocation, batchReplName), buf)
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

func (ri *batchJobInfo) updateAfter(ctx context.Context, api ObjectLayer, duration time.Duration, jobLocation string) error {
	if ri == nil {
		return errInvalidArgument
	}
	now := UTCNow()
	if now.Sub(ri.LastUpdate) >= duration {
		if serverDebugLog {
			console.Debugf("batchReplicate: persisting batchReplication info on drive: threshold:%s, batchRepl:%#v\n", now.Sub(ri.LastUpdate), ri)
		}
		ri.LastUpdate = now
		ri.Version = batchReplVersionV1
		return ri.save(ctx, api, jobLocation)
	}
	return nil
}

func (ri *batchJobInfo) trackCurrentBucketObject(bucket string, info ObjectInfo, failed bool) {
	if ri == nil {
		return
	}
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
	globalBatchJobsMetrics.save(job.ID, ri.clone())
	lastObject := ri.Object

	delay := job.Replicate.Flags.Retry.Delay
	if delay == 0 {
		delay = batchReplJobDefaultRetryDelay
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	skip := func(info FileInfo) (ok bool) {
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
					if kv.Match(BatchJobReplicateKV{Key: t, Value: v}) {
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
					if !strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") && !isStandardHeader(k) {
						continue
					}
					// We only need to match x-amz-meta or standardHeaders
					if kv.Match(BatchJobReplicateKV{Key: k, Value: v}) {
						return true
					}
				}
			}

			// None of the provided metadata filters match skip the object.
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
		Creds:     credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:    u.Scheme == "https",
		Transport: getRemoteInstanceTransport,
	})
	if err != nil {
		return err
	}

	retryAttempts := ri.RetryAttempts
	retry := false
	for attempts := 1; attempts <= retryAttempts; attempts++ {
		ctx, cancel := context.WithCancel(ctx)

		results := make(chan ObjectInfo, 100)
		if err := api.Walk(ctx, r.Source.Bucket, r.Source.Prefix, results, ObjectOptions{
			WalkMarker: lastObject,
			WalkFilter: skip,
		}); err != nil {
			cancel()
			// Do not need to retry if we can't list objects on source.
			return err
		}

		for result := range results {
			stopFn := globalBatchJobsMetrics.trace(batchReplicationMetricObject, job.ID, attempts, result)
			success := true
			if err := r.ReplicateToTarget(ctx, api, c, result, retry); err != nil {
				if isErrVersionNotFound(err) || isErrObjectNotFound(err) {
					// object must be deleted concurrently, allow
					// these failures but do not count them
					continue
				}
				stopFn(err)
				logger.LogIf(ctx, err)
				success = false
			} else {
				stopFn(nil)
			}
			ri.trackCurrentBucketObject(r.Source.Bucket, result, success)
			globalBatchJobsMetrics.save(job.ID, ri.clone())
			// persist in-memory state to disk after every 10secs.
			logger.LogIf(ctx, ri.updateAfter(ctx, api, 10*time.Second, job.Location))
		}

		ri.RetryAttempts = attempts
		ri.Complete = ri.ObjectsFailed == 0
		ri.Failed = ri.ObjectsFailed > 0

		globalBatchJobsMetrics.save(job.ID, ri.clone())

		buf, _ := json.Marshal(ri)
		if err := r.Notify(ctx, bytes.NewReader(buf)); err != nil {
			logger.LogIf(ctx, fmt.Errorf("Unable to notify %v", err))
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
func (r *BatchJobReplicateV1) Validate(ctx context.Context, o ObjectLayer) error {
	if r == nil {
		return nil
	}

	if r.APIVersion != batchReplJobAPIVersion {
		return errInvalidArgument
	}

	if r.Source.Bucket == "" {
		return errInvalidArgument
	}

	info, err := o.GetBucketInfo(ctx, r.Source.Bucket, BucketOptions{})
	if err != nil {
		if isErrBucketNotFound(err) {
			return batchReplicationJobError{
				Code:           "NoSuchSourceBucket",
				Description:    "The specified source bucket does not exist",
				HTTPStatusCode: http.StatusNotFound,
			}
		}
		return err
	}

	if err := r.Source.Type.Validate(); err != nil {
		return err
	}

	if r.Target.Endpoint == "" {
		return errInvalidArgument
	}

	if r.Target.Bucket == "" {
		return errInvalidArgument
	}

	if err := r.Target.Creds.Validate(); err != nil {
		return err
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

	u, err := url.Parse(r.Target.Endpoint)
	if err != nil {
		return err
	}

	cred := r.Target.Creds

	c, err := miniogo.NewCore(u.Host, &miniogo.Options{
		Creds:     credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:    u.Scheme == "https",
		Transport: getRemoteInstanceTransport,
	})
	if err != nil {
		return err
	}

	vcfg, err := c.GetBucketVersioning(ctx, r.Target.Bucket)
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

	if info.Versioning && !vcfg.Enabled() {
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
	if j.Replicate != nil {
		return madmin.BatchJobReplicate
	}
	return madmin.BatchJobType("unknown")
}

// Validate validates the current job, used by 'save()' before
// persisting the job request
func (j BatchJobRequest) Validate(ctx context.Context, o ObjectLayer) error {
	if j.Replicate != nil {
		return j.Replicate.Validate(ctx, o)
	}
	return errInvalidArgument
}

func (j BatchJobRequest) delete(ctx context.Context, api ObjectLayer) {
	if j.Replicate != nil {
		deleteConfig(ctx, api, pathJoin(j.Location, batchReplName))
	}
	globalBatchJobsMetrics.delete(j.ID)
	deleteConfig(ctx, api, j.Location)
}

func (j *BatchJobRequest) save(ctx context.Context, api ObjectLayer) error {
	if j.Replicate == nil {
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
		SourceVersionID: objInfo.VersionID,
		SourceMTime:     objInfo.ModTime,
		SourceETag:      objInfo.ETag,
	}
	return putOpts, nil
}

// ListBatchJobs - lists all currently active batch jobs, optionally takes {jobType}
// input to list only active batch jobs of 'jobType'
func (a adminAPIHandlers) ListBatchJobs(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListBatchJobs")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ListBatchJobsAction)
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

	if err := objectAPI.Walk(ctx, minioMetaBucket, batchJobPrefix, resultCh, ObjectOptions{}); err != nil {
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
	ctx := newContext(r, w, "DescribeBatchJob")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.DescribeBatchJobAction)
	if objectAPI == nil {
		return
	}

	id := r.Form.Get("jobId")
	if id == "" {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, errInvalidArgument), r.URL)
		return
	}

	req := &BatchJobRequest{}
	if err := req.load(ctx, objectAPI, pathJoin(batchJobPrefix, id)); err != nil {
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
	ctx := newContext(r, w, "StartBatchJob")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, creds := validateAdminReq(ctx, w, r, iampolicy.StartBatchJobAction)
	if objectAPI == nil {
		return
	}

	buf, err := io.ReadAll(r.Body)
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

	job.ID = shortuuid.New()
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

//msgp:ignore BatchJobPool

// BatchJobPool batch job pool
type BatchJobPool struct {
	ctx          context.Context
	objLayer     ObjectLayer
	once         sync.Once
	mu           sync.Mutex
	jobCh        chan *BatchJobRequest
	workerKillCh chan struct{}
	workerWg     sync.WaitGroup
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
	}
	jpool.ResizeWorkers(workers)
	jpool.resume()
	return jpool
}

func (j *BatchJobPool) resume() {
	results := make(chan ObjectInfo, 100)
	ctx, cancel := context.WithCancel(j.ctx)
	defer cancel()
	if err := j.objLayer.Walk(ctx, minioMetaBucket, batchJobPrefix, results, ObjectOptions{}); err != nil {
		logger.LogIf(j.ctx, err)
		return
	}
	for result := range results {
		req := &BatchJobRequest{}
		if err := req.load(ctx, j.objLayer, result.Name); err != nil {
			logger.LogIf(ctx, err)
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
	defer j.workerWg.Done()
	for {
		select {
		case <-j.ctx.Done():
			return
		case job, ok := <-j.jobCh:
			if !ok {
				return
			}
			if job.Replicate != nil {
				if err := job.Replicate.Start(j.ctx, j.objLayer, *job); err != nil {
					if !isErrBucketNotFound(err) {
						logger.LogIf(j.ctx, err)
						continue
					}
					// Bucket not found proceed to delete such a job.
				}
			}
			job.delete(j.ctx, j.objLayer)
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
		j.workerWg.Add(1)
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

//msgp:ignore batchJobMetrics
type batchJobMetrics struct {
	sync.RWMutex
	metrics map[string]batchJobInfo
}

var globalBatchJobsMetrics = batchJobMetrics{
	metrics: make(map[string]batchJobInfo),
}

//msgp:ignore batchReplicationMetric
//go:generate stringer -type=batchReplicationMetric -trimprefix=batchReplicationMetric $GOFILE
type batchReplicationMetric uint8

const (
	batchReplicationMetricObject batchReplicationMetric = iota
)

func batchReplicationTrace(d batchReplicationMetric, job string, startTime time.Time, duration time.Duration, info ObjectInfo, attempts int, err error) madmin.TraceInfo {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	funcName := fmt.Sprintf("batchReplication.%s (job-name=%s)", d.String(), job)
	if attempts > 0 {
		funcName = fmt.Sprintf("batchReplication.%s (job-name=%s,attempts=%s)", d.String(), job, humanize.Ordinal(attempts))
	}
	return madmin.TraceInfo{
		TraceType: madmin.TraceBatchReplication,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  funcName,
		Duration:  duration,
		Path:      info.Name,
		Error:     errStr,
	}
}

func (m *batchJobMetrics) report(jobID string) (metrics *madmin.BatchJobMetrics) {
	metrics = &madmin.BatchJobMetrics{CollectedAt: time.Now(), Jobs: make(map[string]madmin.JobMetric)}
	m.RLock()
	defer m.RUnlock()
	for id, job := range m.metrics {
		match := jobID != "" && id == jobID
		metrics.Jobs[id] = madmin.JobMetric{
			JobID:         job.JobID,
			JobType:       job.JobType,
			StartTime:     job.StartTime,
			LastUpdate:    job.LastUpdate,
			RetryAttempts: job.RetryAttempts,
			Complete:      job.Complete,
			Failed:        job.Failed,
			Replicate: &madmin.ReplicateInfo{
				Bucket:           job.Bucket,
				Object:           job.Object,
				Objects:          job.Objects,
				ObjectsFailed:    job.ObjectsFailed,
				BytesTransferred: job.BytesTransferred,
				BytesFailed:      job.BytesFailed,
			},
		}
		if match {
			break
		}
	}
	return metrics
}

func (m *batchJobMetrics) delete(jobID string) {
	m.Lock()
	defer m.Unlock()

	delete(m.metrics, jobID)
}

func (m *batchJobMetrics) save(jobID string, ri batchJobInfo) {
	m.Lock()
	defer m.Unlock()

	m.metrics[jobID] = ri
}

func (m *batchJobMetrics) trace(d batchReplicationMetric, job string, attempts int, info ObjectInfo) func(err error) {
	startTime := time.Now()
	return func(err error) {
		duration := time.Since(startTime)
		if globalTrace.NumSubscribers(madmin.TraceBatchReplication) > 0 {
			globalTrace.Publish(batchReplicationTrace(d, job, startTime, duration, info, attempts, err))
		}
	}
}
