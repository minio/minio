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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7/pkg/tags"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/env"
	"github.com/minio/pkg/wildcard"
	"github.com/minio/pkg/workers"
	"gopkg.in/yaml.v3"
)

// expire:
//   apiVersion: v1
//   bucket: BUCKET
//   prefix: PREFIX
//   # optional flags
//   flags:
//     filter:
//       olderThan: "7d" # match objects older than this value (e.g. 7d10h31s)
//       createdBefore: "date" # match objects created before "date"
//       tags:
//         - key: "name"
//           value: "pick*" # match objects with tag 'name', all values starting with 'pick'
//       metadata:
//         - key: "content-type"
//           value: "image/*" # match objects with 'content-type', all values starting with 'image/'
//       size:
//         lesserThan:  "" # match objects with size lesser than this value (e.g. 10KiB)
//         greaterThan: "" # match objects with size greater than this value (e.g. 10KiB)
//       name:
//         endsWith:   "" # match objects ending with this string
//         contains:   "" # match objects that contain this string
//         startsWith: "" # match objects starting with this string
//       # number of versions to retain
//       # e.g.
//       # maxVersions: 0  # expire all versions (currently implemented)
//       # maxVersions: 1  # expire all versions except latest
//       # maxVersions: 10 # expire all versions higher than 10
//       maxVersions: 0
//     notify:
//       endpoint: "https://notify.endpoint" # notification endpoint to receive job completion status
//       token: "Bearer xxxxx" # optional authentication token for the notification endpoint
//     retry:
//       attempts: 10 # number of retries for the job before giving up
//       delay: "500ms" # least amount of delay between each retry

//go:generate msgp -file $GOFILE -unexported

// BatchExpireKV is a datatype that holds key and values for filtering of objects
// used by metadata filter as well as tags based filtering.
type BatchExpireKV struct {
	Key   string `yaml:"key" json:"key"`
	Value string `yaml:"value" json:"value"`
}

// Validate returns an error if key is empty
func (kv BatchExpireKV) Validate() error {
	if kv.Key == "" {
		return errInvalidArgument
	}
	return nil
}

// Empty indicates if kv is not set
func (kv BatchExpireKV) Empty() bool {
	return kv.Key == "" && kv.Value == ""
}

// Match matches input kv with kv, value will be wildcard matched depending on the user input
func (kv BatchExpireKV) Match(ikv BatchExpireKV) bool {
	if kv.Empty() {
		return true
	}
	if strings.EqualFold(kv.Key, ikv.Key) {
		return wildcard.Match(kv.Value, ikv.Value)
	}
	return false
}

// BatchExpireRetry datatype represents total retry attempts and delay between each retries.
type BatchExpireRetry struct {
	Attempts int           `yaml:"attempts" json:"attempts"` // number of retry attempts
	Delay    time.Duration `yaml:"delay" json:"delay"`       // delay between each retries
}

// Validate validates input replicate retries.
func (r BatchExpireRetry) Validate() error {
	if r.Attempts < 0 {
		return errInvalidArgument
	}

	if r.Delay < 0 {
		return errInvalidArgument
	}

	return nil
}

type BatchExpireSize uint64

func (s BatchExpireSize) LesserThan(sz int64) bool {
	return sz < int64(s)
}

func (s BatchExpireSize) GreaterThan(sz int64) bool {
	return sz > int64(s)
}

func (s *BatchExpireSize) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag == "!!null" {
		*s = 0
	} else {
		sz, err := humanize.ParseBytes(node.Value)
		if err != nil {
			return err
		}
		*s = BatchExpireSize(sz)
	}
	return nil
}

// BatchExpireFilter holds all the filters currently supported for batch replication
type BatchExpireFilter struct {
	OlderThan     time.Duration   `yaml:"olderThan,omitempty" json:"olderThan"`
	CreatedBefore time.Time       `yaml:"createdBefore,omitempty" json:"createdBefore"`
	Tags          []BatchExpireKV `yaml:"tags,omitempty" json:"tags"`
	Metadata      []BatchExpireKV `yaml:"metadata,omitempty" json:"metadata"`
	Size          struct {
		LesserThan  BatchExpireSize `yaml:"lesserThan,omitempty" json:"lesserThan"`
		GreaterThan BatchExpireSize `yaml:"greaterThan,omitempty" json:"greaterThan"`
	}
	Name struct {
		StartsWith string `yaml:"startsWith,omitempty" json:"startsWith"`
		EndsWith   string `yaml:"endsWith,omitempty" json:"endsWith"`
		Contains   string `yaml:"contains,omitempty" json:"contains"`
	}
	MaxVersions int `yaml:"maxVersions,omitempty" json:"maxVersions"`
}

// BatchExpireNotification success or failure notification endpoint for each job attempts
type BatchExpireNotification struct {
	Endpoint string `yaml:"endpoint" json:"endpoint"`
	Token    string `yaml:"token" json:"token"`
}

// BatchJobExpireFlags various configurations for replication job definition currently includes
// - filter
// - notify
// - retry
type BatchJobExpireFlags struct {
	Filter BatchExpireFilter       `yaml:"filter" json:"filter"`
	Notify BatchExpireNotification `yaml:"notify" json:"notify"`
	Retry  BatchExpireRetry        `yaml:"retry" json:"retry"`
}

// BatchJobExpireV1 v1 of batch key rotation job
type BatchJobExpireV1 struct {
	APIVersion string              `yaml:"apiVersion" json:"apiVersion"`
	Flags      BatchJobExpireFlags `yaml:"flags" json:"flags"`
	Bucket     string              `yaml:"bucket" json:"bucket"`
	Prefix     string              `yaml:"prefix" json:"prefix"`
}

// Notify notifies notification endpoint if configured regarding job failure or success.
func (r BatchJobExpireV1) Notify(ctx context.Context, body io.Reader) error {
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

// Expire rotates encryption key of an object
func (r *BatchJobExpireV1) Expire(ctx context.Context, api ObjectLayer, objInfo ObjectInfo) error {
	srcBucket := r.Bucket
	srcObject := objInfo.Name

	_, err := api.DeleteObject(ctx, srcBucket, srcObject, ObjectOptions{
		DeletePrefix: true,
	})
	return err
}

const (
	batchExpireName                 = "batch-expire.bin"
	batchExpireFormat               = 1
	batchExpireVersionV1            = 1
	batchExpireVersion              = batchExpireVersionV1
	batchExpireAPIVersion           = "v1"
	batchExpireJobDefaultRetries    = 3
	batchExpireJobDefaultRetryDelay = 250 * time.Millisecond
)

// Start the batch key rottion job, resumes if there was a pending job via "job.ID"
func (r *BatchJobExpireV1) Start(ctx context.Context, api ObjectLayer, job BatchJobRequest) error {
	ri := &batchJobInfo{
		JobID:     job.ID,
		JobType:   string(job.Type()),
		StartTime: job.Started,
	}
	if err := ri.load(ctx, api, job); err != nil {
		return err
	}

	globalBatchJobsMetrics.save(job.ID, ri)
	lastObject := ri.Object

	delay := job.Expire.Flags.Retry.Delay
	if delay == 0 {
		delay = batchExpireJobDefaultRetryDelay
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	skip := func(info FileInfo) (ok bool) {
		if r.Flags.Filter.OlderThan > 0 && time.Since(info.ModTime) < r.Flags.Filter.OlderThan {
			// skip all objects that are newer than specified older duration
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
					if kv.Match(BatchExpireKV{Key: t, Value: v}) {
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
					if kv.Match(BatchExpireKV{Key: k, Value: v}) {
						return true
					}
				}
			}

			// None of the provided metadata filters match skip the object.
			return false
		}

		if r.Flags.Filter.Size.LesserThan > 0 {
			if !r.Flags.Filter.Size.LesserThan.LesserThan(info.Size) {
				return false
			}
		}

		if r.Flags.Filter.Size.GreaterThan > 0 {
			if !r.Flags.Filter.Size.GreaterThan.GreaterThan(info.Size) {
				return false
			}
		}

		if r.Flags.Filter.Name.StartsWith != "" {
			if !strings.HasPrefix(info.Name, r.Flags.Filter.Name.StartsWith) {
				return false
			}
		}

		if r.Flags.Filter.Name.EndsWith != "" {
			if !strings.HasSuffix(info.Name, r.Flags.Filter.Name.EndsWith) {
				return false
			}
		}

		if r.Flags.Filter.Name.Contains != "" {
			if !strings.Contains(info.Name, r.Flags.Filter.Name.Contains) {
				return false
			}
		}

		return true
	}

	workerSize, err := strconv.Atoi(env.Get("_MINIO_BATCH_EXPIRE_WORKERS", strconv.Itoa(runtime.GOMAXPROCS(0)/2)))
	if err != nil {
		return err
	}

	wk, err := workers.New(workerSize)
	if err != nil {
		// invalid worker size.
		return err
	}

	retryAttempts := ri.RetryAttempts
	ctx, cancel := context.WithCancel(ctx)

	results := make(chan ObjectInfo, workerSize)
	if err := api.Walk(ctx, r.Bucket, r.Prefix, results, ObjectOptions{
		WalkMarker:     lastObject,
		WalkFilter:     skip,
		WalkLatestOnly: r.Flags.Filter.MaxVersions == 0, // We are only interested in latest version of the object
	}); err != nil {
		cancel()
		// Do not need to retry if we can't list objects on source.
		return err
	}

	for result := range results {
		result := result
		wk.Take()
		go func() {
			defer wk.Give()
			for attempts := 1; attempts <= retryAttempts; attempts++ {
				attempts := attempts
				stopFn := globalBatchJobsMetrics.trace(batchJobMetricExpire, job.ID, attempts, result)
				success := true
				if err := r.Expire(ctx, api, result); err != nil {
					stopFn(err)
					logger.LogIf(ctx, err)
					success = false
				} else {
					stopFn(nil)
				}
				ri.trackCurrentBucketObject(r.Bucket, result, success)
				ri.RetryAttempts = attempts
				globalBatchJobsMetrics.save(job.ID, ri)
				// persist in-memory state to disk after every 10secs.
				logger.LogIf(ctx, ri.updateAfter(ctx, api, 10*time.Second, job))
				if success {
					break
				}
			}
		}()
	}
	wk.Wait()

	ri.Complete = ri.ObjectsFailed == 0
	ri.Failed = ri.ObjectsFailed > 0
	globalBatchJobsMetrics.save(job.ID, ri)
	// persist in-memory state to disk.
	logger.LogIf(ctx, ri.updateAfter(ctx, api, 0, job))

	buf, _ := json.Marshal(ri)
	if err := r.Notify(ctx, bytes.NewReader(buf)); err != nil {
		logger.LogIf(ctx, fmt.Errorf("unable to notify %v", err))
	}

	cancel()
	if ri.Failed {
		time.Sleep(delay + time.Duration(rnd.Float64()*float64(delay)))
	}

	return nil
}

//msgp:ignore batchExpireJobError
type batchExpireJobError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

func (e batchExpireJobError) Error() string {
	return e.Description
}

// Validate validates the job definition input
func (r *BatchJobExpireV1) Validate(ctx context.Context, job BatchJobRequest, o ObjectLayer) error {
	if r == nil {
		return nil
	}

	if r.APIVersion != batchExpireAPIVersion {
		return errInvalidArgument
	}

	if r.Bucket == "" {
		return errInvalidArgument
	}

	if _, err := o.GetBucketInfo(ctx, r.Bucket, BucketOptions{}); err != nil {
		if isErrBucketNotFound(err) {
			return batchExpireJobError{
				Code:           "NoSuchSourceBucket",
				Description:    "The specified source bucket does not exist",
				HTTPStatusCode: http.StatusNotFound,
			}
		}
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
	return nil
}
