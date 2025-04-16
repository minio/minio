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
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/bucket/versioning"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/wildcard"
	"github.com/minio/pkg/v3/workers"
	"github.com/minio/pkg/v3/xtime"
	"gopkg.in/yaml.v3"
)

// expire: # Expire objects that match a condition
//   apiVersion: v1
//   bucket: mybucket # Bucket where this batch job will expire matching objects from
//   prefix: myprefix # (Optional) Prefix under which this job will expire objects matching the rules below.
//   rules:
//     - type: object  # regular objects with zero or more older versions
//       name: NAME # match object names that satisfy the wildcard expression.
//       olderThan: 70h # match objects older than this value
//       createdBefore: "2006-01-02T15:04:05.00Z" # match objects created before "date"
//       tags:
//         - key: name
//           value: pick* # match objects with tag 'name', all values starting with 'pick'
//       metadata:
//         - key: content-type
//           value: image/* # match objects with 'content-type', all values starting with 'image/'
//       size:
//         lessThan: "10MiB" # match objects with size less than this value (e.g. 10MiB)
//         greaterThan: 1MiB # match objects with size greater than this value (e.g. 1MiB)
//       purge:
//           # retainVersions: 0 # (default) delete all versions of the object. This option is the fastest.
//           # retainVersions: 5 # keep the latest 5 versions of the object.
//
//     - type: deleted # objects with delete marker as their latest version
//       name: NAME # match object names that satisfy the wildcard expression.
//       olderThan: 10h # match objects older than this value (e.g. 7d10h31s)
//       createdBefore: "2006-01-02T15:04:05.00Z" # match objects created before "date"
//       purge:
//           # retainVersions: 0 # (default) delete all versions of the object. This option is the fastest.
//           # retainVersions: 5 # keep the latest 5 versions of the object including delete markers.
//
//   notify:
//     endpoint: https://notify.endpoint # notification endpoint to receive job completion status
//     token: Bearer xxxxx # optional authentication token for the notification endpoint
//
//   retry:
//     attempts: 10 # number of retries for the job before giving up
//     delay: 500ms # least amount of delay between each retry

//go:generate msgp -file $GOFILE

// BatchJobExpirePurge type accepts non-negative versions to be retained
type BatchJobExpirePurge struct {
	line, col      int
	RetainVersions int `yaml:"retainVersions" json:"retainVersions"`
}

var _ yaml.Unmarshaler = &BatchJobExpirePurge{}

// UnmarshalYAML - BatchJobExpirePurge extends unmarshal to extract line, col
func (p *BatchJobExpirePurge) UnmarshalYAML(val *yaml.Node) error {
	type purge BatchJobExpirePurge
	var tmp purge
	err := val.Decode(&tmp)
	if err != nil {
		return err
	}

	*p = BatchJobExpirePurge(tmp)
	p.line, p.col = val.Line, val.Column
	return nil
}

// Validate returns nil if value is valid, ie > 0.
func (p BatchJobExpirePurge) Validate() error {
	if p.RetainVersions < 0 {
		return BatchJobYamlErr{
			line: p.line,
			col:  p.col,
			msg:  "retainVersions must be >= 0",
		}
	}
	return nil
}

// BatchJobExpireFilter holds all the filters currently supported for batch replication
type BatchJobExpireFilter struct {
	line, col     int
	OlderThan     xtime.Duration      `yaml:"olderThan,omitempty" json:"olderThan"`
	CreatedBefore *time.Time          `yaml:"createdBefore,omitempty" json:"createdBefore"`
	Tags          []BatchJobKV        `yaml:"tags,omitempty" json:"tags"`
	Metadata      []BatchJobKV        `yaml:"metadata,omitempty" json:"metadata"`
	Size          BatchJobSizeFilter  `yaml:"size" json:"size"`
	Type          string              `yaml:"type" json:"type"`
	Name          string              `yaml:"name" json:"name"`
	Purge         BatchJobExpirePurge `yaml:"purge" json:"purge"`
}

var _ yaml.Unmarshaler = &BatchJobExpireFilter{}

// UnmarshalYAML - BatchJobExpireFilter extends unmarshal to extract line, col
// information
func (ef *BatchJobExpireFilter) UnmarshalYAML(value *yaml.Node) error {
	type expFilter BatchJobExpireFilter
	var tmp expFilter
	err := value.Decode(&tmp)
	if err != nil {
		return err
	}
	*ef = BatchJobExpireFilter(tmp)
	ef.line, ef.col = value.Line, value.Column
	return err
}

// Matches returns true if obj matches the filter conditions specified in ef.
func (ef BatchJobExpireFilter) Matches(obj ObjectInfo, now time.Time) bool {
	switch ef.Type {
	case BatchJobExpireObject:
		if obj.DeleteMarker {
			return false
		}
	case BatchJobExpireDeleted:
		if !obj.DeleteMarker {
			return false
		}
	default:
		// we should never come here, Validate should have caught this.
		batchLogOnceIf(context.Background(), fmt.Errorf("invalid filter type: %s", ef.Type), ef.Type)
		return false
	}

	if len(ef.Name) > 0 && !wildcard.Match(ef.Name, obj.Name) {
		return false
	}
	if ef.OlderThan > 0 && now.Sub(obj.ModTime) <= ef.OlderThan.D() {
		return false
	}

	if ef.CreatedBefore != nil && !obj.ModTime.Before(*ef.CreatedBefore) {
		return false
	}

	if len(ef.Tags) > 0 && !obj.DeleteMarker {
		// Only parse object tags if tags filter is specified.
		var tagMap map[string]string
		if len(obj.UserTags) != 0 {
			t, err := tags.ParseObjectTags(obj.UserTags)
			if err != nil {
				return false
			}
			tagMap = t.ToMap()
		}

		for _, kv := range ef.Tags {
			// Object (version) must match all tags specified in
			// the filter
			var match bool
			for t, v := range tagMap {
				if kv.Match(BatchJobKV{Key: t, Value: v}) {
					match = true
				}
			}
			if !match {
				return false
			}
		}
	}

	if len(ef.Metadata) > 0 && !obj.DeleteMarker {
		for _, kv := range ef.Metadata {
			// Object (version) must match all x-amz-meta and
			// standard metadata headers
			// specified in the filter
			var match bool
			for k, v := range obj.UserDefined {
				if !stringsHasPrefixFold(k, "x-amz-meta-") && !isStandardHeader(k) {
					continue
				}
				// We only need to match x-amz-meta or standardHeaders
				if kv.Match(BatchJobKV{Key: k, Value: v}) {
					match = true
				}
			}
			if !match {
				return false
			}
		}
	}

	return ef.Size.InRange(obj.Size)
}

const (
	// BatchJobExpireObject - object type
	BatchJobExpireObject string = "object"
	// BatchJobExpireDeleted - delete marker type
	BatchJobExpireDeleted string = "deleted"
)

// Validate returns nil if ef has valid fields, validation error otherwise.
func (ef BatchJobExpireFilter) Validate() error {
	switch ef.Type {
	case BatchJobExpireObject:
	case BatchJobExpireDeleted:
		if len(ef.Tags) > 0 || len(ef.Metadata) > 0 {
			return BatchJobYamlErr{
				line: ef.line,
				col:  ef.col,
				msg:  "delete type filter can't have tags or metadata",
			}
		}
	default:
		return BatchJobYamlErr{
			line: ef.line,
			col:  ef.col,
			msg:  "invalid batch-expire type",
		}
	}

	for _, tag := range ef.Tags {
		if err := tag.Validate(); err != nil {
			return err
		}
	}

	for _, meta := range ef.Metadata {
		if err := meta.Validate(); err != nil {
			return err
		}
	}
	if err := ef.Purge.Validate(); err != nil {
		return err
	}
	if err := ef.Size.Validate(); err != nil {
		return err
	}
	if ef.CreatedBefore != nil && !ef.CreatedBefore.Before(time.Now()) {
		return BatchJobYamlErr{
			line: ef.line,
			col:  ef.col,
			msg:  "CreatedBefore is in the future",
		}
	}
	return nil
}

// BatchJobExpire represents configuration parameters for a batch expiration
// job typically supplied in yaml form
type BatchJobExpire struct {
	line, col       int
	APIVersion      string                 `yaml:"apiVersion" json:"apiVersion"`
	Bucket          string                 `yaml:"bucket" json:"bucket"`
	Prefix          BatchJobPrefix         `yaml:"prefix" json:"prefix"`
	NotificationCfg BatchJobNotification   `yaml:"notify" json:"notify"`
	Retry           BatchJobRetry          `yaml:"retry" json:"retry"`
	Rules           []BatchJobExpireFilter `yaml:"rules" json:"rules"`
}

var _ yaml.Unmarshaler = &BatchJobExpire{}

// RedactSensitive will redact any sensitive information in b.
func (r *BatchJobExpire) RedactSensitive() {
	if r == nil {
		return
	}
	if r.NotificationCfg.Token != "" {
		r.NotificationCfg.Token = redactedText
	}
}

// UnmarshalYAML - BatchJobExpire extends default unmarshal to extract line, col information.
func (r *BatchJobExpire) UnmarshalYAML(val *yaml.Node) error {
	type expireJob BatchJobExpire
	var tmp expireJob
	err := val.Decode(&tmp)
	if err != nil {
		return err
	}

	*r = BatchJobExpire(tmp)
	r.line, r.col = val.Line, val.Column
	return nil
}

// Notify notifies notification endpoint if configured regarding job failure or success.
func (r BatchJobExpire) Notify(ctx context.Context, body io.Reader) error {
	if r.NotificationCfg.Endpoint == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.NotificationCfg.Endpoint, body)
	if err != nil {
		return err
	}

	if r.NotificationCfg.Token != "" {
		req.Header.Set("Authorization", r.NotificationCfg.Token)
	}

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

// Expire expires object versions which have already matched supplied filter conditions
func (r *BatchJobExpire) Expire(ctx context.Context, api ObjectLayer, vc *versioning.Versioning, objsToDel []ObjectToDelete) []error {
	opts := ObjectOptions{
		PrefixEnabledFn:  vc.PrefixEnabled,
		VersionSuspended: vc.Suspended(),
	}

	allErrs := make([]error, 0, len(objsToDel))

	for {
		count := len(objsToDel)
		if count == 0 {
			break
		}
		if count > maxDeleteList {
			count = maxDeleteList
		}
		_, errs := api.DeleteObjects(ctx, r.Bucket, objsToDel[:count], opts)
		allErrs = append(allErrs, errs...)
		// Next batch of deletion
		objsToDel = objsToDel[count:]
	}

	return allErrs
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

type objInfoCache map[string]*ObjectInfo

func newObjInfoCache() objInfoCache {
	return objInfoCache(make(map[string]*ObjectInfo))
}

func (oiCache objInfoCache) Add(toDel ObjectToDelete, oi *ObjectInfo) {
	oiCache[fmt.Sprintf("%s-%s", toDel.ObjectName, toDel.VersionID)] = oi
}

func (oiCache objInfoCache) Get(toDel ObjectToDelete) (*ObjectInfo, bool) {
	oi, ok := oiCache[fmt.Sprintf("%s-%s", toDel.ObjectName, toDel.VersionID)]
	return oi, ok
}

func batchObjsForDelete(ctx context.Context, r *BatchJobExpire, ri *batchJobInfo, job BatchJobRequest, api ObjectLayer, wk *workers.Workers, expireCh <-chan []expireObjInfo) {
	vc, _ := globalBucketVersioningSys.Get(r.Bucket)
	retryAttempts := job.Expire.Retry.Attempts
	if retryAttempts <= 0 {
		retryAttempts = batchExpireJobDefaultRetries
	}
	delay := job.Expire.Retry.Delay
	if delay <= 0 {
		delay = batchExpireJobDefaultRetryDelay
	}

	var i int
	for toExpire := range expireCh {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if i > 0 {
			if wait := globalBatchConfig.ExpirationWait(); wait > 0 {
				time.Sleep(wait)
			}
		}
		i++
		wk.Take()
		go func(toExpire []expireObjInfo) {
			defer wk.Give()

			toExpireAll := make([]expireObjInfo, 0, len(toExpire))
			toDel := make([]ObjectToDelete, 0, len(toExpire))
			oiCache := newObjInfoCache()
			for _, exp := range toExpire {
				if exp.ExpireAll {
					toExpireAll = append(toExpireAll, exp)
					continue
				}
				// Cache ObjectInfo value via pointers for
				// subsequent use to track objects which
				// couldn't be deleted.
				od := ObjectToDelete{
					ObjectV: ObjectV{
						ObjectName: exp.Name,
						VersionID:  exp.VersionID,
					},
				}
				toDel = append(toDel, od)
				oiCache.Add(od, &exp.ObjectInfo)
			}

			// DeleteObject(deletePrefix: true) to expire all versions of an object
			for _, exp := range toExpireAll {
				var success bool
				for attempts := 1; attempts <= retryAttempts; attempts++ {
					select {
					case <-ctx.Done():
						ri.trackMultipleObjectVersions(exp, success)
						return
					default:
					}
					stopFn := globalBatchJobsMetrics.trace(batchJobMetricExpire, ri.JobID, attempts)
					_, err := api.DeleteObject(ctx, exp.Bucket, encodeDirObject(exp.Name), ObjectOptions{
						DeletePrefix:       true,
						DeletePrefixObject: true, // use prefix delete on exact object (this is an optimization to avoid fan-out calls)
					})
					if err != nil {
						stopFn(exp, err)
						batchLogIf(ctx, fmt.Errorf("Failed to expire %s/%s due to %v (attempts=%d)", exp.Bucket, exp.Name, err, attempts))
					} else {
						stopFn(exp, err)
						success = true
						break
					}
				}
				ri.trackMultipleObjectVersions(exp, success)
			}

			// DeleteMultiple objects
			toDelCopy := make([]ObjectToDelete, len(toDel))
			for attempts := 1; attempts <= retryAttempts; attempts++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				stopFn := globalBatchJobsMetrics.trace(batchJobMetricExpire, ri.JobID, attempts)
				// Copying toDel to select from objects whose
				// deletion failed
				copy(toDelCopy, toDel)
				var failed int
				errs := r.Expire(ctx, api, vc, toDel)
				// reslice toDel in preparation for next retry attempt
				toDel = toDel[:0]
				for i, err := range errs {
					if err != nil {
						stopFn(toDelCopy[i], err)
						batchLogIf(ctx, fmt.Errorf("Failed to expire %s/%s versionID=%s due to %v (attempts=%d)", ri.Bucket, toDelCopy[i].ObjectName, toDelCopy[i].VersionID,
							err, attempts))
						failed++
						if oi, ok := oiCache.Get(toDelCopy[i]); ok {
							ri.trackCurrentBucketObject(r.Bucket, *oi, false, attempts)
						}
						if attempts != retryAttempts {
							// retry
							toDel = append(toDel, toDelCopy[i])
						}
					} else {
						stopFn(toDelCopy[i], nil)
						if oi, ok := oiCache.Get(toDelCopy[i]); ok {
							ri.trackCurrentBucketObject(r.Bucket, *oi, true, attempts)
						}
					}
				}

				globalBatchJobsMetrics.save(ri.JobID, ri)

				if failed == 0 {
					break
				}

				// Add a delay between retry attempts
				if attempts < retryAttempts {
					time.Sleep(delay)
				}
			}
		}(toExpire)
	}
}

type expireObjInfo struct {
	ObjectInfo
	ExpireAll         bool
	DeleteMarkerCount int64
}

// Start the batch expiration job, resumes if there was a pending job via "job.ID"
func (r *BatchJobExpire) Start(ctx context.Context, api ObjectLayer, job BatchJobRequest) error {
	ri := &batchJobInfo{
		JobID:     job.ID,
		JobType:   string(job.Type()),
		StartTime: job.Started,
	}
	if err := ri.loadOrInit(ctx, api, job); err != nil {
		return err
	}

	globalBatchJobsMetrics.save(job.ID, ri)
	lastObject := ri.Object

	now := time.Now().UTC()

	workerSize, err := strconv.Atoi(env.Get("_MINIO_BATCH_EXPIRATION_WORKERS", strconv.Itoa(runtime.GOMAXPROCS(0)/2)))
	if err != nil {
		return err
	}

	wk, err := workers.New(workerSize)
	if err != nil {
		// invalid worker size.
		return err
	}

	ctx, cancelCause := context.WithCancelCause(ctx)
	defer cancelCause(nil)

	results := make(chan itemOrErr[ObjectInfo], workerSize)
	go func() {
		prefixes := r.Prefix.F()
		if len(prefixes) == 0 {
			prefixes = []string{""}
		}
		for _, prefix := range prefixes {
			prefixResultCh := make(chan itemOrErr[ObjectInfo], workerSize)
			err := api.Walk(ctx, r.Bucket, prefix, prefixResultCh, WalkOptions{
				Marker:       lastObject,
				LatestOnly:   false, // we need to visit all versions of the object to implement purge: retainVersions
				VersionsSort: WalkVersionsSortDesc,
			})
			if err != nil {
				cancelCause(err)
				xioutil.SafeClose(results)
				return
			}
			for result := range prefixResultCh {
				results <- result
			}
		}
		xioutil.SafeClose(results)
	}()

	// Goroutine to periodically save batch-expire job's in-memory state
	saverQuitCh := make(chan struct{})
	go func() {
		saveTicker := time.NewTicker(10 * time.Second)
		defer saveTicker.Stop()
		quit := false
		after := time.Minute
		for !quit {
			select {
			case <-saveTicker.C:
			case <-ctx.Done():
				quit = true
			case <-saverQuitCh:
				quit = true
			}

			if quit {
				// save immediately if we are quitting
				after = 0
			}

			ctx, cancel := context.WithTimeout(GlobalContext, 30*time.Second) // independent context
			batchLogIf(ctx, ri.updateAfter(ctx, api, after, job))
			cancel()
		}
	}()

	expireCh := make(chan []expireObjInfo, workerSize)
	expireDoneCh := make(chan struct{})
	go func() {
		defer close(expireDoneCh)
		batchObjsForDelete(ctx, r, ri, job, api, wk, expireCh)
	}()

	var (
		prevObj       ObjectInfo
		matchedFilter BatchJobExpireFilter
		versionsCount int
		toDel         []expireObjInfo
		failed        bool
		done          bool
	)
	deleteMarkerCountMap := map[string]int64{}
	pushToExpire := func() {
		// set preObject deleteMarkerCount
		if len(toDel) > 0 {
			lastDelIndex := len(toDel) - 1
			lastDel := toDel[lastDelIndex]
			if lastDel.ExpireAll {
				toDel[lastDelIndex].DeleteMarkerCount = deleteMarkerCountMap[lastDel.Name]
				// delete the key
				delete(deleteMarkerCountMap, lastDel.Name)
			}
		}
		// send down filtered entries to be deleted using
		// DeleteObjects method
		if len(toDel) > 10 { // batch up to 10 objects/versions to be expired simultaneously.
			xfer := make([]expireObjInfo, len(toDel))
			copy(xfer, toDel)
			select {
			case expireCh <- xfer:
				toDel = toDel[:0] // resetting toDel
			case <-ctx.Done():
				done = true
			}
		}
	}
	for {
		select {
		case result, ok := <-results:
			if !ok {
				done = true
				break
			}
			if result.Err != nil {
				failed = true
				batchLogIf(ctx, result.Err)
				continue
			}
			if result.Item.DeleteMarker {
				deleteMarkerCountMap[result.Item.Name]++
			}
			// Apply filter to find the matching rule to apply expiry
			// actions accordingly.
			// nolint:gocritic
			if result.Item.IsLatest {
				var match BatchJobExpireFilter
				var found bool
				for _, rule := range r.Rules {
					if rule.Matches(result.Item, now) {
						match = rule
						found = true
						break
					}
				}
				if !found {
					continue
				}

				if prevObj.Name != result.Item.Name {
					// switch the object
					pushToExpire()
				}

				prevObj = result.Item
				matchedFilter = match
				versionsCount = 1
				// Include the latest version
				if matchedFilter.Purge.RetainVersions == 0 {
					toDel = append(toDel, expireObjInfo{
						ObjectInfo: result.Item,
						ExpireAll:  true,
					})
					continue
				}
			} else if prevObj.Name == result.Item.Name {
				if matchedFilter.Purge.RetainVersions == 0 {
					continue // including latest version in toDel suffices, skipping other versions
				}
				versionsCount++
			} else {
				// switch the object
				pushToExpire()
				// a file switched with no LatestVersion, logging it
				batchLogIf(ctx, fmt.Errorf("skipping object %s, no latest version found", result.Item.Name))
				continue
			}

			if versionsCount <= matchedFilter.Purge.RetainVersions {
				continue // retain versions
			}
			toDel = append(toDel, expireObjInfo{
				ObjectInfo: result.Item,
			})
			pushToExpire()
		case <-ctx.Done():
			done = true
		}
		if done {
			break
		}
	}

	if context.Cause(ctx) != nil {
		xioutil.SafeClose(expireCh)
		return context.Cause(ctx)
	}
	pushToExpire()
	// Send any remaining objects downstream
	if len(toDel) > 0 {
		select {
		case <-ctx.Done():
		case expireCh <- toDel:
		}
	}
	xioutil.SafeClose(expireCh)

	<-expireDoneCh // waits for the expire goroutine to complete
	wk.Wait()      // waits for all expire workers to retire

	ri.Complete = !failed && ri.ObjectsFailed == 0
	ri.Failed = failed || ri.ObjectsFailed > 0
	globalBatchJobsMetrics.save(job.ID, ri)

	// Close the saverQuitCh - this also triggers saving in-memory state
	// immediately one last time before we exit this method.
	xioutil.SafeClose(saverQuitCh)

	// Notify expire jobs final status to the configured endpoint
	buf, _ := json.Marshal(ri)
	if err := r.Notify(context.Background(), bytes.NewReader(buf)); err != nil {
		batchLogIf(context.Background(), fmt.Errorf("unable to notify %v", err))
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

// maxBatchRules maximum number of rules a batch-expiry job supports
const maxBatchRules = 50

// Validate validates the job definition input
func (r *BatchJobExpire) Validate(ctx context.Context, job BatchJobRequest, o ObjectLayer) error {
	if r == nil {
		return nil
	}

	if r.APIVersion != batchExpireAPIVersion {
		return batchExpireJobError{
			Code:           "InvalidArgument",
			Description:    "Unsupported batch expire API version",
			HTTPStatusCode: http.StatusBadRequest,
		}
	}

	if r.Bucket == "" {
		return batchExpireJobError{
			Code:           "InvalidArgument",
			Description:    "Bucket argument missing",
			HTTPStatusCode: http.StatusBadRequest,
		}
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

	if len(r.Rules) > maxBatchRules {
		return batchExpireJobError{
			Code:           "InvalidArgument",
			Description:    "Too many rules. Batch expire job can't have more than 100 rules",
			HTTPStatusCode: http.StatusBadRequest,
		}
	}

	for _, rule := range r.Rules {
		if err := rule.Validate(); err != nil {
			return batchExpireJobError{
				Code:           "InvalidArgument",
				Description:    fmt.Sprintf("Invalid batch expire rule: %s", err),
				HTTPStatusCode: http.StatusBadRequest,
			}
		}
	}

	if err := r.Retry.Validate(); err != nil {
		return batchExpireJobError{
			Code:           "InvalidArgument",
			Description:    fmt.Sprintf("Invalid batch expire retry configuration: %s", err),
			HTTPStatusCode: http.StatusBadRequest,
		}
	}
	return nil
}
