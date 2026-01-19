// Copyright (c) 2015-2024 MinIO, Inc.
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
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/amztime"
	sse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/s3select"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/zeebo/xxh3"
)

const (
	// Disabled means the lifecycle rule is inactive
	Disabled = "Disabled"
	// TransitionStatus status of transition
	TransitionStatus = "transition-status"
	// TransitionedObjectName name of transitioned object
	TransitionedObjectName = "transitioned-object"
	// TransitionedVersionID is version of remote object
	TransitionedVersionID = "transitioned-versionID"
	// TransitionTier name of transition storage class
	TransitionTier = "transition-tier"
)

// LifecycleSys - Bucket lifecycle subsystem.
type LifecycleSys struct{}

// Get - gets lifecycle config associated to a given bucket name.
func (sys *LifecycleSys) Get(bucketName string) (lc *lifecycle.Lifecycle, err error) {
	lc, _, err = globalBucketMetadataSys.GetLifecycleConfig(bucketName)
	return lc, err
}

// NewLifecycleSys - creates new lifecycle system.
func NewLifecycleSys() *LifecycleSys {
	return &LifecycleSys{}
}

func ilmTrace(startTime time.Time, duration time.Duration, oi ObjectInfo, event string, metadata map[string]string, err string) madmin.TraceInfo {
	sz, _ := oi.GetActualSize()
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["version-id"] = oi.VersionID
	return madmin.TraceInfo{
		TraceType: madmin.TraceILM,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  event,
		Duration:  duration,
		Path:      pathJoin(oi.Bucket, oi.Name),
		Bytes:     sz,
		Error:     err,
		Message:   getSource(4),
		Custom:    metadata,
	}
}

func (sys *LifecycleSys) trace(oi ObjectInfo) func(event string, metadata map[string]string, err error) {
	startTime := time.Now()
	return func(event string, metadata map[string]string, err error) {
		duration := time.Since(startTime)
		if globalTrace.NumSubscribers(madmin.TraceILM) > 0 {
			e := ""
			if err != nil {
				e = err.Error()
			}
			globalTrace.Publish(ilmTrace(startTime, duration, oi, event, metadata, e))
		}
	}
}

type expiryTask struct {
	objInfo ObjectInfo
	event   lifecycle.Event
	src     lcEventSrc
}

// expiryStats records metrics related to ILM expiry activities
type expiryStats struct {
	missedExpiryTasks      atomic.Int64
	missedFreeVersTasks    atomic.Int64
	missedTierJournalTasks atomic.Int64
	workers                atomic.Int32
}

// MissedTasks returns the number of ILM expiry tasks that were missed since
// there were no available workers.
func (e *expiryStats) MissedTasks() int64 {
	return e.missedExpiryTasks.Load()
}

// MissedFreeVersTasks returns the number of free version collection tasks that
// were missed since there were no available workers.
func (e *expiryStats) MissedFreeVersTasks() int64 {
	return e.missedFreeVersTasks.Load()
}

// MissedTierJournalTasks returns the number of tasks to remove tiered objects
// that were missed since there were no available workers.
func (e *expiryStats) MissedTierJournalTasks() int64 {
	return e.missedTierJournalTasks.Load()
}

// NumWorkers returns the number of active workers executing one of ILM expiry
// tasks or free version collection tasks.
func (e *expiryStats) NumWorkers() int32 {
	return e.workers.Load()
}

type expiryOp interface {
	OpHash() uint64
}

type freeVersionTask struct {
	ObjectInfo
}

func (f freeVersionTask) OpHash() uint64 {
	return xxh3.HashString(f.TransitionedObject.Tier + f.TransitionedObject.Name)
}

func (n noncurrentVersionsTask) OpHash() uint64 {
	return xxh3.HashString(n.bucket + n.versions[0].ObjectName)
}

func (j jentry) OpHash() uint64 {
	return xxh3.HashString(j.TierName + j.ObjName)
}

func (e expiryTask) OpHash() uint64 {
	return xxh3.HashString(e.objInfo.Bucket + e.objInfo.Name)
}

// expiryState manages all ILM related expiration activities.
type expiryState struct {
	mu      sync.RWMutex
	workers atomic.Pointer[[]chan expiryOp]

	ctx    context.Context
	objAPI ObjectLayer

	stats expiryStats
}

// PendingTasks returns the number of pending ILM expiry tasks.
func (es *expiryState) PendingTasks() int {
	w := es.workers.Load()
	if w == nil || len(*w) == 0 {
		return 0
	}
	var tasks int
	for _, wrkr := range *w {
		tasks += len(wrkr)
	}
	return tasks
}

// enqueueTierJournalEntry enqueues a tier journal entry referring to a remote
// object corresponding to a 'replaced' object versions. This applies only to
// non-versioned or version suspended buckets.
func (es *expiryState) enqueueTierJournalEntry(je jentry) {
	wrkr := es.getWorkerCh(je.OpHash())
	if wrkr == nil {
		es.stats.missedTierJournalTasks.Add(1)
		return
	}
	select {
	case <-GlobalContext.Done():
	case wrkr <- je:
	default:
		es.stats.missedTierJournalTasks.Add(1)
	}
}

// enqueueFreeVersion enqueues a free version to be deleted
func (es *expiryState) enqueueFreeVersion(oi ObjectInfo) {
	task := freeVersionTask{ObjectInfo: oi}
	wrkr := es.getWorkerCh(task.OpHash())
	if wrkr == nil {
		es.stats.missedFreeVersTasks.Add(1)
		return
	}
	select {
	case <-GlobalContext.Done():
	case wrkr <- task:
	default:
		es.stats.missedFreeVersTasks.Add(1)
	}
}

// enqueueByDays enqueues object versions expired by days for expiry.
func (es *expiryState) enqueueByDays(oi ObjectInfo, event lifecycle.Event, src lcEventSrc) {
	task := expiryTask{objInfo: oi, event: event, src: src}
	wrkr := es.getWorkerCh(task.OpHash())
	if wrkr == nil {
		es.stats.missedExpiryTasks.Add(1)
		return
	}
	select {
	case <-GlobalContext.Done():
	case wrkr <- task:
	default:
		es.stats.missedExpiryTasks.Add(1)
	}
}

func (es *expiryState) enqueueNoncurrentVersions(bucket string, versions []ObjectToDelete, events []lifecycle.Event) {
	if len(versions) == 0 {
		return
	}

	task := noncurrentVersionsTask{
		bucket:   bucket,
		versions: versions,
		events:   events,
	}
	wrkr := es.getWorkerCh(task.OpHash())
	if wrkr == nil {
		es.stats.missedExpiryTasks.Add(1)
		return
	}
	select {
	case <-GlobalContext.Done():
	case wrkr <- task:
	default:
		es.stats.missedExpiryTasks.Add(1)
	}
}

// globalExpiryState is the per-node instance which manages all ILM expiry tasks.
var globalExpiryState *expiryState

// newExpiryState creates an expiryState with buffered channels allocated for
// each ILM expiry task type.
func newExpiryState(ctx context.Context, objAPI ObjectLayer, n int) *expiryState {
	es := &expiryState{
		ctx:    ctx,
		objAPI: objAPI,
	}
	workers := make([]chan expiryOp, 0, n)
	es.workers.Store(&workers)
	es.ResizeWorkers(n)
	return es
}

func (es *expiryState) getWorkerCh(h uint64) chan<- expiryOp {
	w := es.workers.Load()
	if w == nil || len(*w) == 0 {
		return nil
	}
	workers := *w
	return workers[h%uint64(len(workers))]
}

func (es *expiryState) ResizeWorkers(n int) {
	if n == 0 {
		n = 100
	}

	// Lock to avoid multiple resizes to happen at the same time.
	es.mu.Lock()
	defer es.mu.Unlock()
	var workers []chan expiryOp
	if v := es.workers.Load(); v != nil {
		// Copy to new array.
		workers = append(workers, *v...)
	}

	if n == len(workers) || n < 1 {
		return
	}

	for len(workers) < n {
		input := make(chan expiryOp, 10000)
		workers = append(workers, input)
		go es.Worker(input)
		es.stats.workers.Add(1)
	}

	for len(workers) > n {
		worker := workers[len(workers)-1]
		workers = workers[:len(workers)-1]
		worker <- expiryOp(nil)
		es.stats.workers.Add(-1)
	}
	// Atomically replace workers.
	es.workers.Store(&workers)
}

// Worker handles 4 types of expiration tasks.
// 1. Expiry of objects, includes regular and transitioned objects
// 2. Expiry of noncurrent versions due to NewerNoncurrentVersions
// 3. Expiry of free-versions, for remote objects of transitioned object which have been expired since.
// 4. Expiry of remote objects corresponding to objects in a
// non-versioned/version suspended buckets
func (es *expiryState) Worker(input <-chan expiryOp) {
	for {
		select {
		case <-es.ctx.Done():
			return
		case v, ok := <-input:
			if !ok {
				return
			}
			if v == nil {
				// ResizeWorkers signaling worker to quit
				return
			}
			switch v := v.(type) {
			case expiryTask:
				if v.objInfo.TransitionedObject.Status != "" {
					applyExpiryOnTransitionedObject(es.ctx, es.objAPI, v.objInfo, v.event, v.src)
				} else {
					applyExpiryOnNonTransitionedObjects(es.ctx, es.objAPI, v.objInfo, v.event, v.src)
				}
			case noncurrentVersionsTask:
				deleteObjectVersions(es.ctx, es.objAPI, v.bucket, v.versions, v.events)
			case jentry:
				transitionLogIf(es.ctx, deleteObjectFromRemoteTier(es.ctx, v.ObjName, v.VersionID, v.TierName))
			case freeVersionTask:
				oi := v.ObjectInfo
				traceFn := globalLifecycleSys.trace(oi)
				if !oi.TransitionedObject.FreeVersion {
					// nothing to be done
					continue
				}

				ignoreNotFoundErr := func(err error) error {
					switch {
					case isErrVersionNotFound(err), isErrObjectNotFound(err):
						return nil
					}
					return err
				}
				// Remove the remote object
				err := deleteObjectFromRemoteTier(es.ctx, oi.TransitionedObject.Name, oi.TransitionedObject.VersionID, oi.TransitionedObject.Tier)
				if ignoreNotFoundErr(err) != nil {
					transitionLogIf(es.ctx, err)
					traceFn(ILMFreeVersionDelete, nil, err)
					continue
				}

				// Remove this free version
				_, err = es.objAPI.DeleteObject(es.ctx, oi.Bucket, oi.Name, ObjectOptions{
					VersionID:        oi.VersionID,
					InclFreeVersions: true,
				})
				if err == nil {
					auditLogLifecycle(es.ctx, oi, ILMFreeVersionDelete, nil, traceFn)
				}
				if ignoreNotFoundErr(err) != nil {
					transitionLogIf(es.ctx, err)
				}
			default:
				bugLogIf(es.ctx, fmt.Errorf("Invalid work type - %v", v))
			}
		}
	}
}

func initBackgroundExpiry(ctx context.Context, objectAPI ObjectLayer) {
	globalExpiryState = newExpiryState(ctx, objectAPI, globalILMConfig.getExpirationWorkers())
}

type noncurrentVersionsTask struct {
	bucket   string
	versions []ObjectToDelete
	events   []lifecycle.Event
}

type transitionTask struct {
	objInfo ObjectInfo
	src     lcEventSrc
	event   lifecycle.Event
}

type transitionState struct {
	transitionCh chan transitionTask

	ctx        context.Context
	objAPI     ObjectLayer
	mu         sync.Mutex
	numWorkers int
	killCh     chan struct{}

	activeTasks          atomic.Int64
	missedImmediateTasks atomic.Int64

	lastDayMu    sync.RWMutex
	lastDayStats map[string]*lastDayTierStats
}

func (t *transitionState) queueTransitionTask(oi ObjectInfo, event lifecycle.Event, src lcEventSrc) {
	task := transitionTask{objInfo: oi, event: event, src: src}
	select {
	case <-t.ctx.Done():
	case t.transitionCh <- task:
	default:
		switch src {
		case lcEventSrc_s3PutObject, lcEventSrc_s3CopyObject, lcEventSrc_s3CompleteMultipartUpload:
			// Update missed immediate tasks only for incoming requests.
			t.missedImmediateTasks.Add(1)
		}
	}
}

var globalTransitionState *transitionState

// newTransitionState returns a transitionState object ready to be initialized
// via its Init method.
func newTransitionState(ctx context.Context) *transitionState {
	return &transitionState{
		transitionCh: make(chan transitionTask, 100000),
		ctx:          ctx,
		killCh:       make(chan struct{}),
		lastDayStats: make(map[string]*lastDayTierStats),
	}
}

// Init initializes t with given objAPI and instantiates the configured number
// of transition workers.
func (t *transitionState) Init(objAPI ObjectLayer) {
	n := globalAPIConfig.getTransitionWorkers()
	// Prefer ilm.transition_workers over now deprecated api.transition_workers
	if tw := globalILMConfig.getTransitionWorkers(); tw > 0 {
		n = tw
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.objAPI = objAPI
	t.updateWorkers(n)
}

// PendingTasks returns the number of ILM transition tasks waiting for a worker
// goroutine.
func (t *transitionState) PendingTasks() int {
	return len(t.transitionCh)
}

// ActiveTasks returns the number of active (ongoing) ILM transition tasks.
func (t *transitionState) ActiveTasks() int64 {
	return t.activeTasks.Load()
}

// MissedImmediateTasks returns the number of tasks - deferred to scanner due
// to tasks channel being backlogged.
func (t *transitionState) MissedImmediateTasks() int64 {
	return t.missedImmediateTasks.Load()
}

// worker waits for transition tasks
func (t *transitionState) worker(objectAPI ObjectLayer) {
	for {
		select {
		case <-t.killCh:
			return
		case <-t.ctx.Done():
			return
		case task, ok := <-t.transitionCh:
			if !ok {
				return
			}
			t.activeTasks.Add(1)
			if err := transitionObject(t.ctx, objectAPI, task.objInfo, newLifecycleAuditEvent(task.src, task.event)); err != nil {
				if !isErrVersionNotFound(err) && !isErrObjectNotFound(err) && !xnet.IsNetworkOrHostDown(err, false) {
					if !strings.Contains(err.Error(), "use of closed network connection") {
						transitionLogIf(t.ctx, fmt.Errorf("Transition to %s failed for %s/%s version:%s with %w",
							task.event.StorageClass, task.objInfo.Bucket, task.objInfo.Name, task.objInfo.VersionID, err))
					}
				}
			} else {
				ts := tierStats{
					TotalSize:   uint64(task.objInfo.Size),
					NumVersions: 1,
				}
				if task.objInfo.IsLatest {
					ts.NumObjects = 1
				}
				t.addLastDayStats(task.event.StorageClass, ts)
			}
			t.activeTasks.Add(-1)
		}
	}
}

func (t *transitionState) addLastDayStats(tier string, ts tierStats) {
	t.lastDayMu.Lock()
	defer t.lastDayMu.Unlock()

	if _, ok := t.lastDayStats[tier]; !ok {
		t.lastDayStats[tier] = &lastDayTierStats{}
	}
	t.lastDayStats[tier].addStats(ts)
}

func (t *transitionState) getDailyAllTierStats() DailyAllTierStats {
	t.lastDayMu.RLock()
	defer t.lastDayMu.RUnlock()

	res := make(DailyAllTierStats, len(t.lastDayStats))
	for tier, st := range t.lastDayStats {
		res[tier] = st.clone()
	}
	return res
}

// UpdateWorkers at the end of this function leaves n goroutines waiting for
// transition tasks
func (t *transitionState) UpdateWorkers(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.objAPI == nil { // Init hasn't been called yet.
		return
	}
	t.updateWorkers(n)
}

func (t *transitionState) updateWorkers(n int) {
	if n == 0 {
		n = 100
	}

	for t.numWorkers < n {
		go t.worker(t.objAPI)
		t.numWorkers++
	}

	for t.numWorkers > n {
		go func() { t.killCh <- struct{}{} }()
		t.numWorkers--
	}
}

var errInvalidStorageClass = errors.New("invalid storage class")

func validateTransitionTier(lc *lifecycle.Lifecycle) error {
	for _, rule := range lc.Rules {
		if rule.Transition.StorageClass != "" {
			if valid := globalTierConfigMgr.IsTierValid(rule.Transition.StorageClass); !valid {
				return errInvalidStorageClass
			}
		}
		if rule.NoncurrentVersionTransition.StorageClass != "" {
			if valid := globalTierConfigMgr.IsTierValid(rule.NoncurrentVersionTransition.StorageClass); !valid {
				return errInvalidStorageClass
			}
		}
	}
	return nil
}

// enqueueTransitionImmediate enqueues obj for transition if eligible.
// This is to be called after a successful upload of an object (version).
func enqueueTransitionImmediate(obj ObjectInfo, src lcEventSrc) {
	if lc, err := globalLifecycleSys.Get(obj.Bucket); err == nil {
		switch event := lc.Eval(obj.ToLifecycleOpts()); event.Action {
		case lifecycle.TransitionAction, lifecycle.TransitionVersionAction:
			if obj.DeleteMarker || obj.IsDir {
				// nothing to transition
				return
			}
			globalTransitionState.queueTransitionTask(obj, event, src)
		}
	}
}

// expireTransitionedObject handles expiry of transitioned/restored objects
// (versions) in one of the following situations:
//
// 1. when a restored (via PostRestoreObject API) object expires.
// 2. when a transitioned object expires (based on an ILM rule).
func expireTransitionedObject(ctx context.Context, objectAPI ObjectLayer, oi *ObjectInfo, lcEvent lifecycle.Event, src lcEventSrc) error {
	traceFn := globalLifecycleSys.trace(*oi)
	opts := ObjectOptions{
		Versioned:  globalBucketVersioningSys.PrefixEnabled(oi.Bucket, oi.Name),
		Expiration: ExpirationOptions{Expire: true},
	}
	if lcEvent.Action.DeleteVersioned() {
		opts.VersionID = oi.VersionID
	}
	tags := newLifecycleAuditEvent(src, lcEvent).Tags()
	if lcEvent.Action.DeleteRestored() {
		// delete locally restored copy of object or object version
		// from the source, while leaving metadata behind. The data on
		// transitioned tier lies untouched and still accessible
		opts.Transition.ExpireRestored = true
		_, err := objectAPI.DeleteObject(ctx, oi.Bucket, oi.Name, opts)
		if err == nil {
			// TODO consider including expiry of restored object to events we
			// notify.
			auditLogLifecycle(ctx, *oi, ILMExpiry, tags, traceFn)
		}
		return err
	}

	// Delete remote object from warm-tier
	err := deleteObjectFromRemoteTier(ctx, oi.TransitionedObject.Name, oi.TransitionedObject.VersionID, oi.TransitionedObject.Tier)
	if err == nil {
		// Skip adding free version since we successfully deleted the
		// remote object
		opts.SkipFreeVersion = true
	} else {
		transitionLogIf(ctx, err)
	}

	// Now, delete object from hot-tier namespace
	if _, err := objectAPI.DeleteObject(ctx, oi.Bucket, oi.Name, opts); err != nil {
		return err
	}

	// Send audit for the lifecycle delete operation
	defer auditLogLifecycle(ctx, *oi, ILMExpiry, tags, traceFn)

	eventName := event.ObjectRemovedDelete
	if oi.DeleteMarker {
		eventName = event.ObjectRemovedDeleteMarkerCreated
	}
	objInfo := ObjectInfo{
		Name:         oi.Name,
		VersionID:    oi.VersionID,
		DeleteMarker: oi.DeleteMarker,
	}
	// Notify object deleted event.
	sendEvent(eventArgs{
		EventName:  eventName,
		BucketName: oi.Bucket,
		Object:     objInfo,
		UserAgent:  "Internal: [ILM-Expiry]",
		Host:       globalLocalNodeName,
	})

	return nil
}

// generate an object name for transitioned object
func genTransitionObjName(bucket string) (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	us := u.String()
	hash := xxh3.HashString(pathJoin(globalDeploymentID(), bucket))
	obj := fmt.Sprintf("%s/%s/%s/%s", strconv.FormatUint(hash, 16), us[0:2], us[2:4], us)
	return obj, nil
}

// transition object to target specified by the transition ARN. When an object is transitioned to another
// storage specified by the transition ARN, the metadata is left behind on source cluster and original content
// is moved to the transition tier. Note that in the case of encrypted objects, entire encrypted stream is moved
// to the transition tier without decrypting or re-encrypting.
func transitionObject(ctx context.Context, objectAPI ObjectLayer, oi ObjectInfo, lae lcAuditEvent) (err error) {
	timeILM := globalScannerMetrics.timeILM(lae.Action)
	defer func() {
		if err != nil {
			return
		}
		timeILM(1)
	}()

	opts := ObjectOptions{
		Transition: TransitionOptions{
			Status: lifecycle.TransitionPending,
			Tier:   lae.StorageClass,
			ETag:   oi.ETag,
		},
		LifecycleAuditEvent: lae,
		VersionID:           oi.VersionID,
		Versioned:           globalBucketVersioningSys.PrefixEnabled(oi.Bucket, oi.Name),
		VersionSuspended:    globalBucketVersioningSys.PrefixSuspended(oi.Bucket, oi.Name),
		MTime:               oi.ModTime,
	}
	return objectAPI.TransitionObject(ctx, oi.Bucket, oi.Name, opts)
}

type auditTierOp struct {
	Tier             string `json:"tier"`
	TimeToResponseNS int64  `json:"timeToResponseNS"`
	OutputBytes      int64  `json:"tx,omitempty"`
	Error            string `json:"error,omitempty"`
}

func (op auditTierOp) String() string {
	// flattening the auditTierOp{} for audit
	return fmt.Sprintf("tier:%s,respNS:%d,tx:%d,err:%s", op.Tier, op.TimeToResponseNS, op.OutputBytes, op.Error)
}

func auditTierActions(ctx context.Context, tier string, bytes int64) func(err error) {
	startTime := time.Now()
	return func(err error) {
		// Record only when audit targets configured.
		if len(logger.AuditTargets()) == 0 {
			return
		}

		op := auditTierOp{
			Tier:        tier,
			OutputBytes: bytes,
		}

		if err == nil {
			since := time.Since(startTime)
			op.TimeToResponseNS = since.Nanoseconds()
			globalTierMetrics.Observe(tier, since)
			globalTierMetrics.logSuccess(tier)
		} else {
			op.Error = err.Error()
			globalTierMetrics.logFailure(tier)
		}

		logger.GetReqInfo(ctx).AppendTags("tierStats", op.String())
	}
}

// getTransitionedObjectReader returns a reader from the transitioned tier.
func getTransitionedObjectReader(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, oi ObjectInfo, opts ObjectOptions) (gr *GetObjectReader, err error) {
	tgtClient, err := globalTierConfigMgr.getDriver(ctx, oi.TransitionedObject.Tier)
	if err != nil {
		return nil, fmt.Errorf("transition storage class not configured: %w", err)
	}

	fn, off, length, err := NewGetObjectReader(rs, oi, opts, h)
	if err != nil {
		return nil, ErrorRespToObjectError(err, bucket, object)
	}
	gopts := WarmBackendGetOpts{}

	// get correct offsets for object
	if off >= 0 && length >= 0 {
		gopts.startOffset = off
		gopts.length = length
	}

	timeTierAction := auditTierActions(ctx, oi.TransitionedObject.Tier, length)
	reader, err := tgtClient.Get(ctx, oi.TransitionedObject.Name, remoteVersionID(oi.TransitionedObject.VersionID), gopts)
	if err != nil {
		return nil, err
	}
	closer := func() {
		timeTierAction(reader.Close())
	}
	return fn(reader, h, closer)
}

// RestoreRequestType represents type of restore.
type RestoreRequestType string

const (
	// SelectRestoreRequest specifies select request. This is the only valid value
	SelectRestoreRequest RestoreRequestType = "SELECT"
)

// Encryption specifies encryption setting on restored bucket
type Encryption struct {
	EncryptionType sse.Algorithm `xml:"EncryptionType"`
	KMSContext     string        `xml:"KMSContext,omitempty"`
	KMSKeyID       string        `xml:"KMSKeyId,omitempty"`
}

// MetadataEntry denotes name and value.
type MetadataEntry struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

// S3Location specifies s3 location that receives result of a restore object request
type S3Location struct {
	BucketName   string          `xml:"BucketName,omitempty"`
	Encryption   Encryption      `xml:"Encryption,omitempty"`
	Prefix       string          `xml:"Prefix,omitempty"`
	StorageClass string          `xml:"StorageClass,omitempty"`
	Tagging      *tags.Tags      `xml:"Tagging,omitempty"`
	UserMetadata []MetadataEntry `xml:"UserMetadata"`
}

// OutputLocation specifies bucket where object needs to be restored
type OutputLocation struct {
	S3 S3Location `xml:"S3,omitempty"`
}

// IsEmpty returns true if output location not specified.
func (o *OutputLocation) IsEmpty() bool {
	return o.S3.BucketName == ""
}

// SelectParameters specifies sql select parameters
type SelectParameters struct {
	s3select.S3Select
}

// IsEmpty returns true if no select parameters set
func (sp *SelectParameters) IsEmpty() bool {
	return sp == nil
}

var selectParamsXMLName = "SelectParameters"

// UnmarshalXML - decodes XML data.
func (sp *SelectParameters) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Essentially the same as S3Select barring the xml name.
	if start.Name.Local == selectParamsXMLName {
		start.Name = xml.Name{Space: "", Local: "SelectRequest"}
	}
	return sp.S3Select.UnmarshalXML(d, start)
}

// RestoreObjectRequest - xml to restore a transitioned object
type RestoreObjectRequest struct {
	XMLName          xml.Name           `xml:"http://s3.amazonaws.com/doc/2006-03-01/ RestoreRequest" json:"-"`
	Days             int                `xml:"Days,omitempty"`
	Type             RestoreRequestType `xml:"Type,omitempty"`
	Tier             string             `xml:"Tier"`
	Description      string             `xml:"Description,omitempty"`
	SelectParameters *SelectParameters  `xml:"SelectParameters,omitempty"`
	OutputLocation   OutputLocation     `xml:"OutputLocation,omitempty"`
}

// Maximum 2MiB size per restore object request.
const maxRestoreObjectRequestSize = 2 << 20

// parseRestoreRequest parses RestoreObjectRequest from xml
func parseRestoreRequest(reader io.Reader) (*RestoreObjectRequest, error) {
	req := RestoreObjectRequest{}
	if err := xml.NewDecoder(io.LimitReader(reader, maxRestoreObjectRequestSize)).Decode(&req); err != nil {
		return nil, err
	}
	return &req, nil
}

// validate a RestoreObjectRequest as per AWS S3 spec https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html
func (r *RestoreObjectRequest) validate(ctx context.Context, objAPI ObjectLayer) error {
	if r.Type != SelectRestoreRequest && !r.SelectParameters.IsEmpty() {
		return fmt.Errorf("Select parameters can only be specified with SELECT request type")
	}
	if r.Type == SelectRestoreRequest && r.SelectParameters.IsEmpty() {
		return fmt.Errorf("SELECT restore request requires select parameters to be specified")
	}

	if r.Type != SelectRestoreRequest && !r.OutputLocation.IsEmpty() {
		return fmt.Errorf("OutputLocation required only for SELECT request type")
	}
	if r.Type == SelectRestoreRequest && r.OutputLocation.IsEmpty() {
		return fmt.Errorf("OutputLocation required for SELECT requests")
	}

	if r.Days != 0 && r.Type == SelectRestoreRequest {
		return fmt.Errorf("Days cannot be specified with SELECT restore request")
	}
	if r.Days == 0 && r.Type != SelectRestoreRequest {
		return fmt.Errorf("restoration days should be at least 1")
	}
	// Check if bucket exists.
	if !r.OutputLocation.IsEmpty() {
		if _, err := objAPI.GetBucketInfo(ctx, r.OutputLocation.S3.BucketName, BucketOptions{}); err != nil {
			return err
		}
		if r.OutputLocation.S3.Prefix == "" {
			return fmt.Errorf("Prefix is a required parameter in OutputLocation")
		}
		if r.OutputLocation.S3.Encryption.EncryptionType != xhttp.AmzEncryptionAES {
			return NotImplemented{}
		}
	}
	return nil
}

// postRestoreOpts returns ObjectOptions with version-id from the POST restore object request for a given bucket and object.
func postRestoreOpts(ctx context.Context, r *http.Request, bucket, object string) (opts ObjectOptions, err error) {
	versioned := globalBucketVersioningSys.PrefixEnabled(bucket, object)
	versionSuspended := globalBucketVersioningSys.PrefixSuspended(bucket, object)
	vid := strings.TrimSpace(r.Form.Get(xhttp.VersionID))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			s3LogIf(ctx, err)
			return opts, InvalidVersionID{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
		if !versioned && !versionSuspended {
			return opts, InvalidArgument{
				Bucket: bucket,
				Object: object,
				Err:    fmt.Errorf("version-id specified %s but versioning is not enabled on %s", opts.VersionID, bucket),
			}
		}
	}
	return ObjectOptions{
		Versioned:        versioned,
		VersionSuspended: versionSuspended,
		VersionID:        vid,
	}, nil
}

// set ObjectOptions for PUT call to restore temporary copy of transitioned data
func putRestoreOpts(bucket, object string, rreq *RestoreObjectRequest, objInfo ObjectInfo) (putOpts ObjectOptions) {
	meta := make(map[string]string)
	sc := rreq.OutputLocation.S3.StorageClass
	if sc == "" {
		sc = objInfo.StorageClass
	}
	meta[strings.ToLower(xhttp.AmzStorageClass)] = sc

	if rreq.Type == SelectRestoreRequest {
		for _, v := range rreq.OutputLocation.S3.UserMetadata {
			if !stringsHasPrefixFold(v.Name, "x-amz-meta") {
				meta["x-amz-meta-"+v.Name] = v.Value
				continue
			}
			meta[v.Name] = v.Value
		}
		if tags := rreq.OutputLocation.S3.Tagging.String(); tags != "" {
			meta[xhttp.AmzObjectTagging] = tags
		}
		if rreq.OutputLocation.S3.Encryption.EncryptionType != "" {
			meta[xhttp.AmzServerSideEncryption] = xhttp.AmzEncryptionAES
		}
		return ObjectOptions{
			Versioned:        globalBucketVersioningSys.PrefixEnabled(bucket, object),
			VersionSuspended: globalBucketVersioningSys.PrefixSuspended(bucket, object),
			UserDefined:      meta,
		}
	}
	maps.Copy(meta, objInfo.UserDefined)
	if len(objInfo.UserTags) != 0 {
		meta[xhttp.AmzObjectTagging] = objInfo.UserTags
	}
	// Set restore object status
	restoreExpiry := lifecycle.ExpectedExpiryTime(time.Now().UTC(), rreq.Days)
	meta[xhttp.AmzRestore] = completedRestoreObj(restoreExpiry).String()
	return ObjectOptions{
		Versioned:        globalBucketVersioningSys.PrefixEnabled(bucket, object),
		VersionSuspended: globalBucketVersioningSys.PrefixSuspended(bucket, object),
		UserDefined:      meta,
		VersionID:        objInfo.VersionID,
		MTime:            objInfo.ModTime,
		Expires:          objInfo.Expires,
	}
}

var errRestoreHDRMalformed = fmt.Errorf("x-amz-restore header malformed")

// IsRemote returns true if this object version's contents are in its remote
// tier.
func (fi FileInfo) IsRemote() bool {
	if fi.TransitionStatus != lifecycle.TransitionComplete {
		return false
	}
	return !isRestoredObjectOnDisk(fi.Metadata)
}

// IsRemote returns true if this object version's contents are in its remote
// tier.
func (oi ObjectInfo) IsRemote() bool {
	if oi.TransitionedObject.Status != lifecycle.TransitionComplete {
		return false
	}
	return !isRestoredObjectOnDisk(oi.UserDefined)
}

// restoreObjStatus represents a restore-object's status. It can be either
// ongoing or completed.
type restoreObjStatus struct {
	ongoing bool
	expiry  time.Time
}

// ongoingRestoreObj constructs restoreObjStatus for an ongoing restore-object.
func ongoingRestoreObj() restoreObjStatus {
	return restoreObjStatus{
		ongoing: true,
	}
}

// completedRestoreObj constructs restoreObjStatus for a completed restore-object with given expiry.
func completedRestoreObj(expiry time.Time) restoreObjStatus {
	return restoreObjStatus{
		ongoing: false,
		expiry:  expiry.UTC(),
	}
}

// String returns x-amz-restore compatible representation of r.
func (r restoreObjStatus) String() string {
	if r.Ongoing() {
		return `ongoing-request="true"`
	}
	return fmt.Sprintf(`ongoing-request="false", expiry-date="%s"`, r.expiry.Format(http.TimeFormat))
}

// Expiry returns expiry of restored object and true if restore-object has completed.
// Otherwise returns zero value of time.Time and false.
func (r restoreObjStatus) Expiry() (time.Time, bool) {
	if r.Ongoing() {
		return time.Time{}, false
	}
	return r.expiry, true
}

// Ongoing returns true if restore-object is ongoing.
func (r restoreObjStatus) Ongoing() bool {
	return r.ongoing
}

// OnDisk returns true if restored object contents exist in MinIO. Otherwise returns false.
// The restore operation could be in one of the following states,
// - in progress (no content on MinIO's disks yet)
// - completed
// - completed but expired (again, no content on MinIO's disks)
func (r restoreObjStatus) OnDisk() bool {
	if expiry, ok := r.Expiry(); ok && time.Now().UTC().Before(expiry) {
		// completed
		return true
	}
	return false // in progress or completed but expired
}

// parseRestoreObjStatus parses restoreHdr from AmzRestore header. If the value is valid it returns a
// restoreObjStatus value with the status and expiry (if any). Otherwise returns
// the empty value and an error indicating the parse failure.
func parseRestoreObjStatus(restoreHdr string) (restoreObjStatus, error) {
	tokens := strings.SplitN(restoreHdr, ",", 2)
	progressTokens := strings.SplitN(tokens[0], "=", 2)
	if len(progressTokens) != 2 {
		return restoreObjStatus{}, errRestoreHDRMalformed
	}
	if strings.TrimSpace(progressTokens[0]) != "ongoing-request" {
		return restoreObjStatus{}, errRestoreHDRMalformed
	}

	switch progressTokens[1] {
	case "true", `"true"`: // true without double quotes is deprecated in Feb 2022
		if len(tokens) == 1 {
			return ongoingRestoreObj(), nil
		}
	case "false", `"false"`: // false without double quotes is deprecated in Feb 2022
		if len(tokens) != 2 {
			return restoreObjStatus{}, errRestoreHDRMalformed
		}
		expiryTokens := strings.SplitN(tokens[1], "=", 2)
		if len(expiryTokens) != 2 {
			return restoreObjStatus{}, errRestoreHDRMalformed
		}
		if strings.TrimSpace(expiryTokens[0]) != "expiry-date" {
			return restoreObjStatus{}, errRestoreHDRMalformed
		}
		expiry, err := amztime.ParseHeader(strings.Trim(expiryTokens[1], `"`))
		if err != nil {
			return restoreObjStatus{}, errRestoreHDRMalformed
		}
		return completedRestoreObj(expiry), nil
	}
	return restoreObjStatus{}, errRestoreHDRMalformed
}

// isRestoredObjectOnDisk returns true if the restored object is on disk. Note
// this function must be called only if object version's transition status is
// complete.
func isRestoredObjectOnDisk(meta map[string]string) (onDisk bool) {
	if restoreHdr, ok := meta[xhttp.AmzRestore]; ok {
		if restoreStatus, err := parseRestoreObjStatus(restoreHdr); err == nil {
			return restoreStatus.OnDisk()
		}
	}
	return onDisk
}

// ToLifecycleOpts returns lifecycle.ObjectOpts value for oi.
func (oi ObjectInfo) ToLifecycleOpts() lifecycle.ObjectOpts {
	return lifecycle.ObjectOpts{
		Name:               oi.Name,
		UserTags:           oi.UserTags,
		VersionID:          oi.VersionID,
		ModTime:            oi.ModTime,
		Size:               oi.Size,
		IsLatest:           oi.IsLatest,
		NumVersions:        oi.NumVersions,
		DeleteMarker:       oi.DeleteMarker,
		SuccessorModTime:   oi.SuccessorModTime,
		RestoreOngoing:     oi.RestoreOngoing,
		RestoreExpires:     oi.RestoreExpires,
		TransitionStatus:   oi.TransitionedObject.Status,
		UserDefined:        oi.UserDefined,
		VersionPurgeStatus: oi.VersionPurgeStatus,
		ReplicationStatus:  oi.ReplicationStatus,
	}
}
