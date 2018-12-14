/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// healStatusSummary - overall short summary of a healing sequence
type healStatusSummary string

// healStatusSummary constants
const (
	healNotStartedStatus healStatusSummary = "not started"
	healRunningStatus                      = "running"
	healStoppedStatus                      = "stopped"
	healFinishedStatus                     = "finished"
)

const (
	// a heal sequence with this many un-consumed heal result
	// items blocks until heal-status consumption resumes or is
	// aborted due to timeout.
	maxUnconsumedHealResultItems = 1000

	// if no heal-results are consumed (via the heal-status API)
	// for this timeout duration, the heal sequence is aborted.
	healUnconsumedTimeout = 24 * time.Hour

	// time-duration to keep heal sequence state after it
	// completes.
	keepHealSeqStateDuration = time.Minute * 10
)

var (
	errHealIdleTimeout      = fmt.Errorf("healing results were not consumed for too long")
	errHealPushStopNDiscard = fmt.Errorf("heal push stopped due to heal stop signal")
	errHealStopSignalled    = fmt.Errorf("heal stop signaled")

	errFnHealFromAPIErr = func(ctx context.Context, err error) error {
		errCode := toAPIErrorCode(ctx, err)
		apiErr := getAPIError(errCode)
		return fmt.Errorf("Heal internal error: %s: %s",
			apiErr.Code, apiErr.Description)
	}
)

// healSequenceStatus - accumulated status of the heal sequence
type healSequenceStatus struct {
	// lock to update this structure as it is concurrently
	// accessed
	updateLock *sync.RWMutex

	// summary and detail for failures
	Summary       healStatusSummary `json:"Summary"`
	FailureDetail string            `json:"Detail,omitempty"`
	StartTime     time.Time         `json:"StartTime"`

	// disk information
	NumDisks int `json:"NumDisks"`

	// settings for the heal sequence
	HealSettings madmin.HealOpts `json:"Settings"`

	// slice of available heal result records
	Items []madmin.HealResultItem `json:"Items"`
}

// structure to hold state of all heal sequences in server memory
type allHealState struct {
	sync.Mutex

	// map of heal path to heal sequence
	healSeqMap map[string]*healSequence
}

var (
	// global server heal state
	globalAllHealState allHealState
)

// initAllHealState - initialize healing apparatus
func initAllHealState(isErasureMode bool) {
	if !isErasureMode {
		return
	}

	globalAllHealState = allHealState{
		healSeqMap: make(map[string]*healSequence),
	}

	go globalAllHealState.periodicHealSeqsClean()
}

func (ahs *allHealState) periodicHealSeqsClean() {
	// Launch clean-up routine to remove this heal sequence (after
	// it ends) from the global state after timeout has elapsed.
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := UTCNow()
			ahs.Lock()
			for path, h := range ahs.healSeqMap {
				if h.hasEnded() && h.endTime.Add(keepHealSeqStateDuration).Before(now) {
					delete(ahs.healSeqMap, path)
				}
			}
			ahs.Unlock()
		case <-globalServiceDoneCh:
			// server could be restarting - need
			// to exit immediately
			return
		}
	}
}

// getHealSequence - Retrieve a heal sequence by path. The second
// argument returns if a heal sequence actually exists.
func (ahs *allHealState) getHealSequence(path string) (h *healSequence, exists bool) {
	ahs.Lock()
	defer ahs.Unlock()
	h, exists = ahs.healSeqMap[path]
	return h, exists
}

func (ahs *allHealState) stopHealSequence(path string) ([]byte, APIErrorCode) {
	var hsp madmin.HealStopSuccess
	he, exists := ahs.getHealSequence(path)
	if !exists {
		hsp = madmin.HealStopSuccess{
			ClientToken: "invalid",
			StartTime:   UTCNow(),
		}
	} else {
		hsp = madmin.HealStopSuccess{
			ClientToken:   he.clientToken,
			ClientAddress: he.clientAddress,
			StartTime:     he.startTime,
		}

		he.stop()
		for !he.hasEnded() {
			time.Sleep(1 * time.Second)
		}
		ahs.Lock()
		defer ahs.Unlock()
		// Heal sequence explicitly stopped, remove it.
		delete(ahs.healSeqMap, path)
	}

	b, err := json.Marshal(&hsp)
	return b, toAdminAPIErrCode(context.Background(), err)
}

// LaunchNewHealSequence - launches a background routine that performs
// healing according to the healSequence argument. For each heal
// sequence, state is stored in the `globalAllHealState`, which is a
// map of the heal path to `healSequence` which holds state about the
// heal sequence.
//
// Heal results are persisted in server memory for
// `keepHealSeqStateDuration`. This function also launches a
// background routine to clean up heal results after the
// aforementioned duration.
func (ahs *allHealState) LaunchNewHealSequence(h *healSequence) (
	respBytes []byte, errCode APIErrorCode, errMsg string) {

	existsAndLive := false
	he, exists := ahs.getHealSequence(h.path)
	if exists {
		if !he.hasEnded() || len(he.currentStatus.Items) > 0 {
			existsAndLive = true
		}
	}

	if existsAndLive {
		// A heal sequence exists on the given path.
		if h.forceStarted {
			// stop the running heal sequence - wait for it to finish.
			he.stop()
			for !he.hasEnded() {
				time.Sleep(1 * time.Second)
			}
		} else {
			errMsg = "Heal is already running on the given path " +
				"(use force-start option to stop and start afresh). " +
				fmt.Sprintf("The heal was started by IP %s at %s, token is %s",
					h.clientAddress, h.startTime.Format(http.TimeFormat), h.clientToken)

			return nil, ErrHealAlreadyRunning, errMsg
		}
	}

	ahs.Lock()
	defer ahs.Unlock()

	// Check if new heal sequence to be started overlaps with any
	// existing, running sequence
	for k, hSeq := range ahs.healSeqMap {
		if !hSeq.hasEnded() && (strings.HasPrefix(k, h.path) ||
			strings.HasPrefix(h.path, k)) {

			errMsg = "The provided heal sequence path overlaps with an existing " +
				fmt.Sprintf("heal path: %s", k)
			return nil, ErrHealOverlappingPaths, errMsg
		}
	}

	// Add heal state and start sequence
	ahs.healSeqMap[h.path] = h

	// Launch top-level background heal go-routine
	go h.healSequenceStart()

	b, err := json.Marshal(madmin.HealStartSuccess{
		ClientToken:   h.clientToken,
		ClientAddress: h.clientAddress,
		StartTime:     h.startTime,
	})
	if err != nil {
		logger.LogIf(h.ctx, err)
		return nil, ErrInternalError, ""
	}
	return b, ErrNone, ""
}

// PopHealStatusJSON - Called by heal-status API. It fetches the heal
// status results from global state and returns its JSON
// representation. The clientToken helps ensure there aren't
// conflicting clients fetching status.
func (ahs *allHealState) PopHealStatusJSON(path string,
	clientToken string) ([]byte, APIErrorCode) {

	// fetch heal state for given path
	h, exists := ahs.getHealSequence(path)
	if !exists {
		// If there is no such heal sequence, return error.
		return nil, ErrHealNoSuchProcess
	}

	// Check if client-token is valid
	if clientToken != h.clientToken {
		return nil, ErrHealInvalidClientToken
	}

	// Take lock to access and update the heal-sequence
	h.currentStatus.updateLock.Lock()
	defer h.currentStatus.updateLock.Unlock()

	numItems := len(h.currentStatus.Items)

	// calculate index of most recently available heal result
	// record.
	lastResultIndex := h.lastSentResultIndex
	if numItems > 0 {
		lastResultIndex = h.currentStatus.Items[numItems-1].ResultIndex
	}

	// After sending status to client, and before relinquishing
	// the updateLock, reset Item to nil and record the result
	// index sent to the client.
	defer func(i int64) {
		h.lastSentResultIndex = i
		h.currentStatus.Items = nil
	}(lastResultIndex)

	jbytes, err := json.Marshal(h.currentStatus)
	if err != nil {
		logger.LogIf(h.ctx, err)
		return nil, ErrInternalError
	}

	return jbytes, ErrNone
}

// healSequence - state for each heal sequence initiated on the
// server.
type healSequence struct {
	// bucket, and prefix on which heal seq. was initiated
	bucket, objPrefix string

	// path is just pathJoin(bucket, objPrefix)
	path string

	// time at which heal sequence was started
	startTime time.Time

	// time at which heal sequence has ended
	endTime time.Time

	// Heal client info
	clientToken, clientAddress string

	// was this heal sequence force started?
	forceStarted bool

	// heal settings applied to this heal sequence
	settings madmin.HealOpts

	// current accumulated status of the heal sequence
	currentStatus healSequenceStatus

	// channel signaled by background routine when traversal has
	// completed
	traverseAndHealDoneCh chan error

	// channel to signal heal sequence to stop (e.g. from the
	// heal-stop API)
	stopSignalCh chan struct{}

	// the last result index sent to client
	lastSentResultIndex int64

	// Holds the request-info for logging
	ctx context.Context
}

// NewHealSequence - creates healSettings, assumes bucket and
// objPrefix are already validated.
func newHealSequence(bucket, objPrefix, clientAddr string,
	numDisks int, hs madmin.HealOpts, forceStart bool) *healSequence {

	reqInfo := &logger.ReqInfo{RemoteHost: clientAddr, API: "Heal", BucketName: bucket}
	reqInfo.AppendTags("prefix", objPrefix)
	ctx := logger.SetReqInfo(context.Background(), reqInfo)

	return &healSequence{
		bucket:        bucket,
		objPrefix:     objPrefix,
		path:          pathJoin(bucket, objPrefix),
		startTime:     UTCNow(),
		clientToken:   mustGetUUID(),
		clientAddress: clientAddr,
		forceStarted:  forceStart,
		settings:      hs,
		currentStatus: healSequenceStatus{
			Summary:      healNotStartedStatus,
			HealSettings: hs,
			NumDisks:     numDisks,
			updateLock:   &sync.RWMutex{},
		},
		traverseAndHealDoneCh: make(chan error),
		stopSignalCh:          make(chan struct{}),
		ctx:                   ctx,
	}
}

// isQuitting - determines if the heal sequence is quitting (due to an
// external signal)
func (h *healSequence) isQuitting() bool {
	select {
	case <-h.stopSignalCh:
		return true
	default:
		return false
	}
}

// check if the heal sequence has ended
func (h *healSequence) hasEnded() bool {
	h.currentStatus.updateLock.RLock()
	summary := h.currentStatus.Summary
	h.currentStatus.updateLock.RUnlock()
	return summary == healStoppedStatus || summary == healFinishedStatus
}

// stops the heal sequence - safe to call multiple times.
func (h *healSequence) stop() {
	select {
	case <-h.stopSignalCh:
	default:
		close(h.stopSignalCh)
	}
}

// pushHealResultItem - pushes a heal result item for consumption in
// the heal-status API. It blocks if there are
// maxUnconsumedHealResultItems. When it blocks, the heal sequence
// routine is effectively paused - this happens when the server has
// accumulated the maximum number of heal records per heal
// sequence. When the client consumes further records, the heal
// sequence automatically resumes. The return value indicates if the
// operation succeeded.
func (h *healSequence) pushHealResultItem(r madmin.HealResultItem) error {
	// start a timer to keep an upper time limit to find an empty
	// slot to add the given heal result - if no slot is found it
	// means that the server is holding the maximum amount of
	// heal-results in memory and the client has not consumed it
	// for too long.
	unconsumedTimer := time.NewTimer(healUnconsumedTimeout)
	defer func() {
		// stop the timeout timer so it is garbage collected.
		if !unconsumedTimer.Stop() {
			<-unconsumedTimer.C
		}
	}()

	var itemsLen int
	for {
		h.currentStatus.updateLock.Lock()
		itemsLen = len(h.currentStatus.Items)
		if itemsLen == maxUnconsumedHealResultItems {
			// unlock and wait to check again if we can push
			h.currentStatus.updateLock.Unlock()

			// wait for a second, or quit if an external
			// stop signal is received or the
			// unconsumedTimer fires.
			select {
			// Check after a second
			case <-time.After(time.Second):
				continue

			case <-h.stopSignalCh:
				// discard result and return.
				return errHealPushStopNDiscard

			// Timeout if no results consumed for too
			// long.
			case <-unconsumedTimer.C:
				return errHealIdleTimeout

			}
		}
		break
	}

	// Set the correct result index for the new result item
	if itemsLen > 0 {
		r.ResultIndex = 1 + h.currentStatus.Items[itemsLen-1].ResultIndex
	} else {
		r.ResultIndex = 1 + h.lastSentResultIndex
	}

	// append to results
	h.currentStatus.Items = append(h.currentStatus.Items, r)

	// release lock
	h.currentStatus.updateLock.Unlock()

	// This is a "safe" point for the heal sequence to quit if
	// signaled externally.
	if h.isQuitting() {
		return errHealStopSignalled
	}

	return nil
}

// healSequenceStart - this is the top-level background heal
// routine. It launches another go-routine that actually traverses
// on-disk data, checks and heals according to the selected
// settings. This go-routine itself, (1) monitors the traversal
// routine for completion, and (2) listens for external stop
// signals. When either event happens, it sets the finish status for
// the heal-sequence.
func (h *healSequence) healSequenceStart() {
	// Set status as running
	h.currentStatus.updateLock.Lock()
	h.currentStatus.Summary = healRunningStatus
	h.currentStatus.StartTime = UTCNow()
	h.currentStatus.updateLock.Unlock()

	go h.traverseAndHeal()

	select {
	case err, ok := <-h.traverseAndHealDoneCh:
		h.endTime = UTCNow()
		h.currentStatus.updateLock.Lock()
		defer h.currentStatus.updateLock.Unlock()
		// Heal traversal is complete.
		if ok {
			// heal traversal had an error.
			h.currentStatus.Summary = healStoppedStatus
			h.currentStatus.FailureDetail = err.Error()
		} else {
			// heal traversal succeeded.
			h.currentStatus.Summary = healFinishedStatus
		}

	case <-h.stopSignalCh:
		h.endTime = UTCNow()
		h.currentStatus.updateLock.Lock()
		h.currentStatus.Summary = healStoppedStatus
		h.currentStatus.FailureDetail = errHealStopSignalled.Error()
		h.currentStatus.updateLock.Unlock()

		// drain traverse channel so the traversal
		// go-routine does not leak.
		go func() {
			// Eventually the traversal go-routine closes
			// the channel and returns, so this go-routine
			// itself will not leak.
			<-h.traverseAndHealDoneCh
		}()
	}
}

// traverseAndHeal - traverses on-disk data and performs healing
// according to settings. At each "safe" point it also checks if an
// external quit signal has been received and quits if so. Since the
// healing traversal may be mutating on-disk data when an external
// quit signal is received, this routine cannot quit immediately and
// has to wait until a safe point is reached, such as between scanning
// two objects.
func (h *healSequence) traverseAndHeal() {
	var err error
	checkErr := func(f func() error) {
		switch {
		case err != nil:
			return
		case h.isQuitting():
			err = errHealStopSignalled
			return
		}
		err = f()
	}

	// Start with format healing
	checkErr(h.healDiskFormat)

	// Start healing the config.
	checkErr(h.healConfig)

	// Heal buckets and objects
	checkErr(h.healBuckets)

	if err != nil {
		h.traverseAndHealDoneCh <- err
	}

	close(h.traverseAndHealDoneCh)
}

// healConfig - heals config.json, retrun value indicates if a failure occurred.
func (h *healSequence) healConfig() error {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	// NOTE: Healing on configs is run regardless
	// of any bucket being selected, this is to ensure that
	// configs are always uptodate and correct.
	marker := ""
	isTruncated := true
	for isTruncated {
		if globalHTTPServer != nil {
			// Wait at max 1 minute for an inprogress request
			// before proceeding to heal
			waitCount := 60
			// Any requests in progress, delay the heal.
			for globalHTTPServer.GetRequestCount() > 2 && waitCount > 0 {
				waitCount--
				time.Sleep(1 * time.Second)
			}
		}

		// Lists all objects under `config` prefix.
		objectInfos, err := objectAPI.ListObjectsHeal(h.ctx, minioMetaBucket, minioConfigPrefix,
			marker, "", 1000)
		if err != nil {
			return errFnHealFromAPIErr(h.ctx, err)
		}

		for index := range objectInfos.Objects {
			if h.isQuitting() {
				return errHealStopSignalled
			}
			o := objectInfos.Objects[index]
			res, herr := objectAPI.HealObject(h.ctx, o.Bucket, o.Name, h.settings.DryRun)
			// Object might have been deleted, by the time heal
			// was attempted we ignore this file an move on.
			if isErrObjectNotFound(herr) {
				continue
			}
			if herr != nil {
				return herr
			}
			res.Type = madmin.HealItemBucketMetadata
			if err = h.pushHealResultItem(res); err != nil {
				return err
			}
		}

		isTruncated = objectInfos.IsTruncated
		marker = objectInfos.NextMarker
	}

	return nil
}

// healDiskFormat - heals format.json, return value indicates if a
// failure error occurred.
func (h *healSequence) healDiskFormat() error {
	if h.isQuitting() {
		return errHealStopSignalled
	}

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	res, err := objectAPI.HealFormat(h.ctx, h.settings.DryRun)
	// return any error, ignore error returned when disks have
	// already healed.
	if err != nil && err != errNoHealRequired {
		return errFnHealFromAPIErr(h.ctx, err)
	}

	// Healing succeeded notify the peers to reload format and re-initialize disks.
	// We will not notify peers only if healing succeeded.
	if err == nil {
		for host, rerr := range globalNotificationSys.ReloadFormat(h.settings.DryRun) {
			if rerr != nil {
				logger.GetReqInfo(h.ctx).SetTags("peerAddress", host.String())
				logger.LogIf(h.ctx, rerr)
			}
		}
	}

	// Push format heal result
	return h.pushHealResultItem(res)
}

// healBuckets - check for all buckets heal or just particular bucket.
func (h *healSequence) healBuckets() error {
	if h.isQuitting() {
		return errHealStopSignalled
	}

	// 1. If a bucket was specified, heal only the bucket.
	if h.bucket != "" {
		return h.healBucket(h.bucket)
	}

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	buckets, err := objectAPI.ListBucketsHeal(h.ctx)
	if err != nil {
		return errFnHealFromAPIErr(h.ctx, err)
	}

	for _, bucket := range buckets {
		if err = h.healBucket(bucket.Name); err != nil {
			return err
		}
	}

	return nil
}

// healBucket - traverses and heals given bucket
func (h *healSequence) healBucket(bucket string) error {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	results, err := objectAPI.HealBucket(h.ctx, bucket, h.settings.DryRun)
	// push any available results before checking for error
	for _, result := range results {
		if perr := h.pushHealResultItem(result); perr != nil {
			return perr
		}
	}
	// handle heal-bucket error
	if err != nil {
		return err
	}

	if !h.settings.Recursive {
		if h.objPrefix != "" {
			// Check if an object named as the objPrefix exists,
			// and if so heal it.
			_, err = objectAPI.GetObjectInfo(h.ctx, bucket, h.objPrefix, ObjectOptions{})
			if err == nil {
				if err = h.healObject(bucket, h.objPrefix); err != nil {
					return err
				}
			}
		}

		return nil
	}

	entries := runtime.NumCPU() * globalEndpoints.Nodes()

	marker := ""
	isTruncated := true
	for isTruncated {
		if globalHTTPServer != nil {
			// Wait at max 1 minute for an inprogress request
			// before proceeding to heal
			waitCount := 60
			// Any requests in progress, delay the heal.
			for globalHTTPServer.GetRequestCount() > 2 && waitCount > 0 {
				waitCount--
				time.Sleep(1 * time.Second)
			}
		}

		// Heal numCPU * nodes objects at a time.
		objectInfos, err := objectAPI.ListObjectsHeal(h.ctx, bucket,
			h.objPrefix, marker, "", entries)
		if err != nil {
			return errFnHealFromAPIErr(h.ctx, err)
		}

		g := errgroup.WithNErrs(len(objectInfos.Objects))
		for index := range objectInfos.Objects {
			index := index
			g.Go(func() error {
				o := objectInfos.Objects[index]
				return h.healObject(o.Bucket, o.Name)
			}, index)
		}

		for _, err := range g.Wait() {
			if err != nil {
				return err
			}
		}

		isTruncated = objectInfos.IsTruncated
		marker = objectInfos.NextMarker
	}
	return nil
}

// healObject - heal the given object and record result
func (h *healSequence) healObject(bucket, object string) error {
	if h.isQuitting() {
		return errHealStopSignalled
	}

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	hri, err := objectAPI.HealObject(h.ctx, bucket, object, h.settings.DryRun)
	if isErrObjectNotFound(err) {
		return nil
	}
	if err != nil {
		hri.Detail = err.Error()
	}
	return h.pushHealResultItem(hri)
}
