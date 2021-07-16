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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
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

	// nopHeal is a no operating healing action to
	// wait for the current healing operation to finish
	nopHeal = ""
)

var (
	errHealIdleTimeout   = fmt.Errorf("healing results were not consumed for too long")
	errHealStopSignalled = fmt.Errorf("heal stop signaled")

	errFnHealFromAPIErr = func(ctx context.Context, err error) error {
		apiErr := toAdminAPIErr(ctx, err)
		return fmt.Errorf("Heal internal error: %s: %s",
			apiErr.Code, apiErr.Description)
	}
)

// healSequenceStatus - accumulated status of the heal sequence
type healSequenceStatus struct {
	// summary and detail for failures
	Summary       healStatusSummary `json:"Summary"`
	FailureDetail string            `json:"Detail,omitempty"`
	StartTime     time.Time         `json:"StartTime"`

	// settings for the heal sequence
	HealSettings madmin.HealOpts `json:"Settings"`

	// slice of available heal result records
	Items []madmin.HealResultItem `json:"Items"`
}

// structure to hold state of all heal sequences in server memory
type allHealState struct {
	sync.RWMutex

	// map of heal path to heal sequence
	healSeqMap     map[string]*healSequence // Indexed by endpoint
	healLocalDisks map[Endpoint]struct{}
	healStatus     map[string]healingTracker // Indexed by disk ID
}

// newHealState - initialize global heal state management
func newHealState(cleanup bool) *allHealState {
	hstate := &allHealState{
		healSeqMap:     make(map[string]*healSequence),
		healLocalDisks: map[Endpoint]struct{}{},
		healStatus:     make(map[string]healingTracker),
	}
	if cleanup {
		go hstate.periodicHealSeqsClean(GlobalContext)
	}
	return hstate
}

func (ahs *allHealState) healDriveCount() int {
	ahs.RLock()
	defer ahs.RUnlock()

	return len(ahs.healLocalDisks)
}

func (ahs *allHealState) popHealLocalDisks(healLocalDisks ...Endpoint) {
	ahs.Lock()
	defer ahs.Unlock()

	for _, ep := range healLocalDisks {
		delete(ahs.healLocalDisks, ep)
	}
	for id, disk := range ahs.healStatus {
		for _, ep := range healLocalDisks {
			if disk.Endpoint == ep.String() {
				delete(ahs.healStatus, id)
			}
		}
	}
}

// updateHealStatus will update the heal status.
func (ahs *allHealState) updateHealStatus(tracker *healingTracker) {
	ahs.Lock()
	defer ahs.Unlock()
	ahs.healStatus[tracker.ID] = *tracker
}

// Sort by zone, set and disk index
func sortDisks(disks []madmin.Disk) {
	sort.Slice(disks, func(i, j int) bool {
		a, b := &disks[i], &disks[j]
		if a.PoolIndex != b.PoolIndex {
			return a.PoolIndex < b.PoolIndex
		}
		if a.SetIndex != b.SetIndex {
			return a.SetIndex < b.SetIndex
		}
		return a.DiskIndex < b.DiskIndex
	})
}

// getLocalHealingDisks returns local healing disks indexed by endpoint.
func (ahs *allHealState) getLocalHealingDisks() map[string]madmin.HealingDisk {
	ahs.RLock()
	defer ahs.RUnlock()
	dst := make(map[string]madmin.HealingDisk, len(ahs.healStatus))
	for _, v := range ahs.healStatus {
		dst[v.Endpoint] = v.toHealingDisk()
	}

	return dst
}

func (ahs *allHealState) getHealLocalDiskEndpoints() Endpoints {
	ahs.RLock()
	defer ahs.RUnlock()

	var endpoints Endpoints
	for ep := range ahs.healLocalDisks {
		endpoints = append(endpoints, ep)
	}
	return endpoints
}

func (ahs *allHealState) pushHealLocalDisks(healLocalDisks ...Endpoint) {
	ahs.Lock()
	defer ahs.Unlock()

	for _, ep := range healLocalDisks {
		ahs.healLocalDisks[ep] = struct{}{}
	}
}

func (ahs *allHealState) periodicHealSeqsClean(ctx context.Context) {
	// Launch clean-up routine to remove this heal sequence (after
	// it ends) from the global state after timeout has elapsed.
	periodicTimer := time.NewTimer(time.Minute * 5)
	defer periodicTimer.Stop()

	for {
		select {
		case <-periodicTimer.C:
			periodicTimer.Reset(time.Minute * 5)
			now := UTCNow()
			ahs.Lock()
			for path, h := range ahs.healSeqMap {
				if h.hasEnded() && h.endTime.Add(keepHealSeqStateDuration).Before(now) {
					delete(ahs.healSeqMap, path)
				}
			}
			ahs.Unlock()
		case <-ctx.Done():
			// server could be restarting - need
			// to exit immediately
			return
		}
	}
}

// getHealSequenceByToken - Retrieve a heal sequence by token. The second
// argument returns if a heal sequence actually exists.
func (ahs *allHealState) getHealSequenceByToken(token string) (h *healSequence, exists bool) {
	ahs.Lock()
	defer ahs.Unlock()
	for _, healSeq := range ahs.healSeqMap {
		if healSeq.clientToken == token {
			return healSeq, true
		}
	}
	return nil, false
}

// getHealSequence - Retrieve a heal sequence by path. The second
// argument returns if a heal sequence actually exists.
func (ahs *allHealState) getHealSequence(path string) (h *healSequence, exists bool) {
	ahs.Lock()
	defer ahs.Unlock()
	h, exists = ahs.healSeqMap[path]
	return h, exists
}

func (ahs *allHealState) stopHealSequence(path string) ([]byte, APIError) {
	var hsp madmin.HealStopSuccess
	he, exists := ahs.getHealSequence(path)
	if !exists {
		hsp = madmin.HealStopSuccess{
			ClientToken: "unknown",
			StartTime:   UTCNow(),
		}
	} else {
		clientToken := he.clientToken
		if globalIsDistErasure {
			clientToken = fmt.Sprintf("%s@%d", he.clientToken, GetProxyEndpointLocalIndex(globalProxyEndpoints))
		}

		hsp = madmin.HealStopSuccess{
			ClientToken:   clientToken,
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
	return b, toAdminAPIErr(GlobalContext, err)
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
func (ahs *allHealState) LaunchNewHealSequence(h *healSequence, objAPI ObjectLayer) (
	respBytes []byte, apiErr APIError, errMsg string) {

	if h.forceStarted {
		_, apiErr = ahs.stopHealSequence(pathJoin(h.bucket, h.object))
		if apiErr.Code != "" {
			return respBytes, apiErr, ""
		}
	} else {
		oh, exists := ahs.getHealSequence(pathJoin(h.bucket, h.object))
		if exists && !oh.hasEnded() {
			errMsg = "Heal is already running on the given path " +
				"(use force-start option to stop and start afresh). " +
				fmt.Sprintf("The heal was started by IP %s at %s, token is %s",
					h.clientAddress, h.startTime.Format(http.TimeFormat), h.clientToken)
			return nil, errorCodes.ToAPIErr(ErrHealAlreadyRunning), errMsg
		}
	}

	ahs.Lock()
	defer ahs.Unlock()

	// Check if new heal sequence to be started overlaps with any
	// existing, running sequence
	hpath := pathJoin(h.bucket, h.object)
	for k, hSeq := range ahs.healSeqMap {
		if !hSeq.hasEnded() && (HasPrefix(k, hpath) || HasPrefix(hpath, k)) {
			errMsg = "The provided heal sequence path overlaps with an existing " +
				fmt.Sprintf("heal path: %s", k)
			return nil, errorCodes.ToAPIErr(ErrHealOverlappingPaths), errMsg
		}
	}

	// Add heal state and start sequence
	ahs.healSeqMap[hpath] = h

	// Launch top-level background heal go-routine
	go h.healSequenceStart(objAPI)

	clientToken := h.clientToken
	if globalIsDistErasure {
		clientToken = fmt.Sprintf("%s@%d", h.clientToken, GetProxyEndpointLocalIndex(globalProxyEndpoints))
	}

	b, err := json.Marshal(madmin.HealStartSuccess{
		ClientToken:   clientToken,
		ClientAddress: h.clientAddress,
		StartTime:     h.startTime,
	})
	if err != nil {
		logger.LogIf(h.ctx, err)
		return nil, toAdminAPIErr(h.ctx, err), ""
	}
	return b, noError, ""
}

// PopHealStatusJSON - Called by heal-status API. It fetches the heal
// status results from global state and returns its JSON
// representation. The clientToken helps ensure there aren't
// conflicting clients fetching status.
func (ahs *allHealState) PopHealStatusJSON(hpath string,
	clientToken string) ([]byte, APIErrorCode) {

	// fetch heal state for given path
	h, exists := ahs.getHealSequence(hpath)
	if !exists {
		// heal sequence doesn't exist, must have finished.
		jbytes, err := json.Marshal(healSequenceStatus{
			Summary: healFinishedStatus,
		})
		return jbytes, toAdminAPIErrCode(GlobalContext, err)
	}

	// Check if client-token is valid
	if clientToken != h.clientToken {
		return nil, ErrHealInvalidClientToken
	}

	// Take lock to access and update the heal-sequence
	h.mutex.Lock()
	defer h.mutex.Unlock()

	numItems := len(h.currentStatus.Items)

	// calculate index of most recently available heal result
	// record.
	lastResultIndex := h.lastSentResultIndex
	if numItems > 0 {
		lastResultIndex = h.currentStatus.Items[numItems-1].ResultIndex
	}

	h.lastSentResultIndex = lastResultIndex

	jbytes, err := json.Marshal(h.currentStatus)
	if err != nil {
		h.currentStatus.Items = nil

		logger.LogIf(h.ctx, err)
		return nil, ErrInternalError
	}

	h.currentStatus.Items = nil

	return jbytes, ErrNone
}

// healSource denotes single entity and heal option.
type healSource struct {
	bucket    string
	object    string
	versionID string
	opts      *madmin.HealOpts // optional heal option overrides default setting
}

// healSequence - state for each heal sequence initiated on the
// server.
type healSequence struct {
	// bucket, and object on which heal seq. was initiated
	bucket, object string

	// A channel of entities (format, buckets, objects) to heal
	sourceCh chan healSource

	// A channel of entities with heal result
	respCh chan healResult

	// Report healing progress
	reportProgress bool

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

	// canceler to cancel heal sequence.
	cancelCtx context.CancelFunc

	// the last result index sent to client
	lastSentResultIndex int64

	// Number of total items scanned against item type
	scannedItemsMap map[madmin.HealItemType]int64

	// Number of total items healed against item type
	healedItemsMap map[madmin.HealItemType]int64

	// Number of total items where healing failed against endpoint and drive state
	healFailedItemsMap map[string]int64

	// The time of the last scan/heal activity
	lastHealActivity time.Time

	// Holds the request-info for logging
	ctx context.Context

	// used to lock this structure as it is concurrently accessed
	mutex sync.RWMutex
}

// NewHealSequence - creates healSettings, assumes bucket and
// objPrefix are already validated.
func newHealSequence(ctx context.Context, bucket, objPrefix, clientAddr string,
	hs madmin.HealOpts, forceStart bool) *healSequence {

	reqInfo := &logger.ReqInfo{RemoteHost: clientAddr, API: "Heal", BucketName: bucket}
	reqInfo.AppendTags("prefix", objPrefix)
	ctx, cancel := context.WithCancel(logger.SetReqInfo(ctx, reqInfo))

	clientToken := mustGetUUID()

	return &healSequence{
		respCh:         make(chan healResult),
		bucket:         bucket,
		object:         objPrefix,
		reportProgress: true,
		startTime:      UTCNow(),
		clientToken:    clientToken,
		clientAddress:  clientAddr,
		forceStarted:   forceStart,
		settings:       hs,
		currentStatus: healSequenceStatus{
			Summary:      healNotStartedStatus,
			HealSettings: hs,
		},
		traverseAndHealDoneCh: make(chan error),
		cancelCtx:             cancel,
		ctx:                   ctx,
		scannedItemsMap:       make(map[madmin.HealItemType]int64),
		healedItemsMap:        make(map[madmin.HealItemType]int64),
		healFailedItemsMap:    make(map[string]int64),
	}
}

// getScannedItemsCount - returns a count of all scanned items
func (h *healSequence) getScannedItemsCount() int64 {
	var count int64
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	for _, v := range h.scannedItemsMap {
		count = count + v
	}
	return count
}

// getScannedItemsMap - returns map of all scanned items against type
func (h *healSequence) getScannedItemsMap() map[madmin.HealItemType]int64 {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	// Make a copy before returning the value
	retMap := make(map[madmin.HealItemType]int64, len(h.scannedItemsMap))
	for k, v := range h.scannedItemsMap {
		retMap[k] = v
	}

	return retMap
}

// getHealedItemsMap - returns the map of all healed items against type
func (h *healSequence) getHealedItemsMap() map[madmin.HealItemType]int64 {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	// Make a copy before returning the value
	retMap := make(map[madmin.HealItemType]int64, len(h.healedItemsMap))
	for k, v := range h.healedItemsMap {
		retMap[k] = v
	}

	return retMap
}

// gethealFailedItemsMap - returns map of all items where heal failed against
// drive endpoint and status
func (h *healSequence) gethealFailedItemsMap() map[string]int64 {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	// Make a copy before returning the value
	retMap := make(map[string]int64, len(h.healFailedItemsMap))
	for k, v := range h.healFailedItemsMap {
		retMap[k] = v
	}

	return retMap
}

// isQuitting - determines if the heal sequence is quitting (due to an
// external signal)
func (h *healSequence) isQuitting() bool {
	select {
	case <-h.ctx.Done():
		return true
	default:
		return false
	}
}

// check if the heal sequence has ended
func (h *healSequence) hasEnded() bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	// background heal never ends
	if h.clientToken == bgHealingUUID {
		return false
	}
	return !h.endTime.IsZero()
}

// stops the heal sequence - safe to call multiple times.
func (h *healSequence) stop() {
	h.cancelCtx()
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
		h.mutex.Lock()
		itemsLen = len(h.currentStatus.Items)
		if itemsLen == maxUnconsumedHealResultItems {
			// wait for a second, or quit if an external
			// stop signal is received or the
			// unconsumedTimer fires.
			select {
			// Check after a second
			case <-time.After(time.Second):
				h.mutex.Unlock()
				continue

			case <-h.ctx.Done():
				h.mutex.Unlock()
				// discard result and return.
				return errHealStopSignalled

			// Timeout if no results consumed for too long.
			case <-unconsumedTimer.C:
				h.mutex.Unlock()
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
	h.mutex.Unlock()

	return nil
}

// healSequenceStart - this is the top-level background heal
// routine. It launches another go-routine that actually traverses
// on-disk data, checks and heals according to the selected
// settings. This go-routine itself, (1) monitors the traversal
// routine for completion, and (2) listens for external stop
// signals. When either event happens, it sets the finish status for
// the heal-sequence.
func (h *healSequence) healSequenceStart(objAPI ObjectLayer) {
	// Set status as running
	h.mutex.Lock()
	h.currentStatus.Summary = healRunningStatus
	h.currentStatus.StartTime = UTCNow()
	h.mutex.Unlock()

	if h.sourceCh == nil {
		go h.traverseAndHeal(objAPI)
	} else {
		go h.healFromSourceCh()
	}

	select {
	case err, ok := <-h.traverseAndHealDoneCh:
		if !ok {
			return
		}
		h.mutex.Lock()
		h.endTime = UTCNow()
		// Heal traversal is complete.
		if err == nil {
			// heal traversal succeeded.
			h.currentStatus.Summary = healFinishedStatus
		} else {
			// heal traversal had an error.
			h.currentStatus.Summary = healStoppedStatus
			h.currentStatus.FailureDetail = err.Error()
		}
		h.mutex.Unlock()
	case <-h.ctx.Done():
		h.mutex.Lock()
		h.endTime = UTCNow()
		h.currentStatus.Summary = healFinishedStatus
		h.mutex.Unlock()

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

func (h *healSequence) logHeal(healType madmin.HealItemType) {
	h.mutex.Lock()
	h.scannedItemsMap[healType]++
	h.lastHealActivity = UTCNow()
	h.mutex.Unlock()
}

func (h *healSequence) queueHealTask(source healSource, healType madmin.HealItemType) error {
	globalHealConfigMu.Lock()
	opts := globalHealConfig
	globalHealConfigMu.Unlock()

	// Send heal request
	task := healTask{
		bucket:     source.bucket,
		object:     source.object,
		versionID:  source.versionID,
		opts:       h.settings,
		responseCh: h.respCh,
	}
	if source.opts != nil {
		task.opts = *source.opts
	}
	if opts.Bitrot {
		task.opts.ScanMode = madmin.HealDeepScan
	}

	// Wait and proceed if there are active requests
	waitForLowHTTPReq(opts.IOCount, opts.Sleep)

	h.mutex.Lock()
	h.scannedItemsMap[healType]++
	h.lastHealActivity = UTCNow()
	h.mutex.Unlock()

	globalBackgroundHealRoutine.queueHealTask(task)

	select {
	case res := <-h.respCh:
		if !h.reportProgress {
			// Object might have been deleted, by the time heal
			// was attempted, we should ignore this object and
			// return the error and not calculate this object
			// as part of the metrics.
			if isErrObjectNotFound(res.err) || isErrVersionNotFound(res.err) {
				// Return the error so that caller can handle it.
				return res.err
			}

			h.mutex.Lock()
			defer h.mutex.Unlock()

			// Progress is not reported in case of background heal processing.
			// Instead we increment relevant counter based on the heal result
			// for prometheus reporting.
			if res.err != nil {
				for _, d := range res.result.After.Drives {
					// For failed items we report the endpoint and drive state
					// This will help users take corrective actions for drives
					h.healFailedItemsMap[d.Endpoint+","+d.State]++
				}
			} else {
				// Only object type reported for successful healing
				h.healedItemsMap[res.result.Type]++
			}

			// Report caller of any failure
			return res.err
		}
		res.result.Type = healType
		if res.err != nil {
			// Object might have been deleted, by the time heal
			// was attempted, we should ignore this object and return success.
			if isErrObjectNotFound(res.err) || isErrVersionNotFound(res.err) {
				return nil
			}
			// Only report object error
			if healType != madmin.HealItemObject {
				return res.err
			}
			res.result.Detail = res.err.Error()
		}
		return h.pushHealResultItem(res.result)
	case <-h.ctx.Done():
		return nil
	}
}

func (h *healSequence) healItemsFromSourceCh() error {
	for {
		select {
		case source, ok := <-h.sourceCh:
			if !ok {
				return nil
			}

			var itemType madmin.HealItemType
			switch source.bucket {
			case nopHeal:
				continue
			case SlashSeparator:
				itemType = madmin.HealItemMetadata
			default:
				if source.object == "" {
					itemType = madmin.HealItemBucket
				} else {
					itemType = madmin.HealItemObject
				}
			}

			if err := h.queueHealTask(source, itemType); err != nil {
				switch err.(type) {
				case ObjectExistsAsDirectory:
				case ObjectNotFound:
				case VersionNotFound:
				default:
					logger.LogIf(h.ctx, fmt.Errorf("Heal attempt failed for %s: %w",
						pathJoin(source.bucket, source.object), err))
				}
			}
		case <-h.ctx.Done():
			return nil
		}
	}
}

func (h *healSequence) healFromSourceCh() {
	h.healItemsFromSourceCh()
}

func (h *healSequence) healDiskMeta(objAPI ObjectLayer) error {
	// Start healing the config prefix.
	return h.healMinioSysMeta(objAPI, minioConfigPrefix)()
}

func (h *healSequence) healItems(objAPI ObjectLayer, bucketsOnly bool) error {
	if err := h.healDiskMeta(objAPI); err != nil {
		return err
	}

	// Heal buckets and objects
	return h.healBuckets(objAPI, bucketsOnly)
}

// traverseAndHeal - traverses on-disk data and performs healing
// according to settings. At each "safe" point it also checks if an
// external quit signal has been received and quits if so. Since the
// healing traversal may be mutating on-disk data when an external
// quit signal is received, this routine cannot quit immediately and
// has to wait until a safe point is reached, such as between scanning
// two objects.
func (h *healSequence) traverseAndHeal(objAPI ObjectLayer) {
	bucketsOnly := false // Heals buckets and objects also.
	h.traverseAndHealDoneCh <- h.healItems(objAPI, bucketsOnly)
	close(h.traverseAndHealDoneCh)
}

// healMinioSysMeta - heals all files under a given meta prefix, returns a function
// which in-turn heals the respective meta directory path and any files in int.
func (h *healSequence) healMinioSysMeta(objAPI ObjectLayer, metaPrefix string) func() error {
	return func() error {
		// NOTE: Healing on meta is run regardless
		// of any bucket being selected, this is to ensure that
		// meta are always upto date and correct.
		return objAPI.HealObjects(h.ctx, minioMetaBucket, metaPrefix, h.settings, func(bucket, object, versionID string) error {
			if h.isQuitting() {
				return errHealStopSignalled
			}

			err := h.queueHealTask(healSource{
				bucket:    bucket,
				object:    object,
				versionID: versionID,
			}, madmin.HealItemBucketMetadata)
			// Object might have been deleted, by the time heal
			// was attempted we ignore this object an move on.
			if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
				return nil
			}
			return err
		})
	}
}

// healDiskFormat - heals format.json, return value indicates if a
// failure error occurred.
func (h *healSequence) healDiskFormat() error {
	if h.isQuitting() {
		return errHealStopSignalled
	}

	return h.queueHealTask(healSource{bucket: SlashSeparator}, madmin.HealItemMetadata)
}

// healBuckets - check for all buckets heal or just particular bucket.
func (h *healSequence) healBuckets(objAPI ObjectLayer, bucketsOnly bool) error {
	if h.isQuitting() {
		return errHealStopSignalled
	}

	// 1. If a bucket was specified, heal only the bucket.
	if h.bucket != "" {
		return h.healBucket(objAPI, h.bucket, bucketsOnly)
	}

	buckets, err := objAPI.ListBuckets(h.ctx)
	if err != nil {
		return errFnHealFromAPIErr(h.ctx, err)
	}

	// Heal latest buckets first.
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Created.After(buckets[j].Created)
	})

	for _, bucket := range buckets {
		if err = h.healBucket(objAPI, bucket.Name, bucketsOnly); err != nil {
			return err
		}
	}

	return nil
}

// healBucket - traverses and heals given bucket
func (h *healSequence) healBucket(objAPI ObjectLayer, bucket string, bucketsOnly bool) error {
	if err := h.queueHealTask(healSource{bucket: bucket}, madmin.HealItemBucket); err != nil {
		if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
			return err
		}
	}

	if bucketsOnly {
		return nil
	}

	if !h.settings.Recursive {
		if h.object != "" {
			// Check if an object named as the objPrefix exists,
			// and if so heal it.
			oi, err := objAPI.GetObjectInfo(h.ctx, bucket, h.object, ObjectOptions{})
			if err == nil {
				if err = h.healObject(bucket, h.object, oi.VersionID); err != nil {
					if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
						return nil
					}
					return err
				}
			}
		}

		return nil
	}

	if err := objAPI.HealObjects(h.ctx, bucket, h.object, h.settings, h.healObject); err != nil {
		// Object might have been deleted, by the time heal
		// was attempted we ignore this object an move on.
		if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
			return errFnHealFromAPIErr(h.ctx, err)
		}
	}
	return nil
}

// healObject - heal the given object and record result
func (h *healSequence) healObject(bucket, object, versionID string) error {
	if h.isQuitting() {
		return errHealStopSignalled
	}

	err := h.queueHealTask(healSource{
		bucket:    bucket,
		object:    object,
		versionID: versionID,
	}, madmin.HealItemObject)
	return err
}
