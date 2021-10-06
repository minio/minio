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
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/bucket/replication"
)

//go:generate msgp -file=$GOFILE

// replicatedTargetInfo struct represents replication info on a target
type replicatedTargetInfo struct {
	Arn                   string
	Size                  int64
	ReplicationAction     replicationAction // full or metadata only
	OpType                replication.Type  // whether incoming replication, existing object, healing etc..
	ReplicationStatus     replication.StatusType
	PrevReplicationStatus replication.StatusType
	VersionPurgeStatus    VersionPurgeStatusType
	ResyncTimestamp       string
	ReplicationResynced   bool // true only if resync attempted for this target
}

// Empty returns true for a target if arn is empty
func (rt replicatedTargetInfo) Empty() bool {
	return rt.Arn == ""
}

type replicatedInfos struct {
	ReplicationTimeStamp time.Time
	Targets              []replicatedTargetInfo
}

func (ri replicatedInfos) CompletedSize() (sz int64) {
	for _, t := range ri.Targets {
		if t.Empty() {
			continue
		}
		if t.ReplicationStatus == replication.Completed && t.PrevReplicationStatus != replication.Completed {
			sz += t.Size
		}
	}
	return sz
}

// ReplicationAttempted returns true if replication was attempted on any of the targets for the object version
// queued
func (ri replicatedInfos) ReplicationResynced() bool {
	for _, t := range ri.Targets {
		if t.Empty() || !t.ReplicationResynced {
			continue
		}
		return true
	}
	return false
}

func (ri replicatedInfos) ReplicationStatusInternal() string {
	b := new(bytes.Buffer)
	for _, t := range ri.Targets {
		if t.Empty() {
			continue
		}
		fmt.Fprintf(b, "%s=%s;", t.Arn, t.ReplicationStatus.String())
	}
	return b.String()
}

func (ri replicatedInfos) ReplicationStatus() replication.StatusType {
	if len(ri.Targets) == 0 {
		return replication.StatusType("")
	}
	completed := 0
	for _, v := range ri.Targets {
		switch v.ReplicationStatus {
		case replication.Failed:
			return replication.Failed
		case replication.Completed:
			completed++
		}
	}
	if completed == len(ri.Targets) {
		return replication.Completed
	}
	return replication.Pending
}

func (ri replicatedInfos) VersionPurgeStatus() VersionPurgeStatusType {
	if len(ri.Targets) == 0 {
		return VersionPurgeStatusType("")
	}
	completed := 0
	for _, v := range ri.Targets {
		switch v.VersionPurgeStatus {
		case Failed:
			return Failed
		case Complete:
			completed++
		}
	}
	if completed == len(ri.Targets) {
		return Complete
	}
	return Pending
}

func (ri replicatedInfos) VersionPurgeStatusInternal() string {
	b := new(bytes.Buffer)
	for _, t := range ri.Targets {
		if t.Empty() {
			continue
		}
		if t.VersionPurgeStatus.Empty() {
			continue
		}
		fmt.Fprintf(b, "%s=%s;", t.Arn, t.VersionPurgeStatus)
	}
	return b.String()
}

func (ri replicatedInfos) Action() replicationAction {
	for _, t := range ri.Targets {
		if t.Empty() {
			continue
		}
		// rely on replication action from target that actually performed replication now.
		if t.PrevReplicationStatus != replication.Completed {
			return t.ReplicationAction
		}
	}
	return replicateNone
}

var replStatusRegex = regexp.MustCompile(`([^=].*?)=([^,].*?);`)

// TargetReplicationStatus - returns replication status of a target
func (o *ObjectInfo) TargetReplicationStatus(arn string) (status replication.StatusType) {
	repStatMatches := replStatusRegex.FindAllStringSubmatch(o.ReplicationStatusInternal, -1)
	for _, repStatMatch := range repStatMatches {
		if len(repStatMatch) != 3 {
			return
		}
		if repStatMatch[1] == arn {
			return replication.StatusType(repStatMatch[2])
		}
	}
	return
}

type replicateTargetDecision struct {
	Replicate   bool   // Replicate to this target
	Synchronous bool   // Synchronous replication configured.
	Arn         string //ARN of replication target
	ID          string
}

func (t *replicateTargetDecision) String() string {
	return fmt.Sprintf("%t;%t;%s;%s", t.Replicate, t.Synchronous, t.Arn, t.ID)
}

func newReplicateTargetDecision(arn string, replicate bool, sync bool) replicateTargetDecision {
	d := replicateTargetDecision{
		Replicate:   replicate,
		Synchronous: sync,
		Arn:         arn,
	}
	return d
}

// ReplicateDecision represents replication decision for each target
type ReplicateDecision struct {
	targetsMap map[string]replicateTargetDecision
}

// ReplicateAny returns true if atleast one target qualifies for replication
func (d *ReplicateDecision) ReplicateAny() bool {
	for _, t := range d.targetsMap {
		if t.Replicate {
			return true
		}
	}
	return false
}

// Synchronous returns true if atleast one target qualifies for synchronous replication
func (d *ReplicateDecision) Synchronous() bool {
	for _, t := range d.targetsMap {
		if t.Synchronous {
			return true
		}
	}
	return false
}

func (d *ReplicateDecision) String() string {
	b := new(bytes.Buffer)
	for key, value := range d.targetsMap {
		fmt.Fprintf(b, "%s=%s,", key, value.String())
	}
	return strings.TrimSuffix(b.String(), ",")
}

// Set updates ReplicateDecision with target's replication decision
func (d *ReplicateDecision) Set(t replicateTargetDecision) {
	if d.targetsMap == nil {
		d.targetsMap = make(map[string]replicateTargetDecision)
	}
	d.targetsMap[t.Arn] = t
}

// PendingStatus returns a stringified representation of internal replication status with all targets marked as `PENDING`
func (d *ReplicateDecision) PendingStatus() string {
	b := new(bytes.Buffer)
	for _, k := range d.targetsMap {
		if k.Replicate {
			fmt.Fprintf(b, "%s=%s;", k.Arn, replication.Pending.String())
		}
	}
	return b.String()
}

// ResyncDecision is a struct representing a map with target's individual resync decisions
type ResyncDecision struct {
	targets map[string]ResyncTargetDecision
}

// Empty returns true if no targets with resync decision present
func (r *ResyncDecision) Empty() bool {
	return r.targets == nil
}

func (r *ResyncDecision) mustResync() bool {
	for _, v := range r.targets {
		if v.Replicate {
			return true
		}
	}
	return false
}

func (r *ResyncDecision) mustResyncTarget(tgtArn string) bool {
	if r.targets == nil {
		return false
	}
	v, ok := r.targets[tgtArn]
	if ok && v.Replicate {
		return true
	}
	return false
}

// ResyncTargetDecision is struct that represents resync decision for this target
type ResyncTargetDecision struct {
	Replicate       bool
	ResetID         string
	ResetBeforeDate time.Time
}

var errInvalidReplicateDecisionFormat = fmt.Errorf("ReplicateDecision has invalid format")

// parse k-v pairs of target ARN to stringified ReplicateTargetDecision delimited by ',' into a
// ReplicateDecision struct
func parseReplicateDecision(s string) (r ReplicateDecision, err error) {
	r = ReplicateDecision{
		targetsMap: make(map[string]replicateTargetDecision),
	}
	if len(s) == 0 {
		return
	}
	pairs := strings.Split(s, ",")
	for _, p := range pairs {
		slc := strings.Split(p, "=")
		if len(slc) != 2 {
			return r, errInvalidReplicateDecisionFormat
		}
		tgtStr := strings.TrimPrefix(slc[1], "\"")
		tgtStr = strings.TrimSuffix(tgtStr, "\"")
		tgt := strings.Split(tgtStr, ";")
		if len(tgt) != 4 {
			return r, errInvalidReplicateDecisionFormat
		}
		var replicate, sync bool
		var err error
		replicate, err = strconv.ParseBool(tgt[0])
		if err != nil {
			return r, err
		}
		sync, err = strconv.ParseBool(tgt[1])
		if err != nil {
			return r, err
		}
		r.targetsMap[slc[0]] = replicateTargetDecision{Replicate: replicate, Synchronous: sync, Arn: tgt[2], ID: tgt[3]}
	}
	return
}

// ReplicationState represents internal replication state
type ReplicationState struct {
	ReplicaTimeStamp          time.Time              // timestamp when last replica update was received
	ReplicaStatus             replication.StatusType // replica statusstringis
	DeleteMarker              bool                   // represents DeleteMarker replication state
	ReplicationTimeStamp      time.Time              // timestamp when last replication activity happened
	ReplicationStatusInternal string                 // stringified representation of all replication activity
	// VersionPurgeStatusInternal is internally in the format "arn1=PENDING;arn2=COMMPLETED;"
	VersionPurgeStatusInternal string                            // stringified representation of all version purge statuses
	ReplicateDecisionStr       string                            // stringified representation of replication decision for each target
	Targets                    map[string]replication.StatusType // map of ARN->replication status for ongoing replication activity
	PurgeTargets               map[string]VersionPurgeStatusType // map of ARN->VersionPurgeStatus for all the targets
	ResetStatusesMap           map[string]string                 // map of ARN-> stringified reset id and timestamp for all the targets
}

// Equal returns true if replication state is identical for version purge statuses and (replica)tion statuses.
func (rs *ReplicationState) Equal(o ReplicationState) bool {
	return rs.ReplicaStatus == o.ReplicaStatus &&
		rs.ReplicaTimeStamp.Equal(o.ReplicaTimeStamp) &&
		rs.ReplicationTimeStamp.Equal(o.ReplicationTimeStamp) &&
		rs.ReplicationStatusInternal == o.ReplicationStatusInternal &&
		rs.VersionPurgeStatusInternal == o.VersionPurgeStatusInternal
}

// CompositeReplicationStatus returns overall replication status for the object version being replicated.
func (rs *ReplicationState) CompositeReplicationStatus() (st replication.StatusType) {
	switch {
	case rs.ReplicationStatusInternal != "":
		switch replication.StatusType(rs.ReplicationStatusInternal) {
		case replication.Pending, replication.Completed, replication.Failed, replication.Replica: // for backward compatibility
			return replication.StatusType(rs.ReplicationStatusInternal)
		default:
			replStatus := getCompositeReplicationStatus(rs.Targets)
			// return REPLICA status if replica received timestamp is later than replication timestamp
			// provided object replication completed for all targets.
			if !rs.ReplicaTimeStamp.Equal(timeSentinel) && replStatus == replication.Completed && rs.ReplicaTimeStamp.After(rs.ReplicationTimeStamp) {
				return rs.ReplicaStatus
			}
			return replStatus
		}
	case !rs.ReplicaStatus.Empty():
		return rs.ReplicaStatus
	default:
		return
	}
}

// CompositeVersionPurgeStatus returns overall replication purge status for the permanent delete being replicated.
func (rs *ReplicationState) CompositeVersionPurgeStatus() VersionPurgeStatusType {
	switch VersionPurgeStatusType(rs.VersionPurgeStatusInternal) {
	case Pending, Complete, Failed: // for backward compatibility
		return VersionPurgeStatusType(rs.VersionPurgeStatusInternal)
	default:
		return getCompositeVersionPurgeStatus(rs.PurgeTargets)
	}
}

// TargetState returns replicatedInfos struct initialized with the previous state of replication
func (rs *ReplicationState) targetState(arn string) (r replicatedTargetInfo) {
	return replicatedTargetInfo{
		Arn:                   arn,
		PrevReplicationStatus: rs.Targets[arn],
		VersionPurgeStatus:    rs.PurgeTargets[arn],
		ResyncTimestamp:       rs.ResetStatusesMap[arn],
	}
}

// getReplicationState returns replication state using target replicated info for the targets
func getReplicationState(rinfos replicatedInfos, prevState ReplicationState, vID string) ReplicationState {
	rs := ReplicationState{
		ReplicateDecisionStr: prevState.ReplicateDecisionStr,
		ResetStatusesMap:     prevState.ResetStatusesMap,
		ReplicaTimeStamp:     prevState.ReplicaTimeStamp,
		ReplicaStatus:        prevState.ReplicaStatus,
	}
	var replStatuses, vpurgeStatuses string
	replStatuses = rinfos.ReplicationStatusInternal()
	rs.Targets = replicationStatusesMap(replStatuses)
	rs.ReplicationStatusInternal = replStatuses
	rs.ReplicationTimeStamp = rinfos.ReplicationTimeStamp

	vpurgeStatuses = rinfos.VersionPurgeStatusInternal()
	rs.VersionPurgeStatusInternal = vpurgeStatuses
	rs.PurgeTargets = versionPurgeStatusesMap(vpurgeStatuses)

	for _, rinfo := range rinfos.Targets {
		if rinfo.ResyncTimestamp != "" {
			rs.ResetStatusesMap[targetResetHeader(rinfo.Arn)] = rinfo.ResyncTimestamp
		}
	}
	return rs
}

// constructs a replication status map from string representation
func replicationStatusesMap(s string) map[string]replication.StatusType {
	targets := make(map[string]replication.StatusType)
	repStatMatches := replStatusRegex.FindAllStringSubmatch(s, -1)
	for _, repStatMatch := range repStatMatches {
		if len(repStatMatch) != 3 {
			continue
		}
		status := replication.StatusType(repStatMatch[2])
		targets[repStatMatch[1]] = status
	}
	return targets
}

// constructs a version purge status map from string representation
func versionPurgeStatusesMap(s string) map[string]VersionPurgeStatusType {
	targets := make(map[string]VersionPurgeStatusType)
	purgeStatusMatches := replStatusRegex.FindAllStringSubmatch(s, -1)
	for _, purgeStatusMatch := range purgeStatusMatches {
		if len(purgeStatusMatch) != 3 {
			continue
		}
		targets[purgeStatusMatch[1]] = VersionPurgeStatusType(purgeStatusMatch[2])
	}
	return targets
}

// return the overall replication status for all the targets
func getCompositeReplicationStatus(m map[string]replication.StatusType) replication.StatusType {
	if len(m) == 0 {
		return replication.StatusType("")
	}
	completed := 0
	for _, v := range m {
		switch v {
		case replication.Failed:
			return replication.Failed
		case replication.Completed:
			completed++
		}
	}
	if completed == len(m) {
		return replication.Completed
	}
	return replication.Pending
}

// return the overall version purge status for all the targets
func getCompositeVersionPurgeStatus(m map[string]VersionPurgeStatusType) VersionPurgeStatusType {
	if len(m) == 0 {
		return VersionPurgeStatusType("")
	}
	completed := 0
	for _, v := range m {
		switch v {
		case Failed:
			return Failed
		case Complete:
			completed++
		}
	}
	if completed == len(m) {
		return Complete
	}
	return Pending
}

// getHealReplicateObjectInfo returns info needed by heal replication in ReplicateObjectInfo
func getHealReplicateObjectInfo(objInfo ObjectInfo, rcfg replicationConfig) ReplicateObjectInfo {
	oi := objInfo.Clone()
	if rcfg.Config != nil && rcfg.Config.RoleArn != "" {
		// For backward compatibility of objects pending/failed replication.
		// Save replication related statuses in the new internal representation for
		// compatible behavior.
		if !oi.ReplicationStatus.Empty() {
			oi.ReplicationStatusInternal = fmt.Sprintf("%s=%s;", rcfg.Config.RoleArn, oi.ReplicationStatus)
		}
		if !oi.VersionPurgeStatus.Empty() {
			oi.VersionPurgeStatusInternal = fmt.Sprintf("%s=%s;", rcfg.Config.RoleArn, oi.VersionPurgeStatus)
		}
		for k, v := range oi.UserDefined {
			switch {
			case strings.EqualFold(k, ReservedMetadataPrefixLower+ReplicationReset):
				delete(oi.UserDefined, k)
				oi.UserDefined[targetResetHeader(rcfg.Config.RoleArn)] = v
			}
		}
	}
	var dsc ReplicateDecision
	var tgtStatuses map[string]replication.StatusType
	if oi.DeleteMarker || !oi.VersionPurgeStatus.Empty() {
		dsc = checkReplicateDelete(GlobalContext, oi.Bucket, ObjectToDelete{
			ObjectName: oi.Name,
			VersionID:  oi.VersionID,
		}, oi, ObjectOptions{}, nil)
	} else {
		dsc = mustReplicate(GlobalContext, oi.Bucket, oi.Name, getMustReplicateOptions(ObjectInfo{
			UserDefined: oi.UserDefined,
		}, replication.HealReplicationType, ObjectOptions{}))
	}
	tgtStatuses = replicationStatusesMap(oi.ReplicationStatusInternal)

	existingObjResync := rcfg.Resync(GlobalContext, oi, &dsc, tgtStatuses)

	return ReplicateObjectInfo{
		ObjectInfo:        oi,
		OpType:            replication.HealReplicationType,
		Dsc:               dsc,
		ExistingObjResync: existingObjResync,
		TargetStatuses:    tgtStatuses,
	}
}

// vID here represents the versionID client specified in request - need to distinguish between delete marker and delete marker deletion
func (o *ObjectInfo) getReplicationState(dsc string, vID string, heal bool) ReplicationState {
	rs := ReplicationState{
		ReplicationStatusInternal:  o.ReplicationStatusInternal,
		VersionPurgeStatusInternal: o.VersionPurgeStatusInternal,
		ReplicateDecisionStr:       dsc,
		Targets:                    make(map[string]replication.StatusType),
		PurgeTargets:               make(map[string]VersionPurgeStatusType),
		ResetStatusesMap:           make(map[string]string),
	}
	rs.Targets = replicationStatusesMap(o.ReplicationStatusInternal)
	rs.PurgeTargets = versionPurgeStatusesMap(o.VersionPurgeStatusInternal)
	for k, v := range o.UserDefined {
		if strings.HasPrefix(k, ReservedMetadataPrefixLower+ReplicationReset) {
			arn := strings.TrimPrefix(k, fmt.Sprintf("%s-", ReservedMetadataPrefixLower+ReplicationReset))
			rs.ResetStatusesMap[arn] = v
		}
	}
	return rs
}

// ReplicationState returns replication state using other internal replication metadata in ObjectToDelete
func (o *ObjectToDelete) ReplicationState() ReplicationState {
	r := ReplicationState{
		ReplicationStatusInternal:  o.DeleteMarkerReplicationStatus,
		VersionPurgeStatusInternal: o.VersionPurgeStatuses,
		ReplicateDecisionStr:       o.ReplicateDecisionStr,
	}

	r.Targets = replicationStatusesMap(o.DeleteMarkerReplicationStatus)
	r.PurgeTargets = versionPurgeStatusesMap(o.VersionPurgeStatuses)
	return r
}

// VersionPurgeStatus returns a composite version purge status across targets
func (d *DeletedObject) VersionPurgeStatus() VersionPurgeStatusType {
	return d.ReplicationState.CompositeVersionPurgeStatus()
}

// DeleteMarkerReplicationStatus return composite replication status of delete marker across targets
func (d *DeletedObject) DeleteMarkerReplicationStatus() replication.StatusType {
	return d.ReplicationState.CompositeReplicationStatus()
}

// ResyncTargetsInfo holds a slice of targets with resync info per target
type ResyncTargetsInfo struct {
	Targets []ResyncTarget `json:"target,omitempty"`
}

// ResyncTarget is a struct representing the Target reset ID where target is identified by its Arn
type ResyncTarget struct {
	Arn     string `json:"arn"`
	ResetID string `json:"resetid"`
}

// VersionPurgeStatusType represents status of a versioned delete or permanent delete w.r.t bucket replication
type VersionPurgeStatusType string

const (
	// Pending - versioned delete replication is pending.
	Pending VersionPurgeStatusType = "PENDING"

	// Complete - versioned delete replication is now complete, erase version on disk.
	Complete VersionPurgeStatusType = "COMPLETE"

	// Failed - versioned delete replication failed.
	Failed VersionPurgeStatusType = "FAILED"
)

// Empty returns true if purge status was not set.
func (v VersionPurgeStatusType) Empty() bool {
	return string(v) == ""
}

// Pending returns true if the version is pending purge.
func (v VersionPurgeStatusType) Pending() bool {
	return v == Pending || v == Failed
}
