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
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7/pkg/tags"
	sse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/s3select"
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
	if globalIsGateway {
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		return nil, BucketLifecycleNotFound{Bucket: bucketName}
	}

	return globalBucketMetadataSys.GetLifecycleConfig(bucketName)
}

// NewLifecycleSys - creates new lifecycle system.
func NewLifecycleSys() *LifecycleSys {
	return &LifecycleSys{}
}

type expiryTask struct {
	objInfo       ObjectInfo
	versionExpiry bool
}

type expiryState struct {
	once     sync.Once
	expiryCh chan expiryTask
}

func (es *expiryState) queueExpiryTask(oi ObjectInfo, rmVersion bool) {
	select {
	case <-GlobalContext.Done():
		es.once.Do(func() {
			close(es.expiryCh)
		})
	case es.expiryCh <- expiryTask{objInfo: oi, versionExpiry: rmVersion}:
	default:
	}
}

var (
	globalExpiryState *expiryState
)

func newExpiryState() *expiryState {
	return &expiryState{
		expiryCh: make(chan expiryTask, 10000),
	}
}

func initBackgroundExpiry(ctx context.Context, objectAPI ObjectLayer) {
	globalExpiryState = newExpiryState()
	go func() {
		for t := range globalExpiryState.expiryCh {
			applyExpiryRule(ctx, objectAPI, t.objInfo, false, t.versionExpiry)
		}
	}()
}

type transitionState struct {
	once sync.Once
	// add future metrics here
	transitionCh chan ObjectInfo
}

func (t *transitionState) queueTransitionTask(oi ObjectInfo) {
	select {
	case <-GlobalContext.Done():
		t.once.Do(func() {
			close(t.transitionCh)
		})
	case t.transitionCh <- oi:
	default:
	}
}

var (
	globalTransitionState      *transitionState
	globalTransitionConcurrent = runtime.GOMAXPROCS(0) / 2
)

func newTransitionState() *transitionState {
	// fix minimum concurrent transition to 1 for single CPU setup
	if globalTransitionConcurrent == 0 {
		globalTransitionConcurrent = 1
	}
	return &transitionState{
		transitionCh: make(chan ObjectInfo, 10000),
	}
}

// addWorker creates a new worker to process tasks
func (t *transitionState) addWorker(ctx context.Context, objectAPI ObjectLayer) {
	// Add a new worker.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case oi, ok := <-t.transitionCh:
				if !ok {
					return
				}

				if err := transitionObject(ctx, objectAPI, oi); err != nil {
					logger.LogIf(ctx, fmt.Errorf("Transition failed for %s/%s version:%s with %w", oi.Bucket, oi.Name, oi.VersionID, err))
				}
			}
		}
	}()
}

func initBackgroundTransition(ctx context.Context, objectAPI ObjectLayer) {
	if globalTransitionState == nil {
		return
	}

	// Start with globalTransitionConcurrent.
	for i := 0; i < globalTransitionConcurrent; i++ {
		globalTransitionState.addWorker(ctx, objectAPI)
	}
}

var errInvalidStorageClass = errors.New("invalid storage class")

func validateTransitionTier(ctx context.Context, lfc *lifecycle.Lifecycle) error {
	for _, rule := range lfc.Rules {
		if rule.Transition.StorageClass == "" {
			continue
		}
		if valid := globalTierConfigMgr.IsTierValid(rule.Transition.StorageClass); !valid {
			return errInvalidStorageClass
		}
	}
	return nil
}

// expireAction represents different actions to be performed on expiry of a
// restored/transitioned object
type expireAction int

const (
	// ignore the zero value
	_ expireAction = iota
	// expireObj indicates expiry of 'regular' transitioned objects.
	expireObj
	// expireRestoredObj indicates expiry of restored objects.
	expireRestoredObj
)

// expireTransitionedObject handles expiry of transitioned/restored objects
// (versions) in one of the following situations:
//
// 1. when a restored (via PostRestoreObject API) object expires.
// 2. when a transitioned object expires (based on an ILM rule).
func expireTransitionedObject(ctx context.Context, objectAPI ObjectLayer, oi *ObjectInfo, lcOpts lifecycle.ObjectOpts, action expireAction) error {
	var opts ObjectOptions
	opts.Versioned = globalBucketVersioningSys.Enabled(oi.Bucket)
	opts.VersionID = lcOpts.VersionID
	switch action {
	case expireObj:
		// When an object is past expiry or when a transitioned object is being
		// deleted, 'mark' the data in the remote tier for delete.
		entry := jentry{
			ObjName:   oi.transitionedObjName,
			VersionID: oi.transitionVersionID,
			TierName:  oi.TransitionTier,
		}
		if err := globalTierJournal.AddEntry(entry); err != nil {
			logger.LogIf(ctx, err)
			return err
		}
		// Delete metadata on source, now that data in remote tier has been
		// marked for deletion.
		if _, err := objectAPI.DeleteObject(ctx, oi.Bucket, oi.Name, opts); err != nil {
			logger.LogIf(ctx, err)
			return err
		}

		// Send audit for the lifecycle delete operation
		auditLogLifecycle(ctx, oi.Bucket, oi.Name)

		eventName := event.ObjectRemovedDelete
		if lcOpts.DeleteMarker {
			eventName = event.ObjectRemovedDeleteMarkerCreated
		}
		objInfo := ObjectInfo{
			Name:         oi.Name,
			VersionID:    lcOpts.VersionID,
			DeleteMarker: lcOpts.DeleteMarker,
		}
		// Notify object deleted event.
		sendEvent(eventArgs{
			EventName:  eventName,
			BucketName: oi.Bucket,
			Object:     objInfo,
			Host:       "Internal: [ILM-EXPIRY]",
		})

	case expireRestoredObj:
		// delete locally restored copy of object or object version
		// from the source, while leaving metadata behind. The data on
		// transitioned tier lies untouched and still accessible
		opts.Transition.ExpireRestored = true
		_, err := objectAPI.DeleteObject(ctx, oi.Bucket, oi.Name, opts)
		return err
	default:
		return fmt.Errorf("Unknown expire action %v", action)
	}

	return nil
}

// generate an object name for transitioned object
func genTransitionObjName() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	us := u.String()
	obj := fmt.Sprintf("%s/%s/%s", us[0:2], us[2:4], us)
	return obj, nil
}

// transition object to target specified by the transition ARN. When an object is transitioned to another
// storage specified by the transition ARN, the metadata is left behind on source cluster and original content
// is moved to the transition tier. Note that in the case of encrypted objects, entire encrypted stream is moved
// to the transition tier without decrypting or re-encrypting.
func transitionObject(ctx context.Context, objectAPI ObjectLayer, oi ObjectInfo) error {
	lc, err := globalLifecycleSys.Get(oi.Bucket)
	if err != nil {
		return err
	}
	lcOpts := lifecycle.ObjectOpts{
		Name:     oi.Name,
		UserTags: oi.UserTags,
	}
	tierName := getLifeCycleTransitionTier(ctx, lc, oi.Bucket, lcOpts)
	opts := ObjectOptions{
		Transition: TransitionOptions{
			Status: lifecycle.TransitionPending,
			Tier:   tierName,
			ETag:   oi.ETag,
		},
		VersionID: oi.VersionID,
		Versioned: globalBucketVersioningSys.Enabled(oi.Bucket),
		MTime:     oi.ModTime,
	}
	return objectAPI.TransitionObject(ctx, oi.Bucket, oi.Name, opts)
}

// getLifeCycleTransitionTier returns storage class for transition target
func getLifeCycleTransitionTier(ctx context.Context, lc *lifecycle.Lifecycle, bucket string, obj lifecycle.ObjectOpts) string {
	for _, rule := range lc.FilterActionableRules(obj) {
		if rule.Transition.StorageClass != "" {
			return rule.Transition.StorageClass
		}
	}
	return ""
}

// getTransitionedObjectReader returns a reader from the transitioned tier.
func getTransitionedObjectReader(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, oi ObjectInfo, opts ObjectOptions) (gr *GetObjectReader, err error) {
	tgtClient, err := globalTierConfigMgr.getDriver(oi.TransitionTier)
	if err != nil {
		return nil, fmt.Errorf("transition storage class not configured")
	}

	fn, off, length, err := NewGetObjectReader(rs, oi, opts)
	if err != nil {
		return nil, ErrorRespToObjectError(err, bucket, object)
	}
	gopts := WarmBackendGetOpts{}

	// get correct offsets for object
	if off >= 0 && length >= 0 {
		gopts.startOffset = off
		gopts.length = length
	}

	reader, err := tgtClient.Get(ctx, oi.transitionedObjName, remoteVersionID(oi.transitionVersionID), gopts)
	if err != nil {
		return nil, err
	}
	closer := func() {
		reader.Close()
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
	EncryptionType sse.SSEAlgorithm `xml:"EncryptionType"`
	KMSContext     string           `xml:"KMSContext,omitempty"`
	KMSKeyID       string           `xml:"KMSKeyId,omitempty"`
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

var (
	selectParamsXMLName = "SelectParameters"
)

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
	Tier             string             `xml:"Tier,-"`
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
		if _, err := objAPI.GetBucketInfo(ctx, r.OutputLocation.S3.BucketName); err != nil {
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
	versioned := globalBucketVersioningSys.Enabled(bucket)
	versionSuspended := globalBucketVersioningSys.Suspended(bucket)
	vid := strings.TrimSpace(r.URL.Query().Get(xhttp.VersionID))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
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
			if !strings.HasPrefix("x-amz-meta", strings.ToLower(v.Name)) {
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
			Versioned:        globalBucketVersioningSys.Enabled(bucket),
			VersionSuspended: globalBucketVersioningSys.Suspended(bucket),
			UserDefined:      meta,
		}
	}
	for k, v := range objInfo.UserDefined {
		meta[k] = v
	}
	if len(objInfo.UserTags) != 0 {
		meta[xhttp.AmzObjectTagging] = objInfo.UserTags
	}

	return ObjectOptions{
		Versioned:        globalBucketVersioningSys.Enabled(bucket),
		VersionSuspended: globalBucketVersioningSys.Suspended(bucket),
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
	if oi.TransitionStatus != lifecycle.TransitionComplete {
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

// completeRestoreObj constructs restoreObjStatus for a completed restore-object with given expiry.
func completedRestoreObj(expiry time.Time) restoreObjStatus {
	return restoreObjStatus{
		ongoing: false,
		expiry:  expiry.UTC(),
	}
}

// String returns x-amz-restore compatible representation of r.
func (r restoreObjStatus) String() string {
	if r.Ongoing() {
		return "ongoing-request=true"
	}
	return fmt.Sprintf("ongoing-request=false, expiry-date=%s", r.expiry.Format(http.TimeFormat))
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
	case "true":
		if len(tokens) == 1 {
			return ongoingRestoreObj(), nil
		}

	case "false":
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

		expiry, err := time.Parse(http.TimeFormat, expiryTokens[1])
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
