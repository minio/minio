/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7/pkg/tags"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	sse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/s3select"
)

const (
	// Disabled means the lifecycle rule is inactive
	Disabled = "Disabled"
	// TransitionStatus status of transition
	TransitionStatus = "transition-status"
	// TransitionedObjectName name of transitioned object
	TransitionedObjectName = "transitioned-object"
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

type transitionState struct {
	// add future metrics here
	transitionCh chan ObjectInfo
}

func (t *transitionState) queueTransitionTask(oi ObjectInfo) {
	select {
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
	ts := &transitionState{
		transitionCh: make(chan ObjectInfo, 10000),
	}
	go func() {
		<-GlobalContext.Done()
		close(ts.transitionCh)
	}()
	return ts
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

func validateLifecycleTransition(ctx context.Context, bucket string, lfc *lifecycle.Lifecycle) error {
	for _, rule := range lfc.Rules {
		if rule.Transition.StorageClass != "" {
			err := validateTransitionDestination(rule.Transition.StorageClass)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// validateTransitionDestination returns error if transition destination bucket missing or not configured
// It also returns true if transition destination is same as this server.
func validateTransitionDestination(sc string) error {
	client, err := globalTierConfigMgr.GetDriver(sc)
	if err != nil {
		return err
	}
	_, err = client.Get(context.Background(), "probeobject", warmBackendGetOpts{})
	if isErrBucketNotFound(err) || !isErrObjectNotFound(err) {
		return err
	}
	//!!
	//TODO: validate if bucket still present on the target and if target endpoint + bucket happens to be this bucket we are creating
	// lfc config for.
	// sameTarget, _ := isLocalHost(clnt.EndpointURL().Hostname(), clnt.EndpointURL().Port(), globalMinioPort)
	// return sameTarget, arn.Bucket, nil
	return nil
}

// handle deletes of transitioned objects or object versions when one of the following is true:
// 1. temporarily restored copies of objects (restored with the PostRestoreObject API) expired.
// 2. life cycle expiry date is met on the object.
// 3. Object is removed through DELETE api call
func deleteTransitionedObject(ctx context.Context, objectAPI ObjectLayer, bucket, object string, lcOpts lifecycle.ObjectOpts, action lifecycle.Action, tgtObjName, transitionSC string, expiryEvent bool) error {
	lc, err := globalLifecycleSys.Get(bucket)
	if err != nil {
		return err
	}

	tierName := getLifeCycleTransitionTier(ctx, lc, bucket, lcOpts)
	tgtClient, err := globalTierConfigMgr.GetDriver(tierName)
	if err != nil {
		return err
	}

	var opts ObjectOptions
	opts.Versioned = globalBucketVersioningSys.Enabled(bucket)
	opts.VersionID = lcOpts.VersionID
	switch action {
	case lifecycle.DeleteRestoredAction, lifecycle.DeleteRestoredVersionAction:
		// delete locally restored copy of object or object version
		// from the source, while leaving metadata behind. The data on
		// transitioned tier lies untouched and still accessible
		opts.Transition.Status = lcOpts.TransitionStatus
		_, err = objectAPI.DeleteObject(ctx, bucket, object, opts)
		return err
	case lifecycle.DeleteAction, lifecycle.DeleteVersionAction:
		// When an objectglobalTierConfigMgr transitioned tier and
		// metadata from source
		if err := tgtClient.Remove(GlobalContext, tgtObjName); err != nil {
			logger.LogIf(ctx, err)
			return err
		}
		// Delete metadata on source, now that transition tier has been cleaned up.
		if _, err = objectAPI.DeleteObject(ctx, bucket, object, opts); err != nil {
			return err
		}

		if expiryEvent {
			eventName := event.ObjectRemovedDelete
			if lcOpts.DeleteMarker {
				eventName = event.ObjectRemovedDeleteMarkerCreated
			}
			objInfo := ObjectInfo{
				Name:         object,
				VersionID:    lcOpts.VersionID,
				DeleteMarker: lcOpts.DeleteMarker,
			}
			// Notify object deleted event.
			sendEvent(eventArgs{
				EventName:  eventName,
				BucketName: bucket,
				Object:     objInfo,
				Host:       "Internal: [ILM-EXPIRY]",
			})
		}
	}

	// should never reach here
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
	opts := ObjectOptions{Transition: TransitionOptions{
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
	tgtClient, err := globalTierConfigMgr.GetDriver(oi.TransitionTier)
	if err != nil {
		return nil, fmt.Errorf("transition storage class not configured")
	}
	fn, off, length, err := NewGetObjectReader(rs, oi, opts)
	if err != nil {
		return nil, ErrorRespToObjectError(err, bucket, object)
	}
	gopts := warmBackendGetOpts{}

	// get correct offsets for object
	if off >= 0 && length >= 0 {
		gopts.startOffset = off
		gopts.length = length
	}

	reader, err := tgtClient.Get(ctx, oi.transitionedObjName, gopts)
	if err != nil {
		return nil, err
	}
	closeReader := func() { reader.Close() }

	return fn(reader, h, opts.CheckPrecondFn, closeReader)
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
	return sp == nil || sp.S3Select == s3select.S3Select{}
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
		meta[xhttp.AmzObjectTagging] = rreq.OutputLocation.S3.Tagging.String()
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
	meta[xhttp.AmzObjectTagging] = objInfo.UserTags

	return ObjectOptions{
		Versioned:        globalBucketVersioningSys.Enabled(bucket),
		VersionSuspended: globalBucketVersioningSys.Suspended(bucket),
		UserDefined:      meta,
		VersionID:        objInfo.VersionID,
		MTime:            objInfo.ModTime,
		Expires:          objInfo.Expires,
	}
}

var (
	errRestoreHDRMissing   = fmt.Errorf("x-amz-restore header not found")
	errRestoreHDRMalformed = fmt.Errorf("x-amz-restore header malformed")
)

// parse x-amz-restore header from user metadata to get the status of ongoing request and expiry of restoration
// if any. This header value is of format: ongoing-request=true|false, expires=time
func parseRestoreHeaderFromMeta(meta map[string]string) (ongoing bool, expiry time.Time, err error) {
	restoreHdr, ok := meta[xhttp.AmzRestore]
	if !ok {
		return ongoing, expiry, errRestoreHDRMissing
	}
	rslc := strings.SplitN(restoreHdr, ",", 2)
	if len(rslc) != 2 {
		return ongoing, expiry, errRestoreHDRMalformed
	}
	rstatusSlc := strings.SplitN(rslc[0], "=", 2)
	if len(rstatusSlc) != 2 {
		return ongoing, expiry, errRestoreHDRMalformed
	}
	rExpSlc := strings.SplitN(rslc[1], "=", 2)
	if len(rExpSlc) != 2 {
		return ongoing, expiry, errRestoreHDRMalformed
	}

	expiry, err = time.Parse(http.TimeFormat, rExpSlc[1])
	if err != nil {
		return
	}
	return rstatusSlc[1] == "true", expiry, nil
}
