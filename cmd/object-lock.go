/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
)

// RetentionMode - object retention mode.
type RetentionMode string

const (
	// Governance - governance mode.
	Governance RetentionMode = "GOVERNANCE"

	// Compliance - compliance mode.
	Compliance RetentionMode = "COMPLIANCE"

	// Invalid - invalid retention mode.
	Invalid RetentionMode = ""
)

func parseRetentionMode(modeStr string) (mode RetentionMode) {
	switch strings.ToUpper(modeStr) {
	case "GOVERNANCE":
		mode = Governance
	case "COMPLIANCE":
		mode = Compliance
	default:
		mode = Invalid
	}
	return mode
}

var (
	errMalformedBucketObjectConfig = errors.New("Invalid bucket object lock config")
	errInvalidRetentionDate        = errors.New("Date must be provided in ISO 8601 format")
	errPastObjectLockRetainDate    = errors.New("the retain until date must be in the future")
	errUnknownWORMModeDirective    = errors.New("unknown WORM mode directive")
	errObjectLockMissingContentMD5 = errors.New("Content-MD5 HTTP header is required for Put Object requests with Object Lock parameters")
	errObjectLockInvalidHeaders    = errors.New("x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied")
)

const (
	ntpServerEnv = "MINIO_NTP_SERVER"
)

var (
	ntpServer = env.Get(ntpServerEnv, "")
)

// Retention - bucket level retention configuration.
type Retention struct {
	Mode     RetentionMode
	Validity time.Duration
}

// IsEmpty - returns whether retention is empty or not.
func (r Retention) IsEmpty() bool {
	return r.Mode == "" || r.Validity == 0
}

// Retain - check whether given date is retainable by validity time.
func (r Retention) Retain(created time.Time) bool {
	t, err := UTCNowNTP()
	if err != nil {
		logger.LogIf(context.Background(), err)
		// Retain
		return true
	}
	return globalWORMEnabled || created.Add(r.Validity).After(t)
}

// BucketObjectLockConfig - map of bucket and retention configuration.
type BucketObjectLockConfig struct {
	sync.RWMutex
	retentionMap map[string]Retention
}

// Set - set retention configuration.
func (config *BucketObjectLockConfig) Set(bucketName string, retention Retention) {
	config.Lock()
	config.retentionMap[bucketName] = retention
	config.Unlock()
}

// Get - Get retention configuration.
func (config *BucketObjectLockConfig) Get(bucketName string) (r Retention, ok bool) {
	config.RLock()
	defer config.RUnlock()
	r, ok = config.retentionMap[bucketName]
	return r, ok
}

// Remove - removes retention configuration.
func (config *BucketObjectLockConfig) Remove(bucketName string) {
	config.Lock()
	delete(config.retentionMap, bucketName)
	config.Unlock()
}

func newBucketObjectLockConfig() *BucketObjectLockConfig {
	return &BucketObjectLockConfig{
		retentionMap: map[string]Retention{},
	}
}

// DefaultRetention - default retention configuration.
type DefaultRetention struct {
	XMLName xml.Name      `xml:"DefaultRetention"`
	Mode    RetentionMode `xml:"Mode"`
	Days    *uint64       `xml:"Days"`
	Years   *uint64       `xml:"Years"`
}

// Maximum support retention days and years supported by AWS S3.
const (
	// This tested by using `mc lock` command
	maximumRetentionDays  = 36500
	maximumRetentionYears = 100
)

// UnmarshalXML - decodes XML data.
func (dr *DefaultRetention) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type defaultRetention DefaultRetention
	retention := defaultRetention{}

	if err := d.DecodeElement(&retention, &start); err != nil {
		return err
	}

	switch string(retention.Mode) {
	case "GOVERNANCE", "COMPLIANCE":
	default:
		return fmt.Errorf("unknown retention mode %v", retention.Mode)
	}

	if retention.Days == nil && retention.Years == nil {
		return fmt.Errorf("either Days or Years must be specified")
	}

	if retention.Days != nil && retention.Years != nil {
		return fmt.Errorf("either Days or Years must be specified, not both")
	}

	if retention.Days != nil {
		if *retention.Days == 0 {
			return fmt.Errorf("Default retention period must be a positive integer value for 'Days'")
		}
		if *retention.Days > maximumRetentionDays {
			return fmt.Errorf("Default retention period too large for 'Days' %d", *retention.Days)
		}
	} else if *retention.Years == 0 {
		return fmt.Errorf("Default retention period must be a positive integer value for 'Years'")
	} else if *retention.Years > maximumRetentionYears {
		return fmt.Errorf("Default retention period too large for 'Years' %d", *retention.Years)
	}

	*dr = DefaultRetention(retention)

	return nil
}

// ObjectLockConfig - object lock configuration specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/Type_API_ObjectLockConfiguration.html
type ObjectLockConfig struct {
	XMLNS             string   `xml:"xmlns,attr,omitempty"`
	XMLName           xml.Name `xml:"ObjectLockConfiguration"`
	ObjectLockEnabled string   `xml:"ObjectLockEnabled"`
	Rule              *struct {
		DefaultRetention DefaultRetention `xml:"DefaultRetention"`
	} `xml:"Rule,omitempty"`
}

// UnmarshalXML - decodes XML data.
func (config *ObjectLockConfig) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type objectLockConfig ObjectLockConfig
	parsedConfig := objectLockConfig{}

	if err := d.DecodeElement(&parsedConfig, &start); err != nil {
		return err
	}

	if parsedConfig.ObjectLockEnabled != "Enabled" {
		return fmt.Errorf("only 'Enabled' value is allowd to ObjectLockEnabled element")
	}

	*config = ObjectLockConfig(parsedConfig)
	return nil
}

// ToRetention - convert to Retention type.
func (config *ObjectLockConfig) ToRetention() (r Retention) {
	if config.Rule != nil {
		r.Mode = config.Rule.DefaultRetention.Mode

		t, err := UTCNowNTP()
		if err != nil {
			logger.LogIf(context.Background(), err)
			// Do not change any configuration
			// upon NTP failure.
			return r
		}

		if config.Rule.DefaultRetention.Days != nil {
			r.Validity = t.AddDate(0, 0, int(*config.Rule.DefaultRetention.Days)).Sub(t)
		} else {
			r.Validity = t.AddDate(int(*config.Rule.DefaultRetention.Years), 0, 0).Sub(t)
		}
	}

	return r
}

func parseObjectLockConfig(reader io.Reader) (*ObjectLockConfig, error) {
	config := ObjectLockConfig{}
	if err := xml.NewDecoder(reader).Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

func newObjectLockConfig() *ObjectLockConfig {
	return &ObjectLockConfig{
		ObjectLockEnabled: "Enabled",
	}
}

// RetentionDate is a embedded type containing time.Time to unmarshal
// Date in Retention
type RetentionDate struct {
	time.Time
}

// UnmarshalXML parses date from Expiration and validates date format
func (rDate *RetentionDate) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var dateStr string
	err := d.DecodeElement(&dateStr, &startElement)
	if err != nil {
		return err
	}
	// While AWS documentation mentions that the date specified
	// must be present in ISO 8601 format, in reality they allow
	// users to provide RFC 3339 compliant dates.
	retDate, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return errInvalidRetentionDate
	}

	*rDate = RetentionDate{retDate}
	return nil
}

// MarshalXML encodes expiration date if it is non-zero and encodes
// empty string otherwise
func (rDate *RetentionDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if *rDate == (RetentionDate{time.Time{}}) {
		return nil
	}
	return e.EncodeElement(rDate.Format(time.RFC3339), startElement)
}

// ObjectRetention specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html
type ObjectRetention struct {
	XMLNS           string        `xml:"xmlns,attr,omitempty"`
	XMLName         xml.Name      `xml:"Retention"`
	Mode            RetentionMode `xml:"Mode,omitempty"`
	RetainUntilDate RetentionDate `xml:"RetainUntilDate,omitempty"`
}

func parseObjectRetention(reader io.Reader) (*ObjectRetention, error) {
	ret := ObjectRetention{}
	if err := xml.NewDecoder(reader).Decode(&ret); err != nil {
		return nil, err
	}
	if ret.Mode != Compliance && ret.Mode != Governance {
		return &ret, errUnknownWORMModeDirective
	}

	t, err := UTCNowNTP()
	if err != nil {
		logger.LogIf(context.Background(), err)
		return &ret, errPastObjectLockRetainDate
	}

	if ret.RetainUntilDate.Before(t) {
		return &ret, errPastObjectLockRetainDate
	}

	return &ret, nil
}

func isObjectLockRetentionRequested(h http.Header) bool {
	if _, ok := h[xhttp.AmzObjectLockMode]; ok {
		return true
	}
	if _, ok := h[xhttp.AmzObjectLockRetainUntilDate]; ok {
		return true
	}
	return false
}

func isObjectLockLegalHoldRequested(h http.Header) bool {
	_, ok := h[xhttp.AmzObjectLockLegalHold]
	return ok
}

func isObjectLockGovernanceBypassSet(h http.Header) bool {
	v, ok := h[xhttp.AmzObjectLockBypassGovernance]
	if !ok {
		return false
	}
	val := strings.Join(v, "")
	return strings.ToLower(val) == "true"
}

func isObjectLockRequested(h http.Header) bool {
	return isObjectLockLegalHoldRequested(h) || isObjectLockRetentionRequested(h)
}

func parseObjectLockRetentionHeaders(h http.Header) (rmode RetentionMode, r RetentionDate, err error) {
	retMode, ok := h[xhttp.AmzObjectLockMode]
	if ok {
		rmode = parseRetentionMode(strings.Join(retMode, ""))
		if rmode == Invalid {
			return rmode, r, errUnknownWORMModeDirective
		}
	}
	var retDate time.Time
	dateStr, ok := h[xhttp.AmzObjectLockRetainUntilDate]
	if ok {
		// While AWS documentation mentions that the date specified
		// must be present in ISO 8601 format, in reality they allow
		// users to provide RFC 3339 compliant dates.
		retDate, err = time.Parse(time.RFC3339, strings.Join(dateStr, ""))
		if err != nil {
			return rmode, r, errInvalidRetentionDate
		}

		t, err := UTCNowNTP()
		if err != nil {
			logger.LogIf(context.Background(), err)
			return rmode, r, errPastObjectLockRetainDate
		}

		if retDate.Before(t) {
			return rmode, r, errPastObjectLockRetainDate
		}
	}
	if len(retMode) == 0 || len(dateStr) == 0 {
		return rmode, r, errObjectLockInvalidHeaders
	}
	return rmode, RetentionDate{retDate}, nil

}

func getObjectRetentionMeta(meta map[string]string) ObjectRetention {
	var mode RetentionMode
	var retainTill RetentionDate

	if modeStr, ok := meta[strings.ToLower(xhttp.AmzObjectLockMode)]; ok {
		mode = parseRetentionMode(modeStr)
	}
	if tillStr, ok := meta[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)]; ok {
		if t, e := time.Parse(time.RFC3339, tillStr); e == nil {
			retainTill = RetentionDate{t.UTC()}
		}
	}
	return ObjectRetention{Mode: mode, RetainUntilDate: retainTill}
}

// enforceRetentionBypassForDelete enforces whether an existing object under governance can be deleted
// with governance bypass headers set in the request.
// Objects under site wide WORM can never be overwritten.
// For objects in "Governance" mode, overwrite is allowed if a) object retention date is past OR
// governance bypass headers are set and user has governance bypass permissions.
// Objects in "Compliance" mode can be overwritten only if retention date is past.
func enforceRetentionBypassForDelete(ctx context.Context, r *http.Request, bucket, object string, getObjectInfoFn GetObjectInfoFn, govBypassPerm APIErrorCode) (oi ObjectInfo, s3Err APIErrorCode) {
	if globalWORMEnabled {
		return oi, ErrObjectLocked
	}
	var err error
	var opts ObjectOptions
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return oi, toAPIErrorCode(ctx, err)
	}
	oi, err = getObjectInfoFn(ctx, bucket, object, opts)
	if err != nil {
		// ignore case where object no longer exists
		if toAPIError(ctx, err).Code == "NoSuchKey" {
			oi.UserDefined = map[string]string{}
			return oi, ErrNone
		}
		return oi, toAPIErrorCode(ctx, err)
	}
	ret := getObjectRetentionMeta(oi.UserDefined)

	// Here bucket does not support object lock
	if ret.Mode == Invalid {
		return oi, ErrNone
	}
	if ret.Mode != Compliance && ret.Mode != Governance {
		return oi, ErrUnknownWORMModeDirective
	}
	t, err := UTCNowNTP()
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, ErrObjectLocked
	}
	if ret.RetainUntilDate.Before(t) {
		return oi, ErrNone
	}
	if isObjectLockGovernanceBypassSet(r.Header) && ret.Mode == Governance && govBypassPerm == ErrNone {
		return oi, ErrNone
	}
	return oi, ErrObjectLocked
}

// enforceRetentionBypassForPut enforces whether an existing object under governance can be overwritten
// with governance bypass headers set in the request.
// Objects under site wide WORM cannot be overwritten.
// For objects in "Governance" mode, overwrite is allowed if a) object retention date is past OR
// governance bypass headers are set and user has governance bypass permissions.
// Objects in compliance mode can be overwritten only if retention date is being extended. No mode change is permitted.
func enforceRetentionBypassForPut(ctx context.Context, r *http.Request, bucket, object string, getObjectInfoFn GetObjectInfoFn, govBypassPerm APIErrorCode, objRetention *ObjectRetention) (oi ObjectInfo, s3Err APIErrorCode) {
	if globalWORMEnabled {
		return oi, ErrObjectLocked
	}

	var err error
	var opts ObjectOptions
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return oi, toAPIErrorCode(ctx, err)
	}
	oi, err = getObjectInfoFn(ctx, bucket, object, opts)
	if err != nil {
		// ignore case where object no longer exists
		if toAPIError(ctx, err).Code == "NoSuchKey" {
			oi.UserDefined = map[string]string{}
			return oi, ErrNone
		}
		return oi, toAPIErrorCode(ctx, err)
	}

	ret := getObjectRetentionMeta(oi.UserDefined)
	// no retention metadata on object
	if ret.Mode == Invalid {
		_, isWORMBucket := isWORMEnabled(bucket)
		if !isWORMBucket {
			return oi, ErrInvalidBucketObjectLockConfiguration
		}
		return oi, ErrNone
	}
	t, err := UTCNowNTP()
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, ErrObjectLocked
	}

	if ret.Mode == Compliance {
		// Compliance retention mode cannot be changed and retention period cannot be shortened as per
		// https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html#object-lock-retention-modes
		if objRetention.Mode != Compliance || objRetention.RetainUntilDate.Before(ret.RetainUntilDate.Time) {
			return oi, ErrObjectLocked
		}
		if objRetention.RetainUntilDate.Before(t) {
			return oi, ErrInvalidRetentionDate
		}
		return oi, ErrNone
	}

	if ret.Mode == Governance {
		if !isObjectLockGovernanceBypassSet(r.Header) {
			if objRetention.RetainUntilDate.Before(t) {
				return oi, ErrInvalidRetentionDate
			}
			if objRetention.RetainUntilDate.Before((ret.RetainUntilDate.Time)) {
				return oi, ErrObjectLocked
			}
			return oi, ErrNone
		}
		return oi, govBypassPerm
	}
	return oi, ErrNone
}

// checkPutObjectRetentionAllowed enforces object retention policy for requests with WORM headers
// See https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-managing.html for the spec.
// For non-existing objects with object retention headers set, this method returns ErrNone if bucket has
// locking enabled and user has requisite permissions (s3:PutObjectRetention)
// If object exists on object store and site wide WORM enabled - this method
// returns an error. For objects in "Governance" mode, overwrite is allowed if the retention date has expired.
// For objects in "Compliance" mode, retention date cannot be shortened, and mode cannot be altered.
func checkPutObjectRetentionAllowed(ctx context.Context, r *http.Request, bucket, object string, getObjectInfoFn GetObjectInfoFn, retentionPermErr APIErrorCode) (RetentionMode, RetentionDate, APIErrorCode) {
	var mode RetentionMode
	var retainDate RetentionDate

	retention, isWORMBucket := isWORMEnabled(bucket)

	retentionRequested := isObjectLockRequested(r.Header)

	var objExists bool
	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		return mode, retainDate, toAPIErrorCode(ctx, err)
	}
	if objInfo, err := getObjectInfoFn(ctx, bucket, object, opts); err == nil {
		objExists = true
		r := getObjectRetentionMeta(objInfo.UserDefined)
		if globalWORMEnabled || r.Mode == Compliance {
			return mode, retainDate, ErrObjectLocked
		}
		mode = r.Mode
		retainDate = r.RetainUntilDate
	}
	if retentionRequested {
		if !isWORMBucket {
			return mode, retainDate, ErrInvalidBucketObjectLockConfiguration
		}
		rMode, rDate, err := parseObjectLockRetentionHeaders(r.Header)
		if err != nil {
			return mode, retainDate, toAPIErrorCode(ctx, err)
		}
		// AWS S3 just creates a new version of object when an object is being overwritten.
		t, err := UTCNowNTP()
		if err != nil {
			logger.LogIf(ctx, err)
			return mode, retainDate, ErrObjectLocked
		}
		if objExists && retainDate.After(t) {
			return mode, retainDate, ErrObjectLocked
		}
		if rMode == Invalid {
			return mode, retainDate, toAPIErrorCode(ctx, errObjectLockInvalidHeaders)
		}
		if retentionPermErr != ErrNone {
			return mode, retainDate, retentionPermErr
		}
		return rMode, rDate, ErrNone
	}

	if !retentionRequested && isWORMBucket {
		if retention.IsEmpty() && (mode == Compliance || mode == Governance) {
			return mode, retainDate, ErrObjectLocked
		}
		if retentionPermErr != ErrNone {
			return mode, retainDate, retentionPermErr
		}
		t, err := UTCNowNTP()
		if err != nil {
			logger.LogIf(ctx, err)
			return mode, retainDate, ErrObjectLocked
		}
		// AWS S3 just creates a new version of object when an object is being overwritten.
		if objExists && retainDate.After(t) {
			return mode, retainDate, ErrObjectLocked
		}
		// inherit retention from bucket configuration
		return retention.Mode, RetentionDate{t.Add(retention.Validity)}, ErrNone
	}
	return mode, retainDate, ErrNone
}

// filter object lock metadata if s3:GetObjectRetention permission is denied or if isCopy flag set.
func filterObjectLockMetadata(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string, isCopy bool, getRetPerms APIErrorCode) map[string]string {
	ret := getObjectRetentionMeta(metadata)
	if ret.Mode == Invalid || isCopy {
		delete(metadata, xhttp.AmzObjectLockMode)
		delete(metadata, xhttp.AmzObjectLockRetainUntilDate)
		return metadata
	}
	if getRetPerms == ErrNone {
		return metadata
	}
	delete(metadata, xhttp.AmzObjectLockMode)
	delete(metadata, xhttp.AmzObjectLockRetainUntilDate)
	return metadata
}
