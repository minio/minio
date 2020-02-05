/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package lock

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

	"github.com/beevik/ntp"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
)

// Mode - object retention mode.
type Mode string

const (
	// Governance - governance mode.
	Governance Mode = "GOVERNANCE"

	// Compliance - compliance mode.
	Compliance Mode = "COMPLIANCE"

	// Invalid - invalid retention mode.
	Invalid Mode = ""
)

func parseMode(modeStr string) (mode Mode) {
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

// LegalHoldStatus - object legal hold status.
type LegalHoldStatus string

const (
	// ON -legal hold is on.
	ON LegalHoldStatus = "ON"

	// OFF -legal hold is off.
	OFF LegalHoldStatus = "OFF"
)

func parseLegalHoldStatus(holdStr string) LegalHoldStatus {
	switch strings.ToUpper(holdStr) {
	case "ON":
		return ON
	case "OFF":
		return OFF
	}
	return LegalHoldStatus("")
}

var (
	// ErrMalformedBucketObjectConfig -indicates that the bucket object lock config is malformed
	ErrMalformedBucketObjectConfig = errors.New("invalid bucket object lock config")
	// ErrInvalidRetentionDate - indicates that retention date needs to be in ISO 8601 format
	ErrInvalidRetentionDate = errors.New("date must be provided in ISO 8601 format")
	// ErrPastObjectLockRetainDate - indicates that retention date must be in the future
	ErrPastObjectLockRetainDate = errors.New("the retain until date must be in the future")
	// ErrUnknownWORMModeDirective - indicates that the retention mode is invalid
	ErrUnknownWORMModeDirective = errors.New("unknown WORM mode directive")
	// ErrObjectLockMissingContentMD5 - indicates missing Content-MD5 header for put object requests with locking
	ErrObjectLockMissingContentMD5 = errors.New("content-MD5 HTTP header is required for Put Object requests with Object Lock parameters")
	// ErrObjectLockInvalidHeaders indicates that object lock headers are missing
	ErrObjectLockInvalidHeaders = errors.New("x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied")
	// ErrMalformedXML - generic error indicating malformed XML
	ErrMalformedXML = errors.New("the XML you provided was not well-formed or did not validate against our published schema")
)

const (
	ntpServerEnv = "MINIO_NTP_SERVER"
)

var (
	ntpServer = env.Get(ntpServerEnv, "")
)

// UTCNowNTP - is similar in functionality to UTCNow()
// but only used when we do not wish to rely on system
// time.
func UTCNowNTP() (time.Time, error) {
	// ntp server is disabled
	if ntpServer == "" {
		return time.Now().UTC(), nil
	}
	return ntp.Time(ntpServer)
}

// Retention - bucket level retention configuration.
type Retention struct {
	Mode     Mode
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
	return created.Add(r.Validity).After(t)
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

// NewBucketObjectLockConfig returns initialized BucketObjectLockConfig
func NewBucketObjectLockConfig() *BucketObjectLockConfig {
	return &BucketObjectLockConfig{
		retentionMap: map[string]Retention{},
	}
}

// DefaultRetention - default retention configuration.
type DefaultRetention struct {
	XMLName xml.Name `xml:"DefaultRetention"`
	Mode    Mode     `xml:"Mode"`
	Days    *uint64  `xml:"Days"`
	Years   *uint64  `xml:"Years"`
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

// Config - object lock configuration specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/Type_API_ObjectLockConfiguration.html
type Config struct {
	XMLNS             string   `xml:"xmlns,attr,omitempty"`
	XMLName           xml.Name `xml:"ObjectLockConfiguration"`
	ObjectLockEnabled string   `xml:"ObjectLockEnabled"`
	Rule              *struct {
		DefaultRetention DefaultRetention `xml:"DefaultRetention"`
	} `xml:"Rule,omitempty"`
}

// UnmarshalXML - decodes XML data.
func (config *Config) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type objectLockConfig Config
	parsedConfig := objectLockConfig{}

	if err := d.DecodeElement(&parsedConfig, &start); err != nil {
		return err
	}

	if parsedConfig.ObjectLockEnabled != "Enabled" {
		return fmt.Errorf("only 'Enabled' value is allowd to ObjectLockEnabled element")
	}

	*config = Config(parsedConfig)
	return nil
}

// ToRetention - convert to Retention type.
func (config *Config) ToRetention() (r Retention) {
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

// ParseObjectLockConfig parses ObjectLockConfig from xml
func ParseObjectLockConfig(reader io.Reader) (*Config, error) {
	config := Config{}
	if err := xml.NewDecoder(reader).Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// NewObjectLockConfig returns a initialized lock.Config struct
func NewObjectLockConfig() *Config {
	return &Config{
		ObjectLockEnabled: "Enabled",
	}
}

// RetentionDate is a embedded type containing time.Time to unmarshal
// Date in Retention
type RetentionDate struct {
	time.Time
}

// UnmarshalXML parses date from Retention and validates date format
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
		return ErrInvalidRetentionDate
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
	Mode            Mode          `xml:"Mode,omitempty"`
	RetainUntilDate RetentionDate `xml:"RetainUntilDate,omitempty"`
}

// ParseObjectRetention constructs ObjectRetention struct from xml input
func ParseObjectRetention(reader io.Reader) (*ObjectRetention, error) {
	ret := ObjectRetention{}
	if err := xml.NewDecoder(reader).Decode(&ret); err != nil {
		return nil, err
	}
	if ret.Mode != Compliance && ret.Mode != Governance {
		return &ret, ErrUnknownWORMModeDirective
	}

	t, err := UTCNowNTP()
	if err != nil {
		logger.LogIf(context.Background(), err)
		return &ret, ErrPastObjectLockRetainDate
	}

	if ret.RetainUntilDate.Before(t) {
		return &ret, ErrPastObjectLockRetainDate
	}

	return &ret, nil
}

// IsObjectLockRetentionRequested returns true if object lock retention headers are set.
func IsObjectLockRetentionRequested(h http.Header) bool {
	if _, ok := h[xhttp.AmzObjectLockMode]; ok {
		return true
	}
	if _, ok := h[xhttp.AmzObjectLockRetainUntilDate]; ok {
		return true
	}
	return false
}

// IsObjectLockLegalHoldRequested returns true if object lock legal hold header is set.
func IsObjectLockLegalHoldRequested(h http.Header) bool {
	_, ok := h[xhttp.AmzObjectLockLegalHold]
	return ok
}

// IsObjectLockGovernanceBypassSet returns true if object lock governance bypass header is set.
func IsObjectLockGovernanceBypassSet(h http.Header) bool {
	return strings.ToLower(h.Get(xhttp.AmzObjectLockBypassGovernance)) == "true"
}

// IsObjectLockRequested returns true if legal hold or object lock retention headers are requested.
func IsObjectLockRequested(h http.Header) bool {
	return IsObjectLockLegalHoldRequested(h) || IsObjectLockRetentionRequested(h)
}

// ParseObjectLockRetentionHeaders parses http headers to extract retention mode and retention date
func ParseObjectLockRetentionHeaders(h http.Header) (rmode Mode, r RetentionDate, err error) {
	retMode := h.Get(xhttp.AmzObjectLockMode)
	dateStr := h.Get(xhttp.AmzObjectLockRetainUntilDate)
	if len(retMode) == 0 || len(dateStr) == 0 {
		return rmode, r, ErrObjectLockInvalidHeaders
	}
	rmode = parseMode(retMode)
	if rmode == Invalid {
		return rmode, r, ErrUnknownWORMModeDirective
	}

	var retDate time.Time
	// While AWS documentation mentions that the date specified
	// must be present in ISO 8601 format, in reality they allow
	// users to provide RFC 3339 compliant dates.
	retDate, err = time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return rmode, r, ErrInvalidRetentionDate
	}

	t, err := UTCNowNTP()
	if err != nil {
		logger.LogIf(context.Background(), err)
		return rmode, r, ErrPastObjectLockRetainDate
	}

	if retDate.Before(t) {
		return rmode, r, ErrPastObjectLockRetainDate
	}

	return rmode, RetentionDate{retDate}, nil

}

// GetObjectRetentionMeta constructs ObjectRetention from metadata
func GetObjectRetentionMeta(meta map[string]string) ObjectRetention {
	var mode Mode
	var retainTill RetentionDate

	if modeStr, ok := meta[strings.ToLower(xhttp.AmzObjectLockMode)]; ok {
		mode = parseMode(modeStr)
	}
	if tillStr, ok := meta[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)]; ok {
		if t, e := time.Parse(time.RFC3339, tillStr); e == nil {
			retainTill = RetentionDate{t.UTC()}
		}
	}
	return ObjectRetention{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/", Mode: mode, RetainUntilDate: retainTill}
}

// GetObjectLegalHoldMeta constructs ObjectLegalHold from metadata
func GetObjectLegalHoldMeta(meta map[string]string) ObjectLegalHold {

	holdStr, ok := meta[strings.ToLower(xhttp.AmzObjectLockLegalHold)]
	if ok {
		return ObjectLegalHold{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/", Status: parseLegalHoldStatus(holdStr)}
	}
	return ObjectLegalHold{}
}

// ParseObjectLockLegalHoldHeaders parses request headers to construct ObjectLegalHold
func ParseObjectLockLegalHoldHeaders(h http.Header) (lhold ObjectLegalHold, err error) {
	holdStatus, ok := h[xhttp.AmzObjectLockLegalHold]
	if ok {
		lh := parseLegalHoldStatus(strings.Join(holdStatus, ""))
		if lh != ON && lh != OFF {
			return lhold, ErrUnknownWORMModeDirective
		}
		lhold = ObjectLegalHold{Status: lh}
	}
	return lhold, nil

}

// ObjectLegalHold specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html
type ObjectLegalHold struct {
	XMLNS   string          `xml:"xmlns,attr,omitempty"`
	XMLName xml.Name        `xml:"LegalHold"`
	Status  LegalHoldStatus `xml:"Status,omitempty"`
}

// ParseObjectLegalHold decodes the XML into ObjectLegalHold
func ParseObjectLegalHold(reader io.Reader) (hold *ObjectLegalHold, err error) {
	if err = xml.NewDecoder(reader).Decode(&hold); err != nil {
		return
	}

	if hold.Status != ON && hold.Status != OFF {
		return nil, ErrMalformedXML
	}
	if hold.XMLNS == "" {
		hold.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
	}
	return
}

// FilterObjectLockMetadata filters object lock metadata if s3:GetObjectRetention permission is denied or if isCopy flag set.
func FilterObjectLockMetadata(metadata map[string]string, filterRetention, filterLegalHold bool) map[string]string {
	// Copy on write
	dst := metadata
	var copied bool
	delKey := func(key string) {
		if _, ok := metadata[key]; !ok {
			return
		}
		if !copied {
			dst = make(map[string]string, len(metadata))
			for k, v := range metadata {
				dst[k] = v
			}
			copied = true
		}
		delete(dst, key)
	}
	legalHold := GetObjectLegalHoldMeta(metadata)
	if legalHold.Status == "" || filterLegalHold {
		delKey(xhttp.AmzObjectLockLegalHold)
	}

	ret := GetObjectRetentionMeta(metadata)

	if ret.Mode == Invalid || filterRetention {
		delKey(xhttp.AmzObjectLockMode)
		delKey(xhttp.AmzObjectLockRetainUntilDate)
		return dst
	}
	return dst
}
