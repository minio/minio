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

package lock

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/beevik/ntp"
	"github.com/minio/minio/internal/amztime"
	xhttp "github.com/minio/minio/internal/http"

	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/env"
)

const (
	logSubsys = "locking"
)

func lockLogIf(ctx context.Context, err error) {
	logger.LogIf(ctx, logSubsys, err)
}

// Enabled indicates object locking is enabled
const Enabled = "Enabled"

// RetMode - object retention mode.
type RetMode string

const (
	// RetGovernance - governance mode.
	RetGovernance RetMode = "GOVERNANCE"

	// RetCompliance - compliance mode.
	RetCompliance RetMode = "COMPLIANCE"
)

// Valid - returns if retention mode is valid
func (r RetMode) Valid() bool {
	switch r {
	case RetGovernance, RetCompliance:
		return true
	}
	return false
}

func parseRetMode(modeStr string) (mode RetMode) {
	switch strings.ToUpper(modeStr) {
	case "GOVERNANCE":
		mode = RetGovernance
	case "COMPLIANCE":
		mode = RetCompliance
	}
	return mode
}

// LegalHoldStatus - object legal hold status.
type LegalHoldStatus string

const (
	// LegalHoldOn - legal hold is on.
	LegalHoldOn LegalHoldStatus = "ON"

	// LegalHoldOff - legal hold is off.
	LegalHoldOff LegalHoldStatus = "OFF"
)

// Valid - returns true if legal hold status has valid values
func (l LegalHoldStatus) Valid() bool {
	switch l {
	case LegalHoldOn, LegalHoldOff:
		return true
	}
	return false
}

func parseLegalHoldStatus(holdStr string) (st LegalHoldStatus) {
	switch strings.ToUpper(holdStr) {
	case "ON":
		st = LegalHoldOn
	case "OFF":
		st = LegalHoldOff
	}
	return st
}

// Bypass retention governance header.
const (
	AmzObjectLockBypassRetGovernance = "X-Amz-Bypass-Governance-Retention"
	AmzObjectLockRetainUntilDate     = "X-Amz-Object-Lock-Retain-Until-Date"
	AmzObjectLockMode                = "X-Amz-Object-Lock-Mode"
	AmzObjectLockLegalHold           = "X-Amz-Object-Lock-Legal-Hold"
)

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

var ntpServer = env.Get(ntpServerEnv, "")

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
	Mode        RetMode
	Validity    time.Duration
	LockEnabled bool
}

// Retain - check whether given date is retainable by validity time.
func (r Retention) Retain(created time.Time) bool {
	t, err := UTCNowNTP()
	if err != nil {
		lockLogIf(context.Background(), err)
		// Retain
		return true
	}
	return created.Add(r.Validity).After(t)
}

// DefaultRetention - default retention configuration.
type DefaultRetention struct {
	XMLName xml.Name `xml:"DefaultRetention"`
	Mode    RetMode  `xml:"Mode"`
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

	switch retention.Mode {
	case RetGovernance, RetCompliance:
	default:
		return fmt.Errorf("unknown retention mode %v", retention.Mode)
	}

	if retention.Days == nil && retention.Years == nil {
		return fmt.Errorf("either Days or Years must be specified")
	}

	if retention.Days != nil && retention.Years != nil {
		return fmt.Errorf("either Days or Years must be specified, not both")
	}

	//nolint:gocritic
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

// String returns the human readable format of object lock configuration, used in audit logs.
func (config Config) String() string {
	parts := []string{
		fmt.Sprintf("Enabled: %v", config.Enabled()),
	}
	if config.Rule != nil {
		if config.Rule.DefaultRetention.Mode != "" {
			parts = append(parts, fmt.Sprintf("Mode: %s", config.Rule.DefaultRetention.Mode))
		}
		if config.Rule.DefaultRetention.Days != nil {
			parts = append(parts, fmt.Sprintf("Days: %d", *config.Rule.DefaultRetention.Days))
		}
		if config.Rule.DefaultRetention.Years != nil {
			parts = append(parts, fmt.Sprintf("Years: %d", *config.Rule.DefaultRetention.Years))
		}
	}
	return strings.Join(parts, ", ")
}

// Enabled returns true if config.ObjectLockEnabled is set to Enabled
func (config *Config) Enabled() bool {
	return config.ObjectLockEnabled == Enabled
}

// UnmarshalXML - decodes XML data.
func (config *Config) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type objectLockConfig Config
	parsedConfig := objectLockConfig{}

	if err := d.DecodeElement(&parsedConfig, &start); err != nil {
		return err
	}

	if parsedConfig.ObjectLockEnabled != Enabled {
		return fmt.Errorf("only 'Enabled' value is allowed to ObjectLockEnabled element")
	}

	*config = Config(parsedConfig)
	return nil
}

// ToRetention - convert to Retention type.
func (config *Config) ToRetention() Retention {
	r := Retention{
		LockEnabled: config.ObjectLockEnabled == Enabled,
	}
	if config.Rule != nil {
		r.Mode = config.Rule.DefaultRetention.Mode

		t, err := UTCNowNTP()
		if err != nil {
			lockLogIf(context.Background(), err)
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

// Maximum 4KiB size per object lock config.
const maxObjectLockConfigSize = 1 << 12

// ParseObjectLockConfig parses ObjectLockConfig from xml
func ParseObjectLockConfig(reader io.Reader) (*Config, error) {
	config := Config{}
	if err := xml.NewDecoder(io.LimitReader(reader, maxObjectLockConfigSize)).Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// NewObjectLockConfig returns a initialized lock.Config struct
func NewObjectLockConfig() *Config {
	return &Config{
		ObjectLockEnabled: Enabled,
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
	retDate, err := amztime.ISO8601Parse(dateStr)
	if err != nil {
		return ErrInvalidRetentionDate
	}

	*rDate = RetentionDate{retDate}
	return nil
}

// MarshalXML encodes expiration date if it is non-zero and encodes
// empty string otherwise
func (rDate RetentionDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if rDate.IsZero() {
		return nil
	}
	return e.EncodeElement(amztime.ISO8601Format(rDate.Time), startElement)
}

// ObjectRetention specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html
type ObjectRetention struct {
	XMLNS           string        `xml:"xmlns,attr,omitempty"`
	XMLName         xml.Name      `xml:"Retention"`
	Mode            RetMode       `xml:"Mode,omitempty"`
	RetainUntilDate RetentionDate `xml:"RetainUntilDate,omitempty"`
}

func (o ObjectRetention) String() string {
	return fmt.Sprintf("Mode: %s, RetainUntilDate: %s", o.Mode, o.RetainUntilDate.Time)
}

// Maximum 4KiB size per object retention config.
const maxObjectRetentionSize = 1 << 12

// ParseObjectRetention constructs ObjectRetention struct from xml input
func ParseObjectRetention(reader io.Reader) (*ObjectRetention, error) {
	ret := ObjectRetention{}
	if err := xml.NewDecoder(io.LimitReader(reader, maxObjectRetentionSize)).Decode(&ret); err != nil {
		return nil, err
	}
	if ret.Mode != "" && !ret.Mode.Valid() {
		return &ret, ErrUnknownWORMModeDirective
	}

	if ret.Mode.Valid() && ret.RetainUntilDate.IsZero() {
		return &ret, ErrMalformedXML
	}

	if !ret.Mode.Valid() && !ret.RetainUntilDate.IsZero() {
		return &ret, ErrMalformedXML
	}

	t, err := UTCNowNTP()
	if err != nil {
		lockLogIf(context.Background(), err)
		return &ret, ErrPastObjectLockRetainDate
	}

	if !ret.RetainUntilDate.IsZero() && ret.RetainUntilDate.Before(t) {
		return &ret, ErrPastObjectLockRetainDate
	}

	return &ret, nil
}

// IsObjectLockRetentionRequested returns true if object lock retention headers are set.
func IsObjectLockRetentionRequested(h http.Header) bool {
	if _, ok := h[AmzObjectLockMode]; ok {
		return true
	}
	if _, ok := h[AmzObjectLockRetainUntilDate]; ok {
		return true
	}
	return false
}

// IsObjectLockLegalHoldRequested returns true if object lock legal hold header is set.
func IsObjectLockLegalHoldRequested(h http.Header) bool {
	_, ok := h[AmzObjectLockLegalHold]
	return ok
}

// IsObjectLockGovernanceBypassSet returns true if object lock governance bypass header is set.
func IsObjectLockGovernanceBypassSet(h http.Header) bool {
	return strings.EqualFold(h.Get(AmzObjectLockBypassRetGovernance), "true")
}

// IsObjectLockRequested returns true if legal hold or object lock retention headers are requested.
func IsObjectLockRequested(h http.Header) bool {
	return IsObjectLockLegalHoldRequested(h) || IsObjectLockRetentionRequested(h)
}

// ParseObjectLockRetentionHeaders parses http headers to extract retention mode and retention date
func ParseObjectLockRetentionHeaders(h http.Header) (rmode RetMode, r RetentionDate, err error) {
	retMode := h.Get(AmzObjectLockMode)
	dateStr := h.Get(AmzObjectLockRetainUntilDate)
	if len(retMode) == 0 || len(dateStr) == 0 {
		return rmode, r, ErrObjectLockInvalidHeaders
	}

	rmode = parseRetMode(retMode)
	if !rmode.Valid() {
		return rmode, r, ErrUnknownWORMModeDirective
	}

	var retDate time.Time
	// While AWS documentation mentions that the date specified
	// must be present in ISO 8601 format, in reality they allow
	// users to provide RFC 3339 compliant dates.
	retDate, err = amztime.ISO8601Parse(dateStr)
	if err != nil {
		return rmode, r, ErrInvalidRetentionDate
	}
	_, replReq := h[textproto.CanonicalMIMEHeaderKey(xhttp.MinIOSourceReplicationRequest)]

	t, err := UTCNowNTP()
	if err != nil {
		lockLogIf(context.Background(), err)
		return rmode, r, ErrPastObjectLockRetainDate
	}

	if retDate.Before(t) && !replReq {
		return rmode, r, ErrPastObjectLockRetainDate
	}

	return rmode, RetentionDate{retDate}, nil
}

// GetObjectRetentionMeta constructs ObjectRetention from metadata
func GetObjectRetentionMeta(meta map[string]string) ObjectRetention {
	var mode RetMode
	var retainTill RetentionDate

	var modeStr, tillStr string
	ok := false

	modeStr, ok = meta[strings.ToLower(AmzObjectLockMode)]
	if !ok {
		modeStr, ok = meta[AmzObjectLockMode]
	}
	if ok {
		mode = parseRetMode(modeStr)
	} else {
		return ObjectRetention{}
	}

	tillStr, ok = meta[strings.ToLower(AmzObjectLockRetainUntilDate)]
	if !ok {
		tillStr, ok = meta[AmzObjectLockRetainUntilDate]
	}
	if ok {
		if t, e := amztime.ISO8601Parse(tillStr); e == nil {
			retainTill = RetentionDate{t.UTC()}
		}
	}
	return ObjectRetention{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/", Mode: mode, RetainUntilDate: retainTill}
}

// GetObjectLegalHoldMeta constructs ObjectLegalHold from metadata
func GetObjectLegalHoldMeta(meta map[string]string) ObjectLegalHold {
	holdStr, ok := meta[strings.ToLower(AmzObjectLockLegalHold)]
	if !ok {
		holdStr, ok = meta[AmzObjectLockLegalHold]
	}
	if ok {
		return ObjectLegalHold{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/", Status: parseLegalHoldStatus(holdStr)}
	}
	return ObjectLegalHold{}
}

// ParseObjectLockLegalHoldHeaders parses request headers to construct ObjectLegalHold
func ParseObjectLockLegalHoldHeaders(h http.Header) (lhold ObjectLegalHold, err error) {
	holdStatus, ok := h[AmzObjectLockLegalHold]
	if ok {
		lh := parseLegalHoldStatus(holdStatus[0])
		if !lh.Valid() {
			return lhold, ErrUnknownWORMModeDirective
		}
		lhold = ObjectLegalHold{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/", Status: lh}
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

// UnmarshalXML - decodes XML data.
func (l *ObjectLegalHold) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	switch start.Name.Local {
	case "LegalHold", "ObjectLockLegalHold":
	default:
		return xml.UnmarshalError(fmt.Sprintf("expected element type <LegalHold>/<ObjectLockLegalHold> but have <%s>",
			start.Name.Local))
	}
	for {
		// Read tokens from the XML document in a stream.
		t, err := d.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if se, ok := t.(xml.StartElement); ok {
			switch se.Name.Local {
			case "Status":
				var st LegalHoldStatus
				if err = d.DecodeElement(&st, &se); err != nil {
					return err
				}
				l.Status = st
			default:
				return xml.UnmarshalError(fmt.Sprintf("expected element type <Status> but have <%s>", se.Name.Local))
			}
		}
	}
	return nil
}

// IsEmpty returns true if struct is empty
func (l *ObjectLegalHold) IsEmpty() bool {
	return !l.Status.Valid()
}

// ParseObjectLegalHold decodes the XML into ObjectLegalHold
func ParseObjectLegalHold(reader io.Reader) (hold *ObjectLegalHold, err error) {
	buf, err := io.ReadAll(io.LimitReader(reader, maxObjectLockConfigSize))
	if err != nil {
		return nil, err
	}

	hold = &ObjectLegalHold{}
	if err = xml.NewDecoder(bytes.NewReader(buf)).Decode(hold); err != nil {
		return nil, err
	}

	if !hold.Status.Valid() {
		return nil, ErrMalformedXML
	}
	return hold, err
}

// FilterObjectLockMetadata filters object lock metadata if s3:GetObjectRetention permission is denied or if isCopy flag set.
func FilterObjectLockMetadata(metadata map[string]string, filterRetention, filterLegalHold bool) map[string]string {
	// Copy on write
	dst := metadata
	var copied bool
	delKey := func(key string) {
		key = strings.ToLower(key)
		if _, ok := metadata[key]; !ok {
			return
		}
		if !copied {
			dst = make(map[string]string, len(metadata))
			maps.Copy(dst, metadata)
			copied = true
		}
		delete(dst, key)
	}
	legalHold := GetObjectLegalHoldMeta(metadata)
	if !legalHold.Status.Valid() || filterLegalHold {
		delKey(AmzObjectLockLegalHold)
	}

	ret := GetObjectRetentionMeta(metadata)
	if !ret.Mode.Valid() || filterRetention {
		delKey(AmzObjectLockMode)
		delKey(AmzObjectLockRetainUntilDate)
		return dst
	}
	return dst
}
