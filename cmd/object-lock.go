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
	"encoding/xml"
	"fmt"
	"io"
	"sync"
	"time"
)

// RetentionMode - object retention mode.
type RetentionMode string

const (
	// Governance - governance mode.
	Governance RetentionMode = "GOVERNANCE"

	// Compliance - compliance mode.
	Compliance RetentionMode = "COMPLIANCE"
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
	return globalWORMEnabled || created.Add(r.Validity).After(time.Now())
}

// BucketRetentionConfig - map of bucket and retention configuration.
type BucketRetentionConfig struct {
	sync.RWMutex
	retentionMap map[string]Retention
}

// Set - set retention configuration.
func (config *BucketRetentionConfig) Set(bucketName string, retention Retention) {
	config.Lock()
	config.retentionMap[bucketName] = retention
	config.Unlock()
}

// Get - Get retention configuration.
func (config *BucketRetentionConfig) Get(bucketName string) (r Retention, ok bool) {
	config.RLock()
	defer config.RUnlock()

	r, ok = config.retentionMap[bucketName]
	return r, ok
}

// Delete - delete retention configuration.
func (config *BucketRetentionConfig) Delete(bucketName string) {
	config.Lock()
	delete(config.retentionMap, bucketName)
	config.Unlock()
}

func newBucketRetentionConfig() *BucketRetentionConfig {
	return &BucketRetentionConfig{
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
			return fmt.Errorf("Days should not be zero")
		}
	} else if *retention.Years == 0 {
		return fmt.Errorf("Years should not be zero")
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
		utcNow := time.Now().UTC()
		if config.Rule.DefaultRetention.Days != nil {
			r.Validity = utcNow.AddDate(0, 0, int(*config.Rule.DefaultRetention.Days)).Sub(utcNow)
		} else {
			r.Validity = utcNow.AddDate(int(*config.Rule.DefaultRetention.Years), 0, 0).Sub(utcNow)
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
