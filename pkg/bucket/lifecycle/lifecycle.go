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

package lifecycle

import (
	"encoding/xml"
	"io"
	"strings"
	"time"
)

var (
	errLifecycleTooManyRules      = Errorf("Lifecycle configuration allows a maximum of 1000 rules")
	errLifecycleNoRule            = Errorf("Lifecycle configuration should have at least one rule")
	errLifecycleOverlappingPrefix = Errorf("Lifecycle configuration has rules with overlapping prefix")
)

// Action represents a delete action or other transition
// actions that will be implemented later.
type Action int

const (
	// NoneAction means no action required after evaluting lifecycle rules
	NoneAction Action = iota
	// DeleteAction means the object needs to be removed after evaluting lifecycle rules
	DeleteAction
)

func validateRules(rules []Rule) error {
	// Lifecycle config should have at least one rule
	if len(rules) == 0 {
		return errLifecycleNoRule
	}

	if len(rules) > 1000 {
		return errLifecycleTooManyRules
	}

	prefixes := []string{}
	for _, rule := range rules {
		prefix := rule.Prefix()
		for i := range prefixes {
			if prefix != prefixes[i] && (strings.HasPrefix(prefixes[i], prefix) || strings.HasPrefix(prefix, prefixes[i])) {
				return errLifecycleOverlappingPrefix
			}
		}
		prefixes = append(prefixes, prefix)
	}

	return nil
}

// Lifecycle - Configuration for bucket lifecycle.
type Lifecycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// getActions returns the expiration and transition from the object name
// after evaluating all rules.
func (lc Lifecycle) getActions(objName, objTags string) (*Expiration, *Transition) {
	if objName == "" {
		return nil, nil
	}

	for _, rule := range lc.Rules {
		if rule.Status == Disabled {
			continue
		}

		if !strings.HasPrefix(objName, rule.Prefix()) {
			continue
		}

		tags := rule.Tags()
		if tags == "" {
			return rule.Expiration, nil
		}

		if strings.Contains(objTags, tags) {
			return rule.Expiration, nil
		}
	}

	return nil, nil
}

// ComputeAction returns the action to perform by evaluating all lifecycle rules
// against the object name and its modification time.
func (lc Lifecycle) ComputeAction(objName, objTags string, modTime time.Time) Action {
	if modTime.IsZero() {
		return NoneAction
	}

	exp, _ := lc.getActions(objName, objTags)
	if exp == nil {
		return NoneAction
	}

	if exp.Date != nil {
		modTime = exp.Date.Time
	} else {
		modTime = modTime.Add(time.Duration(*exp.Days) * 24 * time.Hour)
	}

	if time.Now().After(modTime) {
		return DeleteAction
	}

	return NoneAction
}

// MarshalXML encodes to XML data.
func (lc Lifecycle) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := validateRules(lc.Rules); err != nil {
		return err
	}

	type subLifecycle Lifecycle // sub-type to avoid recursively called MarshalXML()
	start.Name.Local = "LifecycleConfiguration"
	return e.EncodeElement(subLifecycle(lc), start)
}

// UnmarshalXML decodes XML data.
func (lc *Lifecycle) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type subLifecycle Lifecycle // sub-type to avoid recursively called UnmarshalXML()
	var slc subLifecycle
	if err := d.DecodeElement(&slc, &start); err != nil {
		return err
	}

	if err := validateRules(slc.Rules); err != nil {
		return err
	}

	*lc = Lifecycle(slc)
	return nil
}

// ParseLifecycleConfig - parses data in given reader to Lifecycle.
func ParseLifecycleConfig(reader io.Reader) (*Lifecycle, error) {
	var config Lifecycle

	if err := xml.NewDecoder(reader).Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
