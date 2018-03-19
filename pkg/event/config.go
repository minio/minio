/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package event

import (
	"encoding/xml"
	"errors"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/minio/minio-go/pkg/set"
)

// ValidateFilterRuleValue - checks if given value is filter rule value or not.
func ValidateFilterRuleValue(value string) error {
	for _, segment := range strings.Split(value, "/") {
		if segment == "." || segment == ".." {
			return &ErrInvalidFilterValue{value}
		}
	}

	if len(value) <= 1024 && utf8.ValidString(value) && !strings.Contains(value, `\`) {
		return nil
	}

	return &ErrInvalidFilterValue{value}
}

// FilterRule - represents elements inside <FilterRule>...</FilterRule>
type FilterRule struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

// UnmarshalXML - decodes XML data.
func (filter *FilterRule) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type filterRule FilterRule
	rule := filterRule{}
	if err := d.DecodeElement(&rule, &start); err != nil {
		return err
	}

	if rule.Name != "prefix" && rule.Name != "suffix" {
		return &ErrInvalidFilterName{rule.Name}
	}

	if err := ValidateFilterRuleValue(filter.Value); err != nil {
		return err
	}

	*filter = FilterRule(rule)

	return nil
}

// FilterRuleList - represents multiple <FilterRule>...</FilterRule>
type FilterRuleList struct {
	Rules []FilterRule `xml:"FilterRule,omitempty"`
}

// UnmarshalXML - decodes XML data.
func (ruleList *FilterRuleList) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type filterRuleList FilterRuleList
	rules := filterRuleList{}
	if err := d.DecodeElement(&rules, &start); err != nil {
		return err
	}

	// FilterRuleList must have only one prefix and/or suffix.
	nameSet := set.NewStringSet()
	for _, rule := range rules.Rules {
		if nameSet.Contains(rule.Name) {
			if rule.Name == "prefix" {
				return &ErrFilterNamePrefix{}
			}

			return &ErrFilterNameSuffix{}
		}

		nameSet.Add(rule.Name)
	}

	*ruleList = FilterRuleList(rules)
	return nil
}

// Pattern - returns pattern using prefix and suffix values.
func (ruleList FilterRuleList) Pattern() string {
	var prefix string
	var suffix string

	for _, rule := range ruleList.Rules {
		switch rule.Name {
		case "prefix":
			prefix = rule.Value
		case "suffix":
			suffix = rule.Value
		}
	}

	return NewPattern(prefix, suffix)
}

// S3Key - represents elements inside <S3Key>...</S3Key>
type S3Key struct {
	RuleList FilterRuleList `xml:"S3Key,omitempty" json:"S3Key,omitempty"`
}

// common - represents common elements inside <QueueConfiguration>, <CloudFunctionConfiguration>
// and <TopicConfiguration>
type common struct {
	ID     string `xml:"Id" json:"Id"`
	Filter S3Key  `xml:"Filter" json:"Filter"`
	Events []Name `xml:"Event" json:"Event"`
}

// Queue - represents elements inside <QueueConfiguration>
type Queue struct {
	common
	ARN ARN `xml:"Queue"`
}

// UnmarshalXML - decodes XML data.
func (q *Queue) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type queue Queue
	parsedQueue := queue{}
	if err := d.DecodeElement(&parsedQueue, &start); err != nil {
		return err
	}

	if len(parsedQueue.Events) == 0 {
		return errors.New("missing event name(s)")
	}

	eventStringSet := set.NewStringSet()
	for _, eventName := range parsedQueue.Events {
		if eventStringSet.Contains(eventName.String()) {
			return &ErrDuplicateEventName{eventName}
		}

		eventStringSet.Add(eventName.String())
	}

	*q = Queue(parsedQueue)

	return nil
}

// Validate - checks whether queue has valid values or not.
func (q Queue) Validate(region string, targetList *TargetList) error {
	if region != "" && q.ARN.region != region {
		return &ErrUnknownRegion{q.ARN.region}
	}

	if !targetList.Exists(q.ARN.TargetID) {
		return &ErrARNNotFound{q.ARN}
	}

	return nil
}

// SetRegion - sets region value to queue's ARN.
func (q *Queue) SetRegion(region string) {
	q.ARN.region = region
}

// ToRulesMap - converts Queue to RulesMap
func (q Queue) ToRulesMap() RulesMap {
	pattern := q.Filter.RuleList.Pattern()
	return NewRulesMap(q.Events, pattern, q.ARN.TargetID)
}

// Unused.  Available for completion.
type lambda struct {
	common
	ARN string `xml:"CloudFunction"`
}

// Unused. Available for completion.
type topic struct {
	common
	ARN string `xml:"Topic" json:"Topic"`
}

// Config - notification configuration described in
// http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
type Config struct {
	XMLName    xml.Name `xml:"NotificationConfiguration"`
	QueueList  []Queue  `xml:"QueueConfiguration,omitempty"`
	LambdaList []lambda `xml:"CloudFunctionConfiguration,omitempty"`
	TopicList  []topic  `xml:"TopicConfiguration,omitempty"`
}

// UnmarshalXML - decodes XML data.
func (conf *Config) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Make subtype to avoid recursive UnmarshalXML().
	type config Config
	parsedConfig := config{}
	if err := d.DecodeElement(&parsedConfig, &start); err != nil {
		return err
	}

	// Empty queue list means user wants to delete the notification configuration.
	if len(parsedConfig.QueueList) > 0 {
		for i, q1 := range parsedConfig.QueueList[:len(parsedConfig.QueueList)-1] {
			for _, q2 := range parsedConfig.QueueList[i+1:] {
				if reflect.DeepEqual(q1, q2) {
					return &ErrDuplicateQueueConfiguration{q1}
				}
			}
		}
	}

	if len(parsedConfig.LambdaList) > 0 || len(parsedConfig.TopicList) > 0 {
		return &ErrUnsupportedConfiguration{}
	}

	*conf = Config(parsedConfig)

	return nil
}

// Validate - checks whether config has valid values or not.
func (conf Config) Validate(region string, targetList *TargetList) error {
	for _, queue := range conf.QueueList {
		if err := queue.Validate(region, targetList); err != nil {
			return err
		}

		// TODO: Need to discuss/check why same ARN cannot be used in another queue configuration.
	}

	return nil
}

// SetRegion - sets region to all queue configuration.
func (conf *Config) SetRegion(region string) {
	for i := range conf.QueueList {
		conf.QueueList[i].SetRegion(region)
	}
}

// ToRulesMap - converts all queue configuration to RulesMap.
func (conf *Config) ToRulesMap() RulesMap {
	rulesMap := make(RulesMap)

	for _, queue := range conf.QueueList {
		rulesMap.Add(queue.ToRulesMap())
	}

	return rulesMap
}

// ParseConfig - parses data in reader to notification configuration.
func ParseConfig(reader io.Reader, region string, targetList *TargetList) (*Config, error) {
	var config Config
	if err := xml.NewDecoder(reader).Decode(&config); err != nil {
		return nil, err
	}

	if err := config.Validate(region, targetList); err != nil {
		return nil, err
	}

	config.SetRegion(region)

	return &config, nil
}
