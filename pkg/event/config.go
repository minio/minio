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

package event

import (
	"encoding/xml"
	"errors"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/minio/minio-go/v7/pkg/set"
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

func (filter FilterRule) isEmpty() bool {
	return filter.Name == "" && filter.Value == ""
}

// MarshalXML implements a custom marshaller to support `omitempty` feature.
func (filter FilterRule) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if filter.isEmpty() {
		return nil
	}
	type filterRuleWrapper FilterRule
	return e.EncodeElement(filterRuleWrapper(filter), start)
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

func (ruleList FilterRuleList) isEmpty() bool {
	return len(ruleList.Rules) == 0
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

// MarshalXML implements a custom marshaller to support `omitempty` feature.
func (s3Key S3Key) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if s3Key.RuleList.isEmpty() {
		return nil
	}
	type s3KeyWrapper S3Key
	return e.EncodeElement(s3KeyWrapper(s3Key), start)
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
	if q.ARN.region == "" {
		if !targetList.Exists(q.ARN.TargetID) {
			return &ErrARNNotFound{q.ARN}
		}
		return nil
	}

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
	ARN string `xml:"CloudFunction"`
}

// Unused. Available for completion.
type topic struct {
	ARN string `xml:"Topic" json:"Topic"`
}

// Config - notification configuration described in
// http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
type Config struct {
	XMLNS      string   `xml:"xmlns,attr,omitempty"`
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
				// Removes the region from ARN if server region is not set
				if q2.ARN.region != "" && q1.ARN.region == "" {
					q2.ARN.region = ""
				}
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
	//If xml namespace is empty, set a default value before returning.
	if config.XMLNS == "" {
		config.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
	}
	return &config, nil
}
