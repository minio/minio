/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"errors"
	"sort"
	"strings"

	"github.com/minio/minio-go/pkg/set"
)

// List of valid event types.
var suppportedEventTypes = map[string]struct{}{
	// Object created event types.
	"s3:ObjectCreated:*":                       {},
	"s3:ObjectCreated:Put":                     {},
	"s3:ObjectCreated:Post":                    {},
	"s3:ObjectCreated:Copy":                    {},
	"s3:ObjectCreated:CompleteMultipartUpload": {},
	// Object removed event types.
	"s3:ObjectRemoved:*":      {},
	"s3:ObjectRemoved:Delete": {},
	"s3:ObjectAccessed:Get":   {},
	"s3:ObjectAccessed:Head":  {},
	"s3:ObjectAccessed:*":     {},
}

// checkEvent - checks if an event is supported.
func checkEvent(event string) APIErrorCode {
	_, ok := suppportedEventTypes[event]
	if !ok {
		return ErrEventNotification
	}
	return ErrNone
}

// checkEvents - checks given list of events if all of them are valid.
// given if one of them is invalid, this function returns an error.
func checkEvents(events []string) APIErrorCode {
	for _, event := range events {
		if s3Error := checkEvent(event); s3Error != ErrNone {
			return s3Error
		}
	}
	return ErrNone
}

// Valid if filterName is 'prefix'.
func isValidFilterNamePrefix(filterName string) bool {
	return "prefix" == filterName
}

// Valid if filterName is 'suffix'.
func isValidFilterNameSuffix(filterName string) bool {
	return "suffix" == filterName
}

// Is this a valid filterName? - returns true if valid.
func isValidFilterName(filterName string) bool {
	return isValidFilterNamePrefix(filterName) || isValidFilterNameSuffix(filterName)
}

// hasOverlappingRules has for overlapping rules.
func hasOverlappingRules(rules []filterRule, matchFn func(string, string) bool) bool {
	var strs []string
	for _, rule := range rules {
		strs = append(strs, rule.Value)
	}
	sort.Strings(strs)
	if len(strs) > 1 {
		commonStr := strs[0]
		for _, str := range strs[1:] {
			if matchFn(str, commonStr) {
				return true
			}
		}
	}
	return false
}

func checkFilterRuleValues(filterRules []filterRule) APIErrorCode {
	ruleSet := set.NewStringSet()
	// Validate all filter rules.
	for _, filterRule := range filterRules {
		if !IsValidObjectPrefix(filterRule.Value) {
			return ErrFilterValueInvalid
		}

		// Set the new rule name to keep track of duplicates.
		ruleSet.Add(filterRule.Name)
	}

	// Success all filter rules validated.
	return ErrNone
}

// checkFilterRules - checks given list of filter rules if all of them are valid.
func checkFilterRulesPrefixSuffix(prefixFilterRules, suffixFilterRules []filterRule) APIErrorCode {
	if s3Error := checkFilterRuleValues(prefixFilterRules); s3Error != ErrNone {
		return s3Error
	}
	if s3Error := checkFilterRuleValues(suffixFilterRules); s3Error != ErrNone {
		return s3Error
	}
	if hasOverlappingRules(prefixFilterRules, strings.HasPrefix) {
		return ErrFilterNamePrefix
	}
	if hasOverlappingRules(suffixFilterRules, strings.HasSuffix) {
		return ErrFilterNameSuffix
	}
	return ErrNone
}

// Checks validity of input ARN for a given arnType.
func checkARN(arn, arnType string) APIErrorCode {
	if !strings.HasPrefix(arn, arnType) {
		return ErrARNNotification
	}
	strs := strings.SplitN(arn, ":", -1)
	if len(strs) != 6 {
		return ErrARNNotification
	}

	// Server region is allowed to be empty by default,
	// in such a scenario ARN region is not validating
	// allowing all regions.
	if sregion := globalServerConfig.GetRegion(); sregion != "" {
		region := strs[3]
		if region != sregion {
			return ErrRegionNotification
		}
	}
	accountID := strs[4]
	resource := strs[5]
	if accountID == "" || resource == "" {
		return ErrARNNotification
	}
	return ErrNone
}

// checkQueueARN - check if the queue arn is valid.
func checkQueueARN(queueARN string) APIErrorCode {
	return checkARN(queueARN, minioSqs)
}

// Validates account id for input queue ARN.
func isValidQueueID(queueARN string) bool {
	// Unmarshals QueueARN into structured object.
	sqsARN := unmarshalSqsARN(queueARN)
	// Is Queue identifier valid?.

	if isAMQPQueue(sqsARN) { // AMQP eueue.
		amqpN := globalServerConfig.Notify.GetAMQPByID(sqsARN.AccountID)
		return amqpN.Enable && amqpN.URL != ""
	} else if isMQTTQueue(sqsARN) {
		mqttN := globalServerConfig.Notify.GetMQTTByID(sqsARN.AccountID)
		return mqttN.Enable && mqttN.Broker != ""
	} else if isNATSQueue(sqsARN) {
		natsN := globalServerConfig.Notify.GetNATSByID(sqsARN.AccountID)
		return natsN.Enable && natsN.Address != ""
	} else if isElasticQueue(sqsARN) { // Elastic queue.
		elasticN := globalServerConfig.Notify.GetElasticSearchByID(sqsARN.AccountID)
		return elasticN.Enable && elasticN.URL != ""
	} else if isRedisQueue(sqsARN) { // Redis queue.
		redisN := globalServerConfig.Notify.GetRedisByID(sqsARN.AccountID)
		return redisN.Enable && redisN.Addr != ""
	} else if isPostgreSQLQueue(sqsARN) {
		pgN := globalServerConfig.Notify.GetPostgreSQLByID(sqsARN.AccountID)
		// Postgres can work with only default conn. info.
		return pgN.Enable
	} else if isMySQLQueue(sqsARN) {
		msqlN := globalServerConfig.Notify.GetMySQLByID(sqsARN.AccountID)
		// Mysql can work with only default conn. info.
		return msqlN.Enable
	} else if isKafkaQueue(sqsARN) {
		kafkaN := globalServerConfig.Notify.GetKafkaByID(sqsARN.AccountID)
		return (kafkaN.Enable && len(kafkaN.Brokers) > 0 &&
			kafkaN.Topic != "")
	} else if isWebhookQueue(sqsARN) {
		webhookN := globalServerConfig.Notify.GetWebhookByID(sqsARN.AccountID)
		return webhookN.Enable && webhookN.Endpoint != ""
	}
	return false
}

// Validates all incoming queue configs, checkQueueConfig validates if the
// input fields for each queues is not malformed and has valid configuration
// information.  If validation fails bucket notifications are not enabled.
func validateQueueConfigs(queueConfigs []queueConfig) APIErrorCode {
	var prefixFilterRules []filterRule
	var suffixFilterRules []filterRule

	for _, qConfig := range queueConfigs {
		// Check queue arn is valid.
		if s3Error := checkQueueARN(qConfig.QueueARN); s3Error != ErrNone {
			return s3Error
		}

		// Validate if the account ID is correct.
		if !isValidQueueID(qConfig.QueueARN) {
			return ErrARNNotification
		}

		// Check if valid events are set in queue config.
		if s3Error := checkEvents(qConfig.Events); s3Error != ErrNone {
			return s3Error
		}

		ruleSetPrefixes := set.NewStringSet()
		ruleSetSuffixes := set.NewStringSet()
		for _, filterRule := range qConfig.Filter.Key.FilterRules {
			// Unknown filter rule name found, returns an appropriate error.
			if !isValidFilterName(filterRule.Name) {
				return ErrFilterNameInvalid
			}

			// Check if filter name prefix is duplicated, we should throw an error.
			if isValidFilterNamePrefix(filterRule.Name) {
				if ruleSetPrefixes.Contains(filterRule.Name) {
					return ErrFilterNamePrefix
				}
				ruleSetPrefixes.Add(filterRule.Name)
				prefixFilterRules = append(prefixFilterRules, filterRule)
			}

			// Check if filter name suffix is duplicated, we should throw an error.
			if isValidFilterNameSuffix(filterRule.Name) {
				if ruleSetSuffixes.Contains(filterRule.Name) {
					return ErrFilterNameSuffix
				}
				ruleSetSuffixes.Add(filterRule.Name)
				suffixFilterRules = append(suffixFilterRules, filterRule)
			}
		}
	}

	// Check if valid filters are set in all queue configs.
	return checkFilterRulesPrefixSuffix(prefixFilterRules, suffixFilterRules)
}

// Validates all the bucket notification configuration for their validity,
// if one of the config is malformed or has invalid data it is rejected.
// Configuration is never applied partially.
func validateNotificationConfig(nConfig notificationConfig) APIErrorCode {
	// Minio server does not support lambda/topic configurations
	// currently. Such configuration is rejected.
	if len(nConfig.LambdaConfigs) > 0 || len(nConfig.TopicConfigs) > 0 {
		return ErrUnsupportedNotification
	}

	// Validate all queue configs.
	if s3Error := validateQueueConfigs(nConfig.QueueConfigs); s3Error != ErrNone {
		return s3Error
	}

	// Add validation for other configurations.
	return ErrNone
}

// Unmarshals input value of AWS ARN format into minioSqs object.
// Returned value represents minio sqs types, currently supported are
// - amqp
// - mqtt
// - nats
// - elasticsearch
// - redis
// - postgresql
// - mysql
// - kafka
// - webhook
func unmarshalSqsARN(queueARN string) (mSqs arnSQS) {
	strs := strings.SplitN(queueARN, ":", -1)
	if len(strs) != 6 {
		return
	}

	// Server region is allowed to be empty by default,
	// in such a scenario ARN region is not validating
	// allowing all regions.
	if sregion := globalServerConfig.GetRegion(); sregion != "" {
		region := strs[3]
		if region != sregion {
			return
		}
	}
	sqsType := strs[5]
	switch sqsType {
	case queueTypeAMQP:
		mSqs.Type = queueTypeAMQP
	case queueTypeMQTT:
		mSqs.Type = queueTypeMQTT
	case queueTypeNATS:
		mSqs.Type = queueTypeNATS
	case queueTypeElastic:
		mSqs.Type = queueTypeElastic
	case queueTypeRedis:
		mSqs.Type = queueTypeRedis
	case queueTypePostgreSQL:
		mSqs.Type = queueTypePostgreSQL
	case queueTypeMySQL:
		mSqs.Type = queueTypeMySQL
	case queueTypeKafka:
		mSqs.Type = queueTypeKafka
	case queueTypeWebhook:
		mSqs.Type = queueTypeWebhook
	default:
		errorIf(errors.New("invalid SQS type"), "SQS type: %s", sqsType)
	} // Add more queues here.

	mSqs.AccountID = strs[4]

	return
}
