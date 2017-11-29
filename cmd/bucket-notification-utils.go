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

// checkFilterRules - checks given list of filter rules if all of them are valid.
func checkFilterRules(filterRules []filterRule) APIErrorCode {
	ruleSetMap := make(map[string]string)
	// Validate all filter rules.
	for _, filterRule := range filterRules {
		// Unknown filter rule name found, returns an appropriate error.
		if !isValidFilterName(filterRule.Name) {
			return ErrFilterNameInvalid
		}

		// Filter names should not be set twice per notification service
		// configuration, if found return an appropriate error.
		if _, ok := ruleSetMap[filterRule.Name]; ok {
			if isValidFilterNamePrefix(filterRule.Name) {
				return ErrFilterNamePrefix
			} else if isValidFilterNameSuffix(filterRule.Name) {
				return ErrFilterNameSuffix
			} else {
				return ErrFilterNameInvalid
			}
		}

		if !IsValidObjectPrefix(filterRule.Value) {
			return ErrFilterValueInvalid
		}

		// Set the new rule name to keep track of duplicates.
		ruleSetMap[filterRule.Name] = filterRule.Value
	}
	// Success all prefixes validated.
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

// Check - validates queue configuration and returns error if any.
func checkQueueConfig(qConfig queueConfig) APIErrorCode {
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

	// Check if valid filters are set in queue config.
	if s3Error := checkFilterRules(qConfig.Filter.Key.FilterRules); s3Error != ErrNone {
		return s3Error
	}

	// Success.
	return ErrNone
}

// Validates all incoming queue configs, checkQueueConfig validates if the
// input fields for each queues is not malformed and has valid configuration
// information.  If validation fails bucket notifications are not enabled.
func validateQueueConfigs(queueConfigs []queueConfig) APIErrorCode {
	for _, qConfig := range queueConfigs {
		if s3Error := checkQueueConfig(qConfig); s3Error != ErrNone {
			return s3Error
		}
	}
	// Success.
	return ErrNone
}

// Check all the queue configs for any duplicates.
func checkDuplicateQueueConfigs(configs []queueConfig) APIErrorCode {
	queueConfigARNS := set.NewStringSet()

	// Navigate through each configs and count the entries.
	for _, config := range configs {
		queueConfigARNS.Add(config.QueueARN)
	}

	if len(queueConfigARNS) != len(configs) {
		return ErrOverlappingConfigs
	}

	// Success.
	return ErrNone
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

	// Check for duplicate queue configs.
	if len(nConfig.QueueConfigs) > 1 {
		if s3Error := checkDuplicateQueueConfigs(nConfig.QueueConfigs); s3Error != ErrNone {
			return s3Error
		}
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
