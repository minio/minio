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

	"github.com/minio/minio/pkg/wildcard"
)

// SQS type.
const (
	// Minio sqs ARN prefix.
	minioSqs = "arn:minio:sqs:"

	// Static string indicating queue type 'amqp'.
	queueTypeAMQP = "amqp"
	// Static string indicating queue type 'elasticsearch'.
	queueTypeElastic = "elasticsearch"
	// Static string indicating queue type 'redis'.
	queueTypeRedis = "redis"
)

// Topic type.
const (
	// Minio topic ARN prefix.
	minioTopic = "arn:minio:sns:"

	// Static string indicating sns type 'listen'.
	snsTypeMinio = "listen"
)

var errNotifyNotEnabled = errors.New("requested notifier not enabled")

// Notifier represents collection of supported notification queues.
type notifier struct {
	AMQP          map[string]amqpNotify          `json:"amqp"`
	ElasticSearch map[string]elasticSearchNotify `json:"elasticsearch"`
	Redis         map[string]redisNotify         `json:"redis"`
	// Add new notification queues.
}

// Returns true if queueArn is for an AMQP queue.
func isAMQPQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeAMQP {
		return false
	}
	amqpL := serverConfig.GetAMQPNotifyByID(sqsArn.AccountID)
	if !amqpL.Enable {
		return false
	}
	// Connect to amqp server to validate.
	amqpC, err := dialAMQP(amqpL)
	if err != nil {
		errorIf(err, "Unable to connect to amqp service. %#v", amqpL)
		return false
	}
	defer amqpC.Close()
	return true
}

// Returns true if queueArn is for an Redis queue.
func isRedisQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeRedis {
		return false
	}
	rNotify := serverConfig.GetRedisNotifyByID(sqsArn.AccountID)
	if !rNotify.Enable {
		return false
	}
	// Connect to redis server to validate.
	rPool, err := dialRedis(rNotify)
	if err != nil {
		errorIf(err, "Unable to connect to redis service. %#v", rNotify)
		return false
	}
	defer rPool.Close()
	return true
}

// Returns true if queueArn is for an ElasticSearch queue.
func isElasticQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeElastic {
		return false
	}
	esNotify := serverConfig.GetElasticSearchNotifyByID(sqsArn.AccountID)
	if !esNotify.Enable {
		return false
	}
	elasticC, err := dialElastic(esNotify)
	if err != nil {
		errorIf(err, "Unable to connect to elasticsearch service %#v", esNotify)
		return false
	}
	defer elasticC.Stop()
	return true
}

// Match function matches wild cards in 'pattern' for events.
func eventMatch(eventType string, events []string) (ok bool) {
	for _, event := range events {
		ok = wildcard.MatchSimple(event, eventType)
		if ok {
			break
		}
	}
	return ok
}

// Filter rule match, matches an object against the filter rules.
func filterRuleMatch(object string, frs []filterRule) bool {
	var prefixMatch, suffixMatch = true, true
	for _, fr := range frs {
		if isValidFilterNamePrefix(fr.Name) {
			prefixMatch = strings.HasPrefix(object, fr.Value)
		} else if isValidFilterNameSuffix(fr.Name) {
			suffixMatch = strings.HasSuffix(object, fr.Value)
		}
	}
	return prefixMatch && suffixMatch
}
