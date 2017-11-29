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
	"fmt"

	"github.com/minio/minio/pkg/wildcard"
)

// SQS type.
const (
	// Minio sqs ARN prefix.
	minioSqs = "arn:minio:sqs:"

	// Static string indicating queue type 'amqp'.
	queueTypeAMQP = "amqp"
	// Static string indicating queue type 'mqtt'.
	queueTypeMQTT = "mqtt"
	// Static string indicating queue type 'nats'.
	queueTypeNATS = "nats"
	// Static string indicating queue type 'elasticsearch'.
	queueTypeElastic = "elasticsearch"
	// Static string indicating queue type 'redis'.
	queueTypeRedis = "redis"
	// Static string indicating queue type 'postgresql'.
	queueTypePostgreSQL = "postgresql"
	// Static string indicating queue type 'mysql'.
	queueTypeMySQL = "mysql"
	// Static string indicating queue type 'kafka'.
	queueTypeKafka = "kafka"
	// Static string for Webhooks
	queueTypeWebhook = "webhook"

	// Notifier format value constants
	formatNamespace = "namespace"
	formatAccess    = "access"
)

// Topic type.
const (
	// Minio topic ARN prefix.
	minioTopic = "arn:minio:sns:"

	// Static string indicating sns type 'listen'.
	snsTypeMinio = "listen"
)

var errNotifyNotEnabled = errors.New("requested notifier not enabled")

// Returns true if queueArn is for an AMQP queue.
func isAMQPQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeAMQP {
		return false
	}
	amqpL := globalServerConfig.Notify.GetAMQPByID(sqsArn.AccountID)
	if !amqpL.Enable {
		return false
	}
	// Connect to amqp server to validate.
	amqpC, err := dialAMQP(amqpL)
	if err != nil {
		errorIf(err, "Unable to connect to amqp service. %#v", amqpL)
		return false
	}
	defer amqpC.conn.Close()
	return true
}

// Returns true if mqttARN is for an MQTT queue.
func isMQTTQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeMQTT {
		return false
	}
	mqttL := globalServerConfig.Notify.GetMQTTByID(sqsArn.AccountID)
	if !mqttL.Enable {
		return false
	}
	// Connect to mqtt server to validate.
	mqttC, err := dialMQTT(mqttL)
	if err != nil {
		errorIf(err, "Unable to connect to mqtt service. %#v", mqttL)
		return false
	}
	defer mqttC.Client.Disconnect(250)
	return true
}

// Returns true if natsArn is for an NATS queue.
func isNATSQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeNATS {
		return false
	}
	natsL := globalServerConfig.Notify.GetNATSByID(sqsArn.AccountID)
	if !natsL.Enable {
		return false
	}
	// Connect to nats server to validate.
	natsC, err := dialNATS(natsL, true)
	if err != nil {
		errorIf(err, "Unable to connect to nats service. %#v", natsL)
		return false
	}
	closeNATS(natsC)
	return true
}

// Returns true if queueArn is for an Webhook queue
func isWebhookQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeWebhook {
		return false
	}
	rNotify := globalServerConfig.Notify.GetWebhookByID(sqsArn.AccountID)
	return rNotify.Enable
}

// Returns true if queueArn is for an Redis queue.
func isRedisQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeRedis {
		return false
	}
	rNotify := globalServerConfig.Notify.GetRedisByID(sqsArn.AccountID)
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
	esNotify := globalServerConfig.Notify.GetElasticSearchByID(sqsArn.AccountID)
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

// Returns true if queueArn is for PostgreSQL.
func isPostgreSQLQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypePostgreSQL {
		return false
	}
	pgNotify := globalServerConfig.Notify.GetPostgreSQLByID(sqsArn.AccountID)
	if !pgNotify.Enable {
		return false
	}
	pgC, err := dialPostgreSQL(pgNotify)
	if err != nil {
		errorIf(err, "Unable to connect to PostgreSQL server %#v", pgNotify)
		return false
	}
	defer pgC.Close()
	return true
}

// Returns true if queueArn is for MySQL.
func isMySQLQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeMySQL {
		return false
	}
	msqlNotify := globalServerConfig.Notify.GetMySQLByID(sqsArn.AccountID)
	if !msqlNotify.Enable {
		return false
	}
	myC, err := dialMySQL(msqlNotify)
	if err != nil {
		errorIf(err, "Unable to connect to MySQL server %#v", msqlNotify)
		return false
	}
	defer myC.Close()
	return true
}

// Returns true if queueArn is for Kafka.
func isKafkaQueue(sqsArn arnSQS) bool {
	if sqsArn.Type != queueTypeKafka {
		return false
	}
	kafkaNotifyCfg := globalServerConfig.Notify.GetKafkaByID(sqsArn.AccountID)
	if !kafkaNotifyCfg.Enable {
		return false
	}
	kafkaC, err := dialKafka(kafkaNotifyCfg)
	if err != nil {
		errorIf(err, "Unable to dial Kafka server %#v", kafkaNotifyCfg)
		return false
	}
	defer kafkaC.Close()
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
			prefixMatch = hasPrefix(object, fr.Value)
		} else if isValidFilterNameSuffix(fr.Name) {
			suffixMatch = hasSuffix(object, fr.Value)
		}
	}
	return prefixMatch && suffixMatch
}

// A type to represent dynamic error generation functions for
// notifications.
type notificationErrorFactoryFunc func(string, ...interface{}) error

// A function to build dynamic error generation functions for
// notifications by setting an error prefix string.
func newNotificationErrorFactory(prefix string) notificationErrorFactoryFunc {
	return func(msg string, a ...interface{}) error {
		s := fmt.Sprintf(msg, a...)
		return fmt.Errorf("%s: %s", prefix, s)
	}
}
