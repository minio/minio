/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"fmt"
)

// Notifier represents collection of supported notification queues.
type notifier struct {
	AMQP          amqpConfigs          `json:"amqp"`
	NATS          natsConfigs          `json:"nats"`
	ElasticSearch elasticSearchConfigs `json:"elasticsearch"`
	Redis         redisConfigs         `json:"redis"`
	PostgreSQL    postgreSQLConfigs    `json:"postgresql"`
	Kafka         kafkaConfigs         `json:"kafka"`
	Webhook       webhookConfigs       `json:"webhook"`
	MySQL         mySQLConfigs         `json:"mysql"`
	MQTT          mqttConfigs          `json:"mqtt"`
	// Add new notification queues. IMPORTANT: When new queues are
	// added, update `serverConfig.ConfigDiff()` to reflect the
	// change.
}

type amqpConfigs map[string]amqpNotify

func (a amqpConfigs) Clone() amqpConfigs {
	a2 := make(amqpConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a amqpConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("AMQP [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

type mqttConfigs map[string]mqttNotify

func (a mqttConfigs) Clone() mqttConfigs {
	a2 := make(mqttConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a mqttConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("MQTT [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

type natsConfigs map[string]natsNotify

func (a natsConfigs) Clone() natsConfigs {
	a2 := make(natsConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a natsConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("NATS [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

type elasticSearchConfigs map[string]elasticSearchNotify

func (a elasticSearchConfigs) Clone() elasticSearchConfigs {
	a2 := make(elasticSearchConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a elasticSearchConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("ElasticSearch [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

type redisConfigs map[string]redisNotify

func (a redisConfigs) Clone() redisConfigs {
	a2 := make(redisConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a redisConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("Redis [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

type postgreSQLConfigs map[string]postgreSQLNotify

func (a postgreSQLConfigs) Clone() postgreSQLConfigs {
	a2 := make(postgreSQLConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a postgreSQLConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("PostgreSQL [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

type kafkaConfigs map[string]kafkaNotify

func (a kafkaConfigs) Clone() kafkaConfigs {
	a2 := make(kafkaConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a kafkaConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("Kafka [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

type webhookConfigs map[string]webhookNotify

func (a webhookConfigs) Clone() webhookConfigs {
	a2 := make(webhookConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a webhookConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("Webhook [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

type mySQLConfigs map[string]mySQLNotify

func (a mySQLConfigs) Clone() mySQLConfigs {
	a2 := make(mySQLConfigs, len(a))
	for k, v := range a {
		a2[k] = v
	}
	return a2
}

func (a mySQLConfigs) Validate() error {
	for k, v := range a {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("MySQL [%s] configuration invalid: %s", k, err.Error())
		}
	}
	return nil
}

func (n *notifier) Validate() error {
	if n == nil {
		return nil
	}
	if err := n.AMQP.Validate(); err != nil {
		return err
	}
	if err := n.NATS.Validate(); err != nil {
		return err
	}
	if err := n.ElasticSearch.Validate(); err != nil {
		return err
	}
	if err := n.Redis.Validate(); err != nil {
		return err
	}
	if err := n.PostgreSQL.Validate(); err != nil {
		return err
	}
	if err := n.Kafka.Validate(); err != nil {
		return err
	}
	if err := n.Webhook.Validate(); err != nil {
		return err
	}
	if err := n.MySQL.Validate(); err != nil {
		return err
	}
	return n.MQTT.Validate()
}

func (n *notifier) SetAMQPByID(accountID string, amqpn amqpNotify) {
	n.AMQP[accountID] = amqpn
}

func (n *notifier) GetAMQP() map[string]amqpNotify {
	return n.AMQP.Clone()
}

func (n *notifier) GetAMQPByID(accountID string) amqpNotify {
	return n.AMQP[accountID]
}

func (n *notifier) SetMQTTByID(accountID string, mqttn mqttNotify) {
	n.MQTT[accountID] = mqttn
}

func (n *notifier) GetMQTT() map[string]mqttNotify {
	return n.MQTT.Clone()
}

func (n *notifier) GetMQTTByID(accountID string) mqttNotify {
	return n.MQTT[accountID]
}

func (n *notifier) SetNATSByID(accountID string, natsn natsNotify) {
	n.NATS[accountID] = natsn
}

func (n *notifier) GetNATS() map[string]natsNotify {
	return n.NATS.Clone()
}

func (n *notifier) GetNATSByID(accountID string) natsNotify {
	return n.NATS[accountID]
}

func (n *notifier) SetElasticSearchByID(accountID string, es elasticSearchNotify) {
	n.ElasticSearch[accountID] = es
}

func (n *notifier) GetElasticSearchByID(accountID string) elasticSearchNotify {
	return n.ElasticSearch[accountID]
}

func (n *notifier) GetElasticSearch() map[string]elasticSearchNotify {
	return n.ElasticSearch.Clone()
}

func (n *notifier) SetRedisByID(accountID string, r redisNotify) {
	n.Redis[accountID] = r
}

func (n *notifier) GetRedis() map[string]redisNotify {
	return n.Redis.Clone()
}

func (n *notifier) GetRedisByID(accountID string) redisNotify {
	return n.Redis[accountID]
}

func (n *notifier) GetWebhook() map[string]webhookNotify {
	return n.Webhook.Clone()
}

func (n *notifier) GetWebhookByID(accountID string) webhookNotify {
	return n.Webhook[accountID]
}

func (n *notifier) SetWebhookByID(accountID string, pgn webhookNotify) {
	n.Webhook[accountID] = pgn
}

func (n *notifier) SetPostgreSQLByID(accountID string, pgn postgreSQLNotify) {
	n.PostgreSQL[accountID] = pgn
}

func (n *notifier) GetPostgreSQL() map[string]postgreSQLNotify {
	return n.PostgreSQL.Clone()
}

func (n *notifier) GetPostgreSQLByID(accountID string) postgreSQLNotify {
	return n.PostgreSQL[accountID]
}

func (n *notifier) SetMySQLByID(accountID string, pgn mySQLNotify) {
	n.MySQL[accountID] = pgn
}

func (n *notifier) GetMySQL() map[string]mySQLNotify {
	return n.MySQL.Clone()
}

func (n *notifier) GetMySQLByID(accountID string) mySQLNotify {
	return n.MySQL[accountID]
}

func (n *notifier) SetKafkaByID(accountID string, kn kafkaNotify) {
	n.Kafka[accountID] = kn
}

func (n *notifier) GetKafka() map[string]kafkaNotify {
	return n.Kafka.Clone()
}

func (n *notifier) GetKafkaByID(accountID string) kafkaNotify {
	return n.Kafka[accountID]
}
