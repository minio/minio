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

package notify

import (
	"github.com/minio/minio/pkg/event/target"
)

// Config - notification target configuration structure, holds
// information about various notification targets.
type Config struct {
	AMQP          map[string]target.AMQPArgs          `json:"amqp"`
	Elasticsearch map[string]target.ElasticsearchArgs `json:"elasticsearch"`
	Kafka         map[string]target.KafkaArgs         `json:"kafka"`
	MQTT          map[string]target.MQTTArgs          `json:"mqtt"`
	MySQL         map[string]target.MySQLArgs         `json:"mysql"`
	NATS          map[string]target.NATSArgs          `json:"nats"`
	NSQ           map[string]target.NSQArgs           `json:"nsq"`
	PostgreSQL    map[string]target.PostgreSQLArgs    `json:"postgresql"`
	Redis         map[string]target.RedisArgs         `json:"redis"`
	Webhook       map[string]target.WebhookArgs       `json:"webhook"`
}

const (
	defaultTarget = "1"
)

// NewConfig - initialize notification config.
func NewConfig() Config {
	// Make sure to initialize notification targets
	cfg := Config{
		NSQ:           make(map[string]target.NSQArgs),
		AMQP:          make(map[string]target.AMQPArgs),
		MQTT:          make(map[string]target.MQTTArgs),
		NATS:          make(map[string]target.NATSArgs),
		Redis:         make(map[string]target.RedisArgs),
		MySQL:         make(map[string]target.MySQLArgs),
		Kafka:         make(map[string]target.KafkaArgs),
		Webhook:       make(map[string]target.WebhookArgs),
		PostgreSQL:    make(map[string]target.PostgreSQLArgs),
		Elasticsearch: make(map[string]target.ElasticsearchArgs),
	}
	cfg.NSQ[defaultTarget] = target.NSQArgs{}
	cfg.AMQP[defaultTarget] = target.AMQPArgs{}
	cfg.MQTT[defaultTarget] = target.MQTTArgs{}
	cfg.NATS[defaultTarget] = target.NATSArgs{}
	cfg.Redis[defaultTarget] = target.RedisArgs{}
	cfg.MySQL[defaultTarget] = target.MySQLArgs{}
	cfg.Kafka[defaultTarget] = target.KafkaArgs{}
	cfg.Webhook[defaultTarget] = target.WebhookArgs{}
	cfg.PostgreSQL[defaultTarget] = target.PostgreSQLArgs{}
	cfg.Elasticsearch[defaultTarget] = target.ElasticsearchArgs{}
	return cfg
}
