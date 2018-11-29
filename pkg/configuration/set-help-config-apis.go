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
 *
 */

package configuration

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/minio/minio/cmd/logger"
)

const confFile string = "/home/ersan/work/src/github.com/minio/minio/pkg/configuration/examples/diskData.txt"

var serverConfHandler ServerConfigHandlers

type configKey interface {
	// Set honors both new and legacy keys. Check/validation logic
	// is also included in Set function.

	// The followings are the risks supporting both new and
	// legacy keys at the same time.
	// 1. The same config entry might have been set 2 different,
	// values, in which case, we'll honor the new key settings.
	// 2. Conflict btw GET and SET responses to legacy keys:
	// Get returns error message that the key is invalid while
	// Set sets the value for a legacy key.
	Set(key, value, comment string, cfg ServerConfig) error
	Help(key string) (helpText string, err error) //template like mc --help
}

// ServerConfig kv has leaf node/key names and their values
type ServerConfig struct {
	RWMutex *sync.RWMutex
	kv      map[string]string
}

// ServerConfigHandlers is the mux that routes to the key
// method that is going to be executed
type ServerConfigHandlers map[string]configKey

// Define Set and Help functions for each leaf node

// =VERSION= >>>>>>
type versionKey string

func (v versionKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (v versionKey) Help(key string) (string, error) {
	return "Display help information for \"version\"", nil
} // >>>>>>> =VERSION=

// =credential.accessKey= >>>>>>
type credentialAccessKey string

func (c credentialAccessKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c credentialAccessKey) Help(key string) (string, error) {
	return "Display help information for \"credential.accessKey\"", nil
} // >>>>>>> =credential.accessKey=

// =credential.secretKey= >>>>>>
type credentialSecretKey string

func (c credentialSecretKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c credentialSecretKey) Help(key string) (string, error) {
	return "Display help information for \"credential.secretKey\"", nil
} // >>>>>>> =credential.secretKey=

// =REGION=  >>>>>>
type regionKey string

func (r regionKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (r regionKey) Help(key string) (string, error) {
	return "Display help information for \"region\"", nil
} // >>>>>>> =REGION=

// =BROWSER= >>>>>>
type browserKey string

func (b browserKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (b browserKey) Help(key string) (string, error) {
	return "Display help information for \"browser\"", nil
} // >>>>>>> =BROWSER=

// =WORM= >>>>>>
type wormKey string

func (w wormKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (w wormKey) Help(key string) (string, error) {
	return "Display help information for \"worm\"", nil
} // >>>>>>> =WORM=

// =DOMAIN= >>>>>>
type domainKey string

func (d domainKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (d domainKey) Help(key string) (string, error) {
	return "Display help information for \"domain\"", nil
} // >>>>>>> =DOMAIN=

// =CACHE.DRIVES= >>>>>>
type cacheDrivesKey string

func (d cacheDrivesKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (d cacheDrivesKey) Help(key string) (string, error) {
	return "Display help information for \"cache.drives\"", nil
} // >>>>>>> =CACHE.DRIVES=

// =CACHE.EXPIRY= >>>>>>
type cacheExpiryKey string

func (c cacheExpiryKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c cacheExpiryKey) Help(key string) (string, error) {
	return "Display help information for \"cache.expiry\"", nil
} // >>>>>>> =CACHE.EXPIRY=

// =CACHE.MAXUSE= >>>>>>
type cacheMaxuseKey string

func (c cacheMaxuseKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c cacheMaxuseKey) Help(key string) (string, error) {
	return "Display help information for \"cache.maxuse\"", nil
} // >>>>>>> =CACHE.MAXUSE=

// =CACHE.EXCLUDE= >>>>>>
type cacheExcludeKey string

func (c cacheExcludeKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c cacheExcludeKey) Help(key string) (string, error) {
	return "Display help information for \"cache.exclude\"", nil
} // >>>>>>> =CACHE.EXCLUDE=

// =STORAGECLASS.STANDARD= >>>>>>
type storageclassStandardKey string

func (c storageclassStandardKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c storageclassStandardKey) Help(key string) (string, error) {
	return "Display help information for \"storageclass.standard\"", nil
} // >>>>>>> =STORAGECLASS.STANDARD=

// =STORAGECLASS.RRS= >>>>>>
type storageclassRRSKey string

func (c storageclassRRSKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c storageclassRRSKey) Help(key string) (string, error) {
	return "Display help information for \"storageclass.rrs\"", nil
} // >>>>>>> =STORAGECLASS.RRS=

// =KMS.VAULT.ENDPOINT= >>>>>>
type kmsVaultEndpointKey string

func (c kmsVaultEndpointKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c kmsVaultEndpointKey) Help(key string) (string, error) {
	return "Display help information for \"kms.vault.endpoint\"", nil
} // >>>>>>> =KMS.VAULT.ENDPOINT=

// =KMS.VAULT.AUTH.TYPE= >>>>>>
type kmsVaultAuthTypeKey string

func (c kmsVaultAuthTypeKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c kmsVaultAuthTypeKey) Help(key string) (string, error) {
	return "Display help information for \"kms.vault.auth.type\"", nil
} // >>>>>>> =KMS.VAULT.AUTH.TYPE=

// =KMS.VAULT.AUTH.APPROlE.ID= >>>>>>
type kmsVaultAuthApproleIDKey string

func (c kmsVaultAuthApproleIDKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c kmsVaultAuthApproleIDKey) Help(key string) (string, error) {
	return "Display help information for \"kms.vault.auth.approle.id\"", nil
} // >>>>>>> =KMS.VAULT.AUTH.APPROlE.ID=

// =KMS.VAULT.AUTH.APPROlE.SECRET= >>>>>>
type kmsVaultAuthApproleSecretKey string

func (c kmsVaultAuthApproleSecretKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c kmsVaultAuthApproleSecretKey) Help(key string) (string, error) {
	return "Display help information for \"kms.vault.auth.approle.secret\"", nil
} // >>>>>>> =KMS.VAULT.AUTH.APPROlE.SECRET=

// =KMS.VAULT.KEY-ID.NAME= >>>>>>
type kmsVaultKeyIDNameKey string

func (c kmsVaultKeyIDNameKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c kmsVaultKeyIDNameKey) Help(key string) (string, error) {
	return "Display help information for \"kms.vault.key-id.name\"", nil
} // >>>>>>> =KMS.VAULT.KEY-ID.NAME=

// =KMS.VAULT.KEY-ID.SECRET= >>>>>>
type kmsVaultKeyIDVersionKey string

func (c kmsVaultKeyIDVersionKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c kmsVaultKeyIDVersionKey) Help(key string) (string, error) {
	return "Display help information for \"kms.vault.key-id.version\"", nil
} // >>>>>>> =KMS.VAULT.KEY-ID.SECRET=

// =NOTIFY.AMQP.*= >>>>>>
type notifyAmqpAnyKey string

func (c notifyAmqpAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*\"", nil
} // >>>>>>> =NOTIFY.AMQP.*=

// =NOTIFY.AMQP.*.URL= >>>>>>
type notifyAmqpAnyURLKey string

func (c notifyAmqpAnyURLKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyURLKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.url\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.URL=

// =NOTIFY.AMQP.*.EXCHANGE= >>>>>>
type notifyAmqpAnyExchangeKey string

func (c notifyAmqpAnyExchangeKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyExchangeKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.exchange\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.EXCHANGE=

// =NOTIFY.AMQP.*.ROUTINGKEY= >>>>>>
type notifyAmqpAnyRoutingKeyKey string

func (c notifyAmqpAnyRoutingKeyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyRoutingKeyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.routingKey\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.ROUTINGKEY=

// =NOTIFY.AMQP.*.EXCHANGETYPE= >>>>>>
type notifyAmqpAnyExchangeTypeKey string

func (c notifyAmqpAnyExchangeTypeKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyExchangeTypeKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.exchangeType\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.EXCHANGETYPE=

// =NOTIFY.AMQP.*.DELIVERYMODE= >>>>>>
type notifyAmqpAnyDeliveryModeKey string

func (c notifyAmqpAnyDeliveryModeKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyDeliveryModeKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.deliveryMode\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.DELIVERYMODE=

// =NOTIFY.AMQP.*.MANDATORY= >>>>>>
type notifyAmqpAnyMandatoryKey string

func (c notifyAmqpAnyMandatoryKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyMandatoryKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.mandatory\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.MANDATORY=

// =NOTIFY.AMQP.*.IMMMEDIATE= >>>>>>
type notifyAmqpAnyImmediateKey string

func (c notifyAmqpAnyImmediateKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyImmediateKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.immediate\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.IMMMEDIATE=

// =NOTIFY.AMQP.*.DURABLE= >>>>>>
type notifyAmqpAnyDurableKey string

func (c notifyAmqpAnyDurableKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyDurableKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.durable\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.DURABLE=

// =NOTIFY.AMQP.*.INTERNAL= >>>>>>
type notifyAmqpAnyInternalKey string

func (c notifyAmqpAnyInternalKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyInternalKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.internal\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.INTERNAL=

// =NOTIFY.AMQP.*.NOWAIT= >>>>>>
type notifyAmqpAnyNoWaitKey string

func (c notifyAmqpAnyNoWaitKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyNoWaitKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.noWait\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.NOWAIT=

// =NOTIFY.AMQP.*.AUTODELETED= >>>>>>
type notifyAmqpAnyAutoDeletedKey string

func (c notifyAmqpAnyAutoDeletedKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyAmqpAnyAutoDeletedKey) Help(key string) (string, error) {
	return "Display help information for \"notify.amqp.*.autoDeleted\"", nil
} // >>>>>>> =NOTIFY.AMQP.*.AUTODELETED=

// =NOTIFY.ELASTICSEARCH.*= >>>>>>
type notifyElasticsearchAnyKey string

func (c notifyElasticsearchAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyElasticsearchAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.elasticsearch.*\"", nil
} // >>>>>>> =NOTIFY.ELASTICSEARCH.*=

// =NOTIFY.ELASTICSEARCH.*.FORMAT= >>>>>>
type notifyElasticsearchAnyFormatKey string

func (c notifyElasticsearchAnyFormatKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyElasticsearchAnyFormatKey) Help(key string) (string, error) {
	return "Display help information for \"notify.elasticsearch.*.format\"", nil
} // >>>>>>> =NOTIFY.ELASTICSEARCH.*.FORMAT=

// =NOTIFY.ELASTICSEARCH.*.URL= >>>>>>
type notifyElasticsearchAnyURLKey string

func (c notifyElasticsearchAnyURLKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyElasticsearchAnyURLKey) Help(key string) (string, error) {
	return "Display help information for \"notify.elasticsearch.*.url\"", nil
} // >>>>>>> =NOTIFY.ELASTICSEARCH.*.URL=

// =NOTIFY.ELASTICSEARCH.*.INDEX= >>>>>>
type notifyElasticsearchAnyIndexKey string

func (c notifyElasticsearchAnyIndexKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyElasticsearchAnyIndexKey) Help(key string) (string, error) {
	return "Display help information for \"notify.elasticsearch.*.index\"", nil
} // >>>>>>> =NOTIFY.ELASTICSEARCH.*.INDEX=

// =NOTIFY.KAFKA.*= >>>>>>
type notifyKafkaAnyKey string

func (c notifyKafkaAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*=

// =NOTIFY.KAFKA.*.BROKERS= >>>>>>
type notifyKafkaAnyBrokersKey string

func (c notifyKafkaAnyBrokersKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnyBrokersKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*.brokers\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*.BROKERS=

// =NOTIFY.KAFKA.*.TOPIC= >>>>>>
type notifyKafkaAnyTopicKey string

func (c notifyKafkaAnyTopicKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnyTopicKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*.topic\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*.TOPIC=

// =NOTIFY.KAFKA.*.TLS= >>>>>>
type notifyKafkaAnyTLSKey string

func (c notifyKafkaAnyTLSKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnyTLSKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*.tls\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*.TLS=

// =NOTIFY.KAFKA.*.TLS.SKIPVERIFY= >>>>>>
type notifyKafkaAnyTLSSkipVerifyKey string

func (c notifyKafkaAnyTLSSkipVerifyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnyTLSSkipVerifyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*.tls.skipVerify\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*.TLS.SKIPVERIFY=

// =NOTIFY.KAFKA.*.TLS.CLIENTAUTH= >>>>>>
type notifyKafkaAnyTLSClientAuthKey string

func (c notifyKafkaAnyTLSClientAuthKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnyTLSClientAuthKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*.tls.clientAuth\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*.TLS.CLIENTAUTH=

// =NOTIFY.KAFKA.*.SASL= >>>>>>
type notifyKafkaAnySaslKey string

func (c notifyKafkaAnySaslKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnySaslKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*.sasl\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*.SASL=

// =NOTIFY.KAFKA.*.SASL.USERNAME= >>>>>>
type notifyKafkaAnySaslUsernameKey string

func (c notifyKafkaAnySaslUsernameKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnySaslUsernameKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*.sasl.username\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*.SASL.USERNAME=

// =NOTIFY.KAFKA.*.SASL.PASSWORD= >>>>>>
type notifyKafkaAnySaslPasswordKey string

func (c notifyKafkaAnySaslPasswordKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyKafkaAnySaslPasswordKey) Help(key string) (string, error) {
	return "Display help information for \"notify.kafka.*.sasl.password\"", nil
} // >>>>>>> =NOTIFY.KAFKA.*.SASL.PASSWORD=

// =NOTIFY.MQTT.*= >>>>>>
type notifyMqttAnyKey string

func (c notifyMqttAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*\"", nil
} // >>>>>>> =NOTIFY.MQTT.*=

// =NOTIFY.MQTT.*.BROKER= >>>>>>
type notifyMqttAnyBrokerKey string

func (c notifyMqttAnyBrokerKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyBrokerKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*.broker\"", nil
} // >>>>>>> =NOTIFY.MQTT.*.BROKER=

// =NOTIFY.MQTT.*.TOPIC= >>>>>>
type notifyMqttAnyTopicKey string

func (c notifyMqttAnyTopicKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyTopicKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*.topic\"", nil
} // >>>>>>> =NOTIFY.MQTT.*.TOPIC=

// =NOTIFY.MQTT.*.QOS= >>>>>>
type notifyMqttAnyQosKey string

func (c notifyMqttAnyQosKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyQosKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*.qos\"", nil
} // >>>>>>> =NOTIFY.MQTT.*.QOS=

// =NOTIFY.MQTT.*.CLIENTID= >>>>>>
type notifyMqttAnyClientIDKey string

func (c notifyMqttAnyClientIDKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyClientIDKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*.clientId\"", nil
} // >>>>>>> =NOTIFY.MQTT.*.CLIENTID=

// =NOTIFY.MQTT.*.USERNAME= >>>>>>
type notifyMqttAnyUsernameKey string

func (c notifyMqttAnyUsernameKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyUsernameKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*.username\"", nil
} // >>>>>>> =NOTIFY.MQTT.*.USERNAME=

// =NOTIFY.MQTT.*.PASSWORD= >>>>>>
type notifyMqttAnyPasswordKey string

func (c notifyMqttAnyPasswordKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyPasswordKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*.password\"", nil
} // >>>>>>> =NOTIFY.MQTT.*.PASSWORD=

// =NOTIFY.MQTT.*.RECONNECTINTERVAL= >>>>>>
type notifyMqttAnyReconnectIntervalKey string

func (c notifyMqttAnyReconnectIntervalKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyReconnectIntervalKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*.reconnectInterval\"", nil
} // >>>>>>> =NOTIFY.MQTT.*.RECONNECTINTERVAL=

// =NOTIFY.MQTT.*.KEEPALIVEINTERVAL= >>>>>>
type notifyMqttAnyKeepAliveIntervalKey string

func (c notifyMqttAnyKeepAliveIntervalKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMqttAnyKeepAliveIntervalKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mqtt.*.keepAliveInterval\"", nil
} // >>>>>>> =NOTIFY.MQTT.*.KEEPALIVEINTERVAL=

// =NOTIFY.MYSQL.*= >>>>>>
type notifyMysqlAnyKey string

func (c notifyMysqlAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*=

// =NOTIFY.MYSQL.*.FORMAT= >>>>>>
type notifyMysqlAnyFormatKey string

func (c notifyMysqlAnyFormatKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyFormatKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*.format\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*.FORMAT=

// =NOTIFY.MYSQL.*.DSNSTRING= >>>>>>
type notifyMysqlAnyDsnStringKey string

func (c notifyMysqlAnyDsnStringKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyDsnStringKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*.dsnString\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*.DSNSTRING=

// =NOTIFY.MYSQL.*.TABLE= >>>>>>
type notifyMysqlAnyTableKey string

func (c notifyMysqlAnyTableKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyTableKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*.table\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*.TABLE=

// =NOTIFY.MYSQL.*.HOST= >>>>>>
type notifyMysqlAnyHostKey string

func (c notifyMysqlAnyHostKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyHostKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*.host\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*.HOST=

// =NOTIFY.MYSQL.*.PORT= >>>>>>
type notifyMysqlAnyPortKey string

func (c notifyMysqlAnyPortKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyPortKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*.port\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*.PORT=

// =NOTIFY.MYSQL.*.USER= >>>>>>
type notifyMysqlAnyUserKey string

func (c notifyMysqlAnyUserKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyUserKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*.user\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*.USER=

// =NOTIFY.MYSQL.*.PASSWORD= >>>>>>
type notifyMysqlAnyPasswordKey string

func (c notifyMysqlAnyPasswordKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyPasswordKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*.password\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*.PASSWORD=

// =NOTIFY.MYSQL.*.DATABASE= >>>>>>
type notifyMysqlAnyDatabaseKey string

func (c notifyMysqlAnyDatabaseKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyMysqlAnyDatabaseKey) Help(key string) (string, error) {
	return "Display help information for \"notify.mysql.*.database\"", nil
} // >>>>>>> =NOTIFY.MYSQL.*.DATABASE=

// =NOTIFY.NATS.*= >>>>>>
type notifyNatsAnyKey string

func (c notifyNatsAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*\"", nil
} // >>>>>>> =NOTIFY.NATS.*=

// =NOTIFY.NATS.*.ADDRESS= >>>>>>
type notifyNatsAnyAddressKey string

func (c notifyNatsAnyAddressKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyAddressKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.address\"", nil
} // >>>>>>> =NOTIFY.NATS.*.ADDRESS=

// =NOTIFY.NATS.*.SUBJECT= >>>>>>
type notifyNatsAnySubjectKey string

func (c notifyNatsAnySubjectKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnySubjectKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.subject\"", nil
} // >>>>>>> =NOTIFY.NATS.*.SUBJECT=

// =NOTIFY.NATS.*.USERNAME= >>>>>>
type notifyNatsAnyUsernameKey string

func (c notifyNatsAnyUsernameKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyUsernameKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.username\"", nil
} // >>>>>>> =NOTIFY.NATS.*.USERNAME=

// =NOTIFY.NATS.*.PASSWORD= >>>>>>
type notifyNatsAnyPasswordKey string

func (c notifyNatsAnyPasswordKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyPasswordKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.password\"", nil
} // >>>>>>> =NOTIFY.NATS.*.PASSWORD=

// =NOTIFY.NATS.*.TOKEN= >>>>>>
type notifyNatsAnyTokenKey string

func (c notifyNatsAnyTokenKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyTokenKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.token\"", nil
} // >>>>>>> =NOTIFY.NATS.*.TOKEN=

// =NOTIFY.NATS.*.SECURE= >>>>>>
type notifyNatsAnySecureKey string

func (c notifyNatsAnySecureKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnySecureKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.secure\"", nil
} // >>>>>>> =NOTIFY.NATS.*.SECURE=

// =NOTIFY.NATS.*.PINGINTERVAL= >>>>>>
type notifyNatsAnyPingIntervalKey string

func (c notifyNatsAnyPingIntervalKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyPingIntervalKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.pingInterval\"", nil
} // >>>>>>> =NOTIFY.NATS.*.PINGINTERVAL=

// =NOTIFY.NATS.*.STREAMING= >>>>>>
type notifyNatsAnyStreamingKey string

func (c notifyNatsAnyStreamingKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyStreamingKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.streaming\"", nil
} // >>>>>>> =NOTIFY.NATS.*.STREAMING=

// =NOTIFY.NATS.*.STREAMING.CLUSTERID= >>>>>>
type notifyNatsAnyStreamingClusterIDKey string

func (c notifyNatsAnyStreamingClusterIDKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyStreamingClusterIDKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.streaming.clusterID\"", nil
} // >>>>>>> =NOTIFY.NATS.*.STREAMING.CLUSTERID=

// =NOTIFY.NATS.*.STREAMING.CLIENTID= >>>>>>
type notifyNatsAnyStreamingClientIDKey string

func (c notifyNatsAnyStreamingClientIDKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyStreamingClientIDKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.streaming.clientID\"", nil
} // >>>>>>> =NOTIFY.NATS.*.STREAMING.CLIENTID=

// =NOTIFY.NATS.*.STREAMING.ASYNC= >>>>>>
type notifyNatsAnyStreamingAsyncKey string

func (c notifyNatsAnyStreamingAsyncKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyStreamingAsyncKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.streaming.async\"", nil
} // >>>>>>> =NOTIFY.NATS.*.STREAMING.ASYNC=

// =NOTIFY.NATS.*.STREAMING.MAXPUBACKSINGLIGHT= >>>>>>
type notifyNatsAnyStreamingMaxPubAcksInflightKey string

func (c notifyNatsAnyStreamingMaxPubAcksInflightKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyNatsAnyStreamingMaxPubAcksInflightKey) Help(key string) (string, error) {
	return "Display help information for \"notify.nats.*.streaming.maxPubAcksInflight\"", nil
} // >>>>>>> =NOTIFY.NATS.*.STREAMING.MAXPUBACKSINGLIGHT=

// =NOTIFY.POSTGRESQL.*= >>>>>>
type notifyPostgresqlAnyKey string

func (c notifyPostgresqlAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*=

// =NOTIFY.POSTGRESQL.*.FORMAT= >>>>>>
type notifyPostgresqlAnyFormatKey string

func (c notifyPostgresqlAnyFormatKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyFormatKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*.format\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*.FORMAT=

// =NOTIFY.POSTGRESQL.*.CONNECTIONSTRING= >>>>>>
type notifyPostgresqlAnyConnectionStringKey string

func (c notifyPostgresqlAnyConnectionStringKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyConnectionStringKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*.connectionString\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*.CONNECTIONSTRING=

// =NOTIFY.POSTGRESQL.*.TABLE= >>>>>>
type notifyPostgresqlAnyTableKey string

func (c notifyPostgresqlAnyTableKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyTableKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*.table\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*.TABLE=

// =NOTIFY.POSTGRESQL.*.HOST= >>>>>>
type notifyPostgresqlAnyHostKey string

func (c notifyPostgresqlAnyHostKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyHostKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*.host\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*.HOST=

// =NOTIFY.POSTGRESQL.*.PORT= >>>>>>
type notifyPostgresqlAnyPortKey string

func (c notifyPostgresqlAnyPortKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyPortKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*.port\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*.PORT=

// =NOTIFY.POSTGRESQL.*.USER= >>>>>>
type notifyPostgresqlAnyUserKey string

func (c notifyPostgresqlAnyUserKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyUserKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*.user\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*.USER=

// =NOTIFY.POSTGRESQL.*.PASSWORD= >>>>>>
type notifyPostgresqlAnyPasswordKey string

func (c notifyPostgresqlAnyPasswordKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyPasswordKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*.password\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*.PASSWORD=

// =NOTIFY.POSTGRESQL.*.DATABASE= >>>>>>
type notifyPostgresqlAnyDatabaseKey string

func (c notifyPostgresqlAnyDatabaseKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyPostgresqlAnyDatabaseKey) Help(key string) (string, error) {
	return "Display help information for \"notify.postgresql.*.database\"", nil
} // >>>>>>> =NOTIFY.POSTGRESQL.*.DATABASE=

// =NOTIFY.REDIS.*= >>>>>>
type notifyRedisAnyKey string

func (c notifyRedisAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyRedisAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.redis.*\"", nil
} // >>>>>>> =NOTIFY.REDIS.*=

// =NOTIFY.REDIS.*.FORMAT= >>>>>>
type notifyRedisAnyFormatKey string

func (c notifyRedisAnyFormatKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyRedisAnyFormatKey) Help(key string) (string, error) {
	return "Display help information for \"notify.redis.*.format\"", nil
} // >>>>>>> =NOTIFY.REDIS.*.FORMAT=

// =NOTIFY.REDIS.*.ADDRESS= >>>>>>
type notifyRedisAnyAddressKey string

func (c notifyRedisAnyAddressKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyRedisAnyAddressKey) Help(key string) (string, error) {
	return "Display help information for \"notify.redis.*.address\"", nil
} // >>>>>>> =NOTIFY.REDIS.*.ADDRESS=

// =NOTIFY.REDIS.*.PASSWORD= >>>>>>
type notifyRedisAnyPasswordKey string

func (c notifyRedisAnyPasswordKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyRedisAnyPasswordKey) Help(key string) (string, error) {
	return "Display help information for \"notify.redis.*.password\"", nil
} // >>>>>>> =NOTIFY.REDIS.*.PASSWORD=

// =NOTIFY.REDIS.*.KEY= >>>>>>
type notifyRedisAnyKeyKey string

func (c notifyRedisAnyKeyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyRedisAnyKeyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.redis.*.key\"", nil
} // >>>>>>> =NOTIFY.REDIS.*.KEY=

// =NOTIFY.WEBHOOK.*= >>>>>>
type notifyWebhookAnyKey string

func (c notifyWebhookAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyWebhookAnyKey) Help(key string) (string, error) {
	return "Display help information for \"notify.webhook.*\"", nil
} // >>>>>>> =NOTIFY.WEBHOOK.*=

// =NOTIFY.WEBHOOK.*.ENNDPOINT= >>>>>>
type notifyWebhookAnyEndpointKey string

func (c notifyWebhookAnyEndpointKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (c notifyWebhookAnyEndpointKey) Help(key string) (string, error) {
	return "Display help information for \"notify.webhook.*.enndpoint\"", nil
} // >>>>>>> =NOTIFY.WEBHOOK.*.ENNDPOINT=

// =LOG.CONSOLE= >>>>>>
type logConsoleKey string

func (l logConsoleKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logConsoleKey) Help(key string) (string, error) {
	return "Display help information for \"log.console\"", nil
} // >>>>>>> =LOG.CONSOLE=

// =LOG.CONSOLE.AUDIT= >>>>>>
type logConsoleAuditKey string

func (l logConsoleAuditKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logConsoleAuditKey) Help(key string) (string, error) {
	return "Display help information for \"log.console.audit\"", nil
} // >>>>>>> =LOG.CONSOLE.AUDIT=

// =LOG.CONSOLE.ANONYMOUS >>>>>>
type logConsoleAnonymousKey string

func (l logConsoleAnonymousKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logConsoleAnonymousKey) Help(key string) (string, error) {
	return "Display help information for \"log.console.anonymous\"", nil
} // >>>>>>> =LOG.CONSOLE.ANONYMOUS

// =LOG.HTTP.*= >>>>>>
type logHTTPAnyKey string

func (l logHTTPAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logHTTPAnyKey) Help(key string) (string, error) {
	return "Display help information for \"log.http.*\"", nil
} // >>>>>>> =LOG.HTTP.*=

// =LOG.HTTP.*.ENDPOINT= >>>>>>
type logHTTPAnyEndpointKey string

func (l logHTTPAnyEndpointKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logHTTPAnyEndpointKey) Help(key string) (string, error) {
	return "Display help information for \"log.http.*.endpoint\"", nil
} // >>>>>>> =LOG.HTTP.*.ENDPOINT=

// =LOG.HTTP.*.AUDIT= >>>>>>
type logHTTPAnyAuditKey string

func (l logHTTPAnyAuditKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logHTTPAnyAuditKey) Help(key string) (string, error) {
	return "Display help information for \"log.http.*.audit\"", nil
} // >>>>>>> =LOG.HTTP.*.AUDIT=

// =LOG.HTTP.*.ANONYMOUS= >>>>>>
type logHTTPAnyAnonymousKey string

func (l logHTTPAnyAnonymousKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logHTTPAnyAnonymousKey) Help(key string) (string, error) {
	return "Display help information for \"log.http.*.anonymous\"", nil
} // >>>>>>> =LOG.HTTP.*.ANONYMOUS=

// =LOGGER.CONSOLE= >>>>>>
type loggerConsoleKey string

func (l loggerConsoleKey) Set(key, val, comment string, cfg ServerConfig) error {
	// This is a deprecated key function. It'll still stay
	// active, but we save the value in "log.console.*"
	key = strings.Replace(key, "logger", "log", 1)
	cfg.kv[key] = val
	logger.LogIf(context.Background(), errors.New("Key name \"logger\" is DEPRECATED!\nWrote the value in \"log\" instead"))
	return nil
}
func (l loggerConsoleKey) Help(key string) (string, error) {
	return "No help information for DEPRECATED key \"logger\"", nil
} // >>>>>>> =LOGGER.CONSOLE=

// =LOGGER.CONSOLE.AUDIT= >>>>>>
type loggerConsoleAuditKey string

func (l loggerConsoleAuditKey) Set(key, val, comment string, cfg ServerConfig) error {
	// This is a deprecated key function. It'll still stay
	// active, but we save the value in "log.console.*"
	key = strings.Replace(key, "logger", "log", 1)
	cfg.kv[key] = val
	logger.LogIf(context.Background(), errors.New("Key name \"logger\" is DEPRECATED!\nWrote the value in \"log\" instead"))
	return nil
}
func (l loggerConsoleAuditKey) Help(key string) (string, error) {
	return "No help information for DEPRECATED key \"logger\"", nil
} // >>>>>>> =LOGGER.CONSOLE.AUDIT=

// =LOGGER.CONSOLE.ANONYMOUS >>>>>>
type loggerConsoleAnonymousKey string

func (l loggerConsoleAnonymousKey) Set(key, val, comment string, cfg ServerConfig) error {
	// This is a deprecated key function. It'll still stay
	// active, but we save the value in "log.console.*"
	key = strings.Replace(key, "logger", "log", 1)
	cfg.kv[key] = val
	logger.LogIf(context.Background(), errors.New("Key name \"logger\" is DEPRECATED!\nWrote the value in \"log\" instead"))
	return nil
}
func (l loggerConsoleAnonymousKey) Help(key string) (string, error) {
	return "No help information for DEPRECATED key \"logger\"", nil
} // >>>>>>> =LOGGER.CONSOLE.ANONYMOUS

// =LOGGER.HTTP.*= >>>>>>
type loggerHTTPAnyKey string

func (l loggerHTTPAnyKey) Set(key, val, comment string, cfg ServerConfig) error {
	// This is a deprecated key function. It'll still stay
	// active, but we save the value in "log.http.*"
	key = strings.Replace(key, "logger", "log", 1)
	cfg.kv[key] = val
	logger.LogIf(context.Background(), errors.New("Key name \"logger\" is DEPRECATED!\nWrote the value in \"log\" instead"))
	return nil
}
func (l loggerHTTPAnyKey) Help(key string) (string, error) {
	return "No help information for DEPRECATED \"logger.http.*\"", nil
} // >>>>>>> =LOGGER.HTTP.*=

// =LOGGER.HTTP.*.ENDPOINT= >>>>>>
type loggerHTTPAnyEndpointKey string

func (l loggerHTTPAnyEndpointKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l loggerHTTPAnyEndpointKey) Help(key string) (string, error) {
	return "Display help information for \"logger.http.*.endpoint\"", nil
} // >>>>>>> =LOGGER.HTTP.*.ENDPOINT=

// =LOGGER.HTTP.*.AUDIT= >>>>>>
type loggerHTTPAnyAuditKey string

func (l loggerHTTPAnyAuditKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l loggerHTTPAnyAuditKey) Help(key string) (string, error) {
	return "Display help information for \"logger.http.*.audit\"", nil
} // >>>>>>> =LOGGER.HTTP.*.AUDIT=

// =LOGGER.HTTP.*.ANONYMOUS= >>>>>>
type loggerHTTPAnyAnonymousKey string

func (l loggerHTTPAnyAnonymousKey) Set(key, val, comment string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l loggerHTTPAnyAnonymousKey) Help(key string) (string, error) {
	return "Display help information for \"logger.http.*.anonymous\"", nil
} // >>>>>>> =LOGGER.HTTP.*.ANONYMOUS=

func init() {
	var versionK versionKey

	var credentialAccessK credentialAccessKey
	var credentialSecretK credentialSecretKey

	var regionK regionKey
	var browserK browserKey
	var wormK wormKey
	var domainK domainKey

	var storageclassStandardK storageclassStandardKey
	var storageclassRRSK storageclassRRSKey

	var cacheDrivesK cacheDrivesKey
	var cacheExpiryK cacheExpiryKey
	var cacheMaxuseK cacheMaxuseKey
	var cacheExcludeK cacheExcludeKey

	var kmsVaultEndpointK kmsVaultEndpointKey
	var kmsVaultAuthTypeK kmsVaultAuthTypeKey
	var kmsVaultAuthApproleIDK kmsVaultAuthApproleIDKey
	var kmsVaultAuthApproleSecretK kmsVaultAuthApproleSecretKey
	var kmsVaultKeyIDNameK kmsVaultKeyIDNameKey
	var kmsVaultKeyIDVersionK kmsVaultKeyIDVersionKey

	var notifyAmqpAnyK notifyAmqpAnyKey
	var notifyAmqpAnyURLK notifyAmqpAnyURLKey
	var notifyAmqpAnyExchangeK notifyAmqpAnyExchangeKey
	var notifyAmqpAnyRoutingKeyK notifyAmqpAnyRoutingKeyKey
	var notifyAmqpAnyExchangeTypeK notifyAmqpAnyExchangeTypeKey
	var notifyAmqpAnyDeliveryModeK notifyAmqpAnyDeliveryModeKey
	var notifyAmqpAnyMandatoryK notifyAmqpAnyMandatoryKey
	var notifyAmqpAnyImmediateK notifyAmqpAnyImmediateKey
	var notifyAmqpAnyDurableK notifyAmqpAnyDurableKey
	var notifyAmqpAnyInternalK notifyAmqpAnyInternalKey
	var notifyAmqpAnyNoWaitK notifyAmqpAnyNoWaitKey
	var notifyAmqpAnyAutoDeletedK notifyAmqpAnyAutoDeletedKey

	var notifyElasticsearchAnyK notifyElasticsearchAnyKey
	var notifyElasticsearchAnyFormatK notifyElasticsearchAnyFormatKey
	var notifyElasticsearchAnyURLK notifyElasticsearchAnyURLKey
	var notifyElasticsearchAnyIndexK notifyElasticsearchAnyIndexKey

	var notifyKafkaAnyK notifyKafkaAnyKey
	var notifyKafkaAnyBrokersK notifyKafkaAnyBrokersKey
	var notifyKafkaAnyTopicK notifyKafkaAnyTopicKey
	var notifyKafkaAnyTLSK notifyKafkaAnyTLSKey
	var notifyKafkaAnyTLSSkipVerifyK notifyKafkaAnyTLSSkipVerifyKey
	var notifyKafkaAnyTLSClientAuthK notifyKafkaAnyTLSClientAuthKey
	var notifyKafkaAnySaslK notifyKafkaAnySaslKey
	var notifyKafkaAnySaslUsernameK notifyKafkaAnySaslUsernameKey
	var notifyKafkaAnySaslPasswordK notifyKafkaAnySaslPasswordKey

	var notifyMqttAnyK notifyMqttAnyKey
	var notifyMqttAnyBrokerK notifyMqttAnyBrokerKey
	var notifyMqttAnyTopicK notifyMqttAnyTopicKey
	var notifyMqttAnyQosK notifyMqttAnyQosKey
	var notifyMqttAnyClientIDK notifyMqttAnyClientIDKey
	var notifyMqttAnyUsernameK notifyMqttAnyUsernameKey
	var notifyMqttAnyPasswordK notifyMqttAnyPasswordKey
	var notifyMqttAnyReconnectIntervalK notifyMqttAnyReconnectIntervalKey
	var notifyMqttAnyKeepAliveIntervalK notifyMqttAnyKeepAliveIntervalKey

	var notifyMysqlAnyK notifyMysqlAnyKey
	var notifyMysqlAnyFormatK notifyMysqlAnyFormatKey
	var notifyMysqlAnyDsnStringK notifyMysqlAnyDsnStringKey
	var notifyMysqlAnyTableK notifyMysqlAnyTableKey
	var notifyMysqlAnyHostK notifyMysqlAnyHostKey
	var notifyMysqlAnyPortK notifyMysqlAnyPortKey
	var notifyMysqlAnyUserK notifyMysqlAnyUserKey
	var notifyMysqlAnyPasswordK notifyMysqlAnyPasswordKey
	var notifyMysqlAnyDatabaseK notifyMysqlAnyDatabaseKey

	var notifyNatsAnyK notifyNatsAnyKey
	var notifyNatsAnyAddressK notifyNatsAnyAddressKey
	var notifyNatsAnySubjectK notifyNatsAnySubjectKey
	var notifyNatsAnyUsernameK notifyNatsAnyUsernameKey
	var notifyNatsAnyPasswordK notifyNatsAnyPasswordKey
	var notifyNatsAnyTokenK notifyNatsAnyTokenKey
	var notifyNatsAnySecureK notifyNatsAnySecureKey
	var notifyNatsAnyPingIntervalK notifyNatsAnyPingIntervalKey
	var notifyNatsAnyStreamingK notifyNatsAnyStreamingKey
	var notifyNatsAnyStreamingClusterIDK notifyNatsAnyStreamingClusterIDKey
	var notifyNatsAnyStreamingClientIDK notifyNatsAnyStreamingClientIDKey
	var notifyNatsAnyStreamingAsyncK notifyNatsAnyStreamingAsyncKey
	var notifyNatsAnyStreamingMaxPubAcksInflightK notifyNatsAnyStreamingMaxPubAcksInflightKey

	var notifyPostgresqlAnyK notifyPostgresqlAnyKey
	var notifyPostgresqlAnyFormatK notifyPostgresqlAnyFormatKey
	var notifyPostgresqlAnyConnectionStringK notifyPostgresqlAnyConnectionStringKey
	var notifyPostgresqlAnyTableK notifyPostgresqlAnyTableKey
	var notifyPostgresqlAnyHostK notifyPostgresqlAnyHostKey
	var notifyPostgresqlAnyPortK notifyPostgresqlAnyPortKey
	var notifyPostgresqlAnyUserK notifyPostgresqlAnyUserKey
	var notifyPostgresqlAnyPasswordK notifyPostgresqlAnyPasswordKey
	var notifyPostgresqlAnyDatabaseK notifyPostgresqlAnyDatabaseKey

	var notifyRedisAnyK notifyRedisAnyKey
	var notifyRedisAnyFormatK notifyRedisAnyFormatKey
	var notifyRedisAnyAddressK notifyRedisAnyAddressKey
	var notifyRedisAnyPasswordK notifyRedisAnyPasswordKey
	var notifyRedisAnyKeyK notifyRedisAnyKeyKey

	var notifyWebhookAnyK notifyWebhookAnyKey
	var notifyWebhookAnyEndpointK notifyWebhookAnyEndpointKey

	var logConsoleK logConsoleKey
	var logConsoleAuditK logConsoleAuditKey
	var logConsoleAnonymousK logConsoleAnonymousKey

	var logHTTPAnyK logHTTPAnyKey
	var logHTTPAnyEndpointK logHTTPAnyEndpointKey
	var logHTTPAnyAuditK logHTTPAnyAuditKey
	var logHTTPAnyAnonymousK logHTTPAnyAnonymousKey

	var loggerConsoleK loggerConsoleKey
	var loggerConsoleAuditK loggerConsoleAuditKey
	var loggerConsoleAnonymousK loggerConsoleAnonymousKey

	var loggerHTTPAnyK loggerHTTPAnyKey
	var loggerHTTPAnyEndpointK loggerHTTPAnyEndpointKey
	var loggerHTTPAnyAuditK loggerHTTPAnyAuditKey
	var loggerHTTPAnyAnonymousK loggerHTTPAnyAnonymousKey

	serverConfHandler = ServerConfigHandlers{}

	// Register Set and Help commands for each leaf node
	serverConfHandler["version"] = versionK

	serverConfHandler["credential.accessKey"] = credentialAccessK
	serverConfHandler["credential.secretKey"] = credentialSecretK

	serverConfHandler["region"] = regionK
	serverConfHandler["browser"] = browserK
	serverConfHandler["worm"] = wormK
	serverConfHandler["domain"] = domainK

	serverConfHandler["storageclass.standard"] = storageclassStandardK
	serverConfHandler["storageclass.rrs"] = storageclassRRSK

	serverConfHandler["cache.drives"] = cacheDrivesK
	serverConfHandler["cache.expiry"] = cacheExpiryK
	serverConfHandler["cache.maxuse"] = cacheMaxuseK
	serverConfHandler["cache.exclude"] = cacheExcludeK

	serverConfHandler["kms.vault.endpoint"] = kmsVaultEndpointK
	serverConfHandler["kms.vault.auth.type"] = kmsVaultAuthTypeK
	serverConfHandler["kms.vault.auth.approle.id"] = kmsVaultAuthApproleIDK
	serverConfHandler["kms.vault.auth.approle.secret"] = kmsVaultAuthApproleSecretK
	serverConfHandler["kms.vault.key-id.name"] = kmsVaultKeyIDNameK
	serverConfHandler["kms.vault.key-id.version"] = kmsVaultKeyIDVersionK

	serverConfHandler["notify.amqp.*"] = notifyAmqpAnyK
	serverConfHandler["notify.amqp.*.url"] = notifyAmqpAnyURLK
	serverConfHandler["notify.amqp.*.exchange"] = notifyAmqpAnyExchangeK
	serverConfHandler["notify.amqp.*.routingKey"] = notifyAmqpAnyRoutingKeyK
	serverConfHandler["notify.amqp.*.exchangeType"] = notifyAmqpAnyExchangeTypeK
	serverConfHandler["notify.amqp.*.deliveryMode"] = notifyAmqpAnyDeliveryModeK
	serverConfHandler["notify.amqp.*.mandatory"] = notifyAmqpAnyMandatoryK
	serverConfHandler["notify.amqp.*.immediate"] = notifyAmqpAnyImmediateK
	serverConfHandler["notify.amqp.*.durable"] = notifyAmqpAnyDurableK
	serverConfHandler["notify.amqp.*.internal"] = notifyAmqpAnyInternalK
	serverConfHandler["notify.amqp.*.noWait"] = notifyAmqpAnyNoWaitK
	serverConfHandler["notify.amqp.*.autoDeleted"] = notifyAmqpAnyAutoDeletedK

	serverConfHandler["notify.elasticsearch.*"] = notifyElasticsearchAnyK
	serverConfHandler["notify.elasticsearch.*.format"] = notifyElasticsearchAnyFormatK
	serverConfHandler["notify.elasticsearch.*.url"] = notifyElasticsearchAnyURLK
	serverConfHandler["notify.elasticsearch.*.index"] = notifyElasticsearchAnyIndexK

	serverConfHandler["notify.kafka.*"] = notifyKafkaAnyK
	serverConfHandler["notify.kafka.*.brokers"] = notifyKafkaAnyBrokersK
	serverConfHandler["notify.kafka.*.topic"] = notifyKafkaAnyTopicK
	serverConfHandler["notify.kafka.*.tls"] = notifyKafkaAnyTLSK
	serverConfHandler["notify.kafka.*.tls.skipVerify"] = notifyKafkaAnyTLSSkipVerifyK
	serverConfHandler["notify.kafka.*.tls.clientAuth"] = notifyKafkaAnyTLSClientAuthK
	serverConfHandler["notify.kafka.*.sasl"] = notifyKafkaAnySaslK
	serverConfHandler["notify.kafka.*.sasl.username"] = notifyKafkaAnySaslUsernameK
	serverConfHandler["notify.kafka.*.sasl.password"] = notifyKafkaAnySaslPasswordK

	serverConfHandler["notify.mqtt.*"] = notifyMqttAnyK
	serverConfHandler["notify.mqtt.*.broker"] = notifyMqttAnyBrokerK
	serverConfHandler["notify.mqtt.*.topic"] = notifyMqttAnyTopicK
	serverConfHandler["notify.mqtt.*.qos"] = notifyMqttAnyQosK
	serverConfHandler["notify.mqtt.*.clientId"] = notifyMqttAnyClientIDK
	serverConfHandler["notify.mqtt.*.username"] = notifyMqttAnyUsernameK
	serverConfHandler["notify.mqtt.*.password"] = notifyMqttAnyPasswordK
	serverConfHandler["notify.mqtt.*.reconnectInterval"] = notifyMqttAnyReconnectIntervalK
	serverConfHandler["notify.mqtt.*.keepAliveInterval"] = notifyMqttAnyKeepAliveIntervalK

	serverConfHandler["notify.mysql.*"] = notifyMysqlAnyK
	serverConfHandler["notify.mysql.*.format"] = notifyMysqlAnyFormatK
	serverConfHandler["notify.mysql.*.dsnString"] = notifyMysqlAnyDsnStringK
	serverConfHandler["notify.mysql.*.table"] = notifyMysqlAnyTableK
	serverConfHandler["notify.mysql.*.host"] = notifyMysqlAnyHostK
	serverConfHandler["notify.mysql.*.port"] = notifyMysqlAnyPortK
	serverConfHandler["notify.mysql.*.user"] = notifyMysqlAnyUserK
	serverConfHandler["notify.mysql.*.password"] = notifyMysqlAnyPasswordK
	serverConfHandler["notify.mysql.*.database"] = notifyMysqlAnyDatabaseK

	serverConfHandler["notify.nats.*"] = notifyNatsAnyK
	serverConfHandler["notify.nats.*.address"] = notifyNatsAnyAddressK
	serverConfHandler["notify.nats.*.subject"] = notifyNatsAnySubjectK
	serverConfHandler["notify.nats.*.username"] = notifyNatsAnyUsernameK
	serverConfHandler["notify.nats.*.password"] = notifyNatsAnyPasswordK
	serverConfHandler["notify.nats.*.token"] = notifyNatsAnyTokenK
	serverConfHandler["notify.nats.*.secure"] = notifyNatsAnySecureK
	serverConfHandler["notify.nats.*.pingInterval"] = notifyNatsAnyPingIntervalK
	serverConfHandler["notify.nats.*.streaming"] = notifyNatsAnyStreamingK
	serverConfHandler["notify.nats.*.streaming.clusterID"] = notifyNatsAnyStreamingClusterIDK
	serverConfHandler["notify.nats.*.streaming.clientID"] = notifyNatsAnyStreamingClientIDK
	serverConfHandler["notify.nats.*.streaming.async"] = notifyNatsAnyStreamingAsyncK
	serverConfHandler["notify.nats.*.streaming.maxPubAcksInflight"] = notifyNatsAnyStreamingMaxPubAcksInflightK

	serverConfHandler["notify.postgresql.*"] = notifyPostgresqlAnyK
	serverConfHandler["notify.postgresql.*.format"] = notifyPostgresqlAnyFormatK
	serverConfHandler["notify.postgresql.*.connectionString"] = notifyPostgresqlAnyConnectionStringK
	serverConfHandler["notify.postgresql.*.table"] = notifyPostgresqlAnyTableK
	serverConfHandler["notify.postgresql.*.host"] = notifyPostgresqlAnyHostK
	serverConfHandler["notify.postgresql.*.port"] = notifyPostgresqlAnyPortK
	serverConfHandler["notify.postgresql.*.user"] = notifyPostgresqlAnyUserK
	serverConfHandler["notify.postgresql.*.password"] = notifyPostgresqlAnyPasswordK
	serverConfHandler["notify.postgresql.*.database"] = notifyPostgresqlAnyDatabaseK

	serverConfHandler["notify.redis.*"] = notifyRedisAnyK
	serverConfHandler["notify.redis.*.format"] = notifyRedisAnyFormatK
	serverConfHandler["notify.redis.*.address"] = notifyRedisAnyAddressK
	serverConfHandler["notify.redis.*.password"] = notifyRedisAnyPasswordK
	serverConfHandler["notify.redis.*.key"] = notifyRedisAnyKeyK

	serverConfHandler["notify.webhook.*"] = notifyWebhookAnyK
	serverConfHandler["notify.webhook.*.endpoint"] = notifyWebhookAnyEndpointK

	serverConfHandler["log.console"] = logConsoleK
	serverConfHandler["log.console.audit"] = logConsoleAuditK
	serverConfHandler["log.console.anonymous"] = logConsoleAnonymousK

	serverConfHandler["log.http.*"] = logHTTPAnyK
	serverConfHandler["log.http.*.endpoint"] = logHTTPAnyEndpointK
	serverConfHandler["log.http.*.audit"] = logHTTPAnyAuditK
	serverConfHandler["log.http.*.anonymous"] = logHTTPAnyAnonymousK

	// Key "logger" is deprecated. The new key is "log"
	serverConfHandler["logger.console"] = loggerConsoleK
	serverConfHandler["logger.console.audit"] = loggerConsoleAuditK
	serverConfHandler["logger.console.anonymous"] = loggerConsoleAnonymousK

	serverConfHandler["logger.http.*"] = loggerHTTPAnyK
	serverConfHandler["logger.http.*.endpoint"] = loggerHTTPAnyEndpointK
	serverConfHandler["logger.http.*.audit"] = loggerHTTPAnyAuditK
	serverConfHandler["logger.http.*.anonymous"] = loggerHTTPAnyAnonymousK
}

func checkRandomKeyValidity(key string, cfgHandler ServerConfigHandlers) (string, error) {
	// Keys with user specified random subkey will be
	// validated in this function. If valid, key will also be tranformed
	// to "transformedKey" with random subkey replaced with a "*"
	var transformedKey string
	// Min/Max number of characters required/allowed in a user
	// specified random subkey must be in between 3 and 64
	minNoOfChrs := "1"
	maxNoOfChrs := "64"

	// Variable to hold the regular expression
	var r *regexp.Regexp
	// Base regular expression for both "log" and
	// "notify" keys with a user specified random subkey
	basePattern := "\\.([0-9a-zA-Z]{" + minNoOfChrs + "," + maxNoOfChrs + "})(|"

	// Decide if the key is a child of "log" or "notify"
	splitKey := strings.Split(key, ".")
	switch splitKey[0] {
	// Log key regular expression is used to replace the
	// user specified random subKey with a "*"
	// Example:
	// key = "log.http.target1" where "target1" is the user specified
	// random key. If key is valid, it'll be transformed into "log.http.*",
	// which will  be used to get/set the value using ServerConfig
	case "log", "logger":
		suffixPattern := "\\.endpoint|\\.audit|\\.anonymous)$"
		r, _ = regexp.Compile("(log\\.http|logger\\.http)" + basePattern + suffixPattern)
	case "notify":
		// Second key after "notify" key
		switch splitKey[1] {
		case "amqp":
			fmt.Println("Entered AMQP")
			suffixPattern := "\\.url|\\.exchange|\\.routingKey|\\.exchangeType|\\.deliveryMode|\\.mandatory|\\.immediate|\\.durable|\\.internal|\\.noWait|\\.autoDeleted)$"
			r, _ = regexp.Compile("(notify\\.amqp)" + basePattern + suffixPattern)
		case "elasticsearch":
			suffixPattern := "\\.format|\\.url|\\.index)$"
			r, _ = regexp.Compile("(notify\\.elasticsearch)" + basePattern + suffixPattern)
		case "kafka":
			suffixPattern := "\\.brokers|\\.topic|\\.tls|\\.tls\\.skipVerify|\\.tls\\.clientAuth|\\.sasl|\\.sasl\\.username|\\.sasl\\.password)$"
			r, _ = regexp.Compile("(notify\\.kafka)" + basePattern + suffixPattern)
		case "mqtt":
			suffixPattern := "\\.broker|\\.topic|\\.qos|\\.clientId|\\.username|\\.password|\\.reconnectInterval|\\.keepAliveInterval)$"
			r, _ = regexp.Compile("(notify\\.mqtt)" + basePattern + suffixPattern)
		case "mysql":
			suffixPattern := "\\.format|\\.dsnString|\\.table|\\.host|\\.port|\\.user|\\.password|\\.database)$"
			r, _ = regexp.Compile("(notify\\.mysql)" + basePattern + suffixPattern)
		case "nats":
			suffixPattern := "\\.address|\\.subject|\\.username|\\.password|\\.token|\\.secure|\\.pingInterval|\\.streaming|\\.streaming\\.clusterID|\\.streaming\\.clientID|\\.streaming\\.async|\\.streaming\\.maxPubAcksInflight)$"
			r, _ = regexp.Compile("(notify\\.nats)" + basePattern + suffixPattern)
		case "postgresql":
			suffixPattern := "\\.format|\\.connectionString|\\.table|\\.host|\\.port|\\.user|\\.password|\\.database)$"
			r, _ = regexp.Compile("(notify\\.postgresql)" + basePattern + suffixPattern)
		case "redis":
			suffixPattern := "\\.endpoint|\\.format|\\.address|\\.password|\\.key)$"
			r, _ = regexp.Compile("(notify\\.redis)" + basePattern + suffixPattern)
		case "webhook":
			suffixPattern := "\\.endpoint)$"
			r, _ = regexp.Compile("(notify\\.webhook)" + basePattern + suffixPattern)
		}
	default:
		// Unexpected key name.
		logger.LogIf(context.Background(), errors.New("Invalid key:"+key))
		// Just assume to match the full key name
		r, _ = regexp.Compile(key)
	}

	// Get submatched subkeys using regular expression decided above
	// FindStringSubmatch is expected to generate a slice with the
	// following information:
	// [fullKey, keysBeforeRandomKey, randomKey, keysAfterRandomKey]
	matchedKeys := r.FindStringSubmatch(key)
	if splitKey[1] == "amqp" {
		fmt.Println("AMQP regexp r:", r)
		fmt.Println("key:", key)
		fmt.Println("matchedKeys: ", matchedKeys)
		fmt.Println()
	}
	// Initialization
	randomKey := ""
	// Less than 3 matches in slice means key is invalid
	if len(matchedKeys) < 3 {
		logger.LogIf(context.Background(), errors.New("Invalid key:"+key))
		return "", errors.New("ERROR: Invalid key, " + key)
	}
	randomKey = matchedKeys[2]

	// Replace found user specified random subKey with a "*".
	// The transformedKey is the new key for ServerConfigHandlers map.
	transformedKey = strings.Replace(matchedKeys[0], randomKey, "*", 1)

	if _, ok := cfgHandler[transformedKey]; ok {
		return transformedKey, nil
	}
	return "", errors.New("ERROR: Invalid key, " + key)

}

// SetHandler sets key value in server configuration database
func (s *ServerConfig) SetHandler(key, val, comment string) error {
	var err error

	// Load the configuration data from disk into memory
	if err = s.load(); err != nil {
		return errors.New("Failed to load the configuration file." + err.Error())
	}

	// Set
	// Validate assuming the key is a regular key with no user specified
	// random subkey in it. If this fails, try validating the key
	// assuming it has a user specified random subkey in it.
	// If the key is found to be valid, set it to val.
	if _, ok := serverConfHandler[key]; ok {
		if err := serverConfHandler[key].Set(key, val, comment, *s); err != nil {
			return errors.New("Failed to set config key='" + key + "', to value='" + val + "'. " + err.Error())
		}
	} else if transformedKey, err := checkRandomKeyValidity(key, serverConfHandler); err == nil {
		// Validity check for keys with user specified random subkey
		if err := serverConfHandler[transformedKey].Set(key, val, comment, *s); err != nil {
			return errors.New("Failed to set config key='" + key + "', to value='" + val + "'. " + err.Error())
		}
	}

	// Save the set/modified configuration from memory to disk
	if err = s.save(); err != nil {
		return errors.New("Can't save config file after setting key='" + key + "', to value='" + val + "'. " + err.Error())
	}
	return nil
}

// GetHandler gets single or multiple or full configuration info
func (s *ServerConfig) GetHandler(keys []string) (map[string]string, error) {
	// Load the configuration data from disk into memory
	if err := s.load(); err != nil {
		return map[string]string{}, err
	}

	// Zero length keys means full configuration
	// file is returned
	if len(keys) == 0 {
		fmt.Println("FULL CONFIG")
		return s.kv, nil
	}
	// Greater than zero length keys means only the
	// the values for the provided keys are returned
	kvPartial := make(map[string]string)
	for _, key := range keys {
		fmt.Println("PARTIAL CONFIG")
		kvPartial[key] = s.kv[key]
	}
	return kvPartial, nil
}

// HelpHandler displays key name, its type/syntax and
// a short description for its purpose
func (s *ServerConfig) HelpHandler(key string) (string, error) {
	// Validate assuming it is a regular key with no user specified
	// random subkey. If it fails, try validating the key assuming it
	// has a user specified random subkey
	var helpText string
	var err error
	if _, ok := serverConfHandler[key]; ok {
		if helpText, err = serverConfHandler[key].Help(key); err != nil {
			return "", err
		}
	} else if transformedKey, err := checkRandomKeyValidity(key, serverConfHandler); err == nil {
		// Validity check for keys with a user specified random subkey
		if helpText, err = serverConfHandler[transformedKey].Help(key); err != nil {
			return "", err
		}
	}
	return helpText, nil
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// writeLines writes the lines to the given file.
func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func normalizeConfigEntry(entry string) (entryArr []string, isComment, validKV bool) {
	// fmt.Println("entry --> ", entry)

	rComment, _ := regexp.Compile("[\\s]*(#[\\s]*[^\\s]*)|[\\s]*([^\\s]+)([\\s]*=[\\s]*)([^\\s]+)[\\s]*(#[\\s]*[^\\s]*)")
	matchedComment := rComment.FindStringSubmatch(entry)

	if len(matchedComment) > 0 {
		// All comments are handled in this block
		if matchedComment[1] == "" {
			if len(matchedComment) >= 6 {
				// Combination of key/value and comment in the
				// same line is handled here.
				// Since this line is a combination, we return
				// false for "isComment" boolean
				return strings.Split(strings.Replace(entry, matchedComment[3], " ", -1), " "), false, true
			}
			// fmt.Println("Invalid key=value pair, ", entry)
			logger.LogIf(context.Background(), errors.New("Invalid key=value pair, "+entry))
			return strings.Split(entry, " "), true, false
		}
		// Pure comments are handled here
		return strings.Split(matchedComment[1], " "), true, true
	}
	// Handle key=val pairs here
	r, _ := regexp.Compile("[\\s]*([^\\s]+)([\\s]*=[\\s]*)([^\\s]+)")
	matchedKey := r.FindStringSubmatch(entry)
	if len(matchedKey) >= 3 {
		return strings.Split(strings.Replace(entry, matchedKey[2], " ", -1), " "), false, true
	}
	logger.LogIf(context.Background(), errors.New("Invalid key=value pair, "+entry))
	return strings.Split(entry, " "), false, false
}

// Load loads configuration from disk to memory (serverConfig.kv)
func (s *ServerConfig) load() error {
	fmt.Println("Entered Load")
	s.kv = make(map[string]string)

	// Check if configuration file exists
	if _, err := os.Stat(confFile); os.IsNotExist(err) {
		logger.Fatal(errors.New("No configuration file found: "), err.Error())
	} else {
		// Configuration file exists.
		// Read configuration data from etcd or file
		s.RWMutex.RLock()
		defer s.RWMutex.RUnlock()

		lines, err := readLines(confFile)
		if err != nil {
			return err
		}

		// Go through each line of config file
		var commentAttached, lineWithComment string
		for _, line := range lines {
			element, isComment, validKV := normalizeConfigEntry(line)
			if !validKV {
				continue
			}
			if isComment {
				commentAttached += strings.Join(element, " ") + "\n"
				continue
			} else {
				lineWithComment = commentAttached + strings.Join(element, " ")
				commentAttached = ""
			}
			// Set the server configuration map, "s.kv",
			key := element[0]
			val := element[1]
			// fmt.Printf("[%s]: %s\n", key, val)
			// Validate assuming the key is a regular key with no user specified
			// random subkey in it. If this check fails, try validating the key
			// assuming it has a user specified random subkey in it.
			if _, ok := serverConfHandler[key]; ok {
				if err := serverConfHandler[key].Set(key, val, lineWithComment, *s); err != nil {
					// Report the error and continue with the next element
					logger.LogIf(context.Background(), err)
				}
				continue
			} else if transformedKey, err := checkRandomKeyValidity(key, serverConfHandler); err == nil {
				// Validity check for keys with user specified random subkey
				if err := serverConfHandler[transformedKey].Set(key, val, lineWithComment, *s); err != nil {
					// Report the error and continue with the next element
					logger.LogIf(context.Background(), err)
				}
				continue
			}
			logger.LogIf(context.Background(), errors.New("Invalid key: "+key))
		}
	}
	return nil
}

// Save saves configuration info into disk
func (s *ServerConfig) save() error {
	var byteData []byte

	// Lock for writing
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()

	for k, v := range s.kv {
		byteData = append(byteData, []byte(k+" = "+v+"\n")...)
	}

	// The following line is for debugging purposes
	if err := ioutil.WriteFile(confFile, byteData, 0644); err != nil {
		return err
	}
	return nil
}
