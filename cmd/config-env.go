package cmd

import (
	"errors"
	"os"
	"strconv"
)

type envConfigV19 struct {
	Credential *credential
	Region     *string
	Browser    *BrowserFlag

	Notify envNotifier
}

type envNotifier struct {
	AMQP          envNotifyAMQP
	NATS          envNotifyNATS
	ElasticSearch envNotifyElasticSearch
	Redis         envNotifyRedis
	PostgreSQL    envNotifyPostgreSQL
	Kafka         envNotifyKafka
	Webhook       envNotifyWebhook
	MySQL         envNotifyMySQL
	MQTT          envNotifyMQTT
}

type envNotifyAMQP struct {
	Enable       *bool
	URL          *string
	Exchange     *string
	RoutingKey   *string
	ExchangeType *string
	DeliveryMode *uint8
	Mandatory    *bool
	Immediate    *bool
	Durable      *bool
	Internal     *bool
	NoWait       *bool
	AutoDeleted  *bool
}

type envNotifyNATSStreaming struct {
	Enable             *bool
	ClusterID          *string
	ClientID           *string
	Async              *bool
	MaxPubAcksInflight *int
}

type envNotifyNATS struct {
	Enable       *bool
	Address      *string
	Subject      *string
	Username     *string
	Password     *string
	Token        *string
	Secure       *bool
	PingInterval *int64
	Streaming    envNotifyNATSStreaming
}

type envNotifyElasticSearch struct {
	Enable *bool
	Format *string
	URL    *string
	Index  *string
}

type envNotifyRedis struct {
	Enable   *bool
	Format   *string
	Addr     *string
	Password *string
	Key      *string
}

type envNotifyPostgreSQL struct {
	Enable           *bool
	Format           *string
	ConnectionString *string
	Table            *string
	Host             *string
	Port             *string
	User             *string
	Password         *string
	Database         *string
}

type envNotifyKafka struct {
	Enable *bool
	Topic  *string
}

type envNotifyWebhook struct {
	Enable   *bool
	Endpoint *string
}

type envNotifyMySQL struct {
	Enable    *bool
	Format    *string
	DsnString *string
	Table     *string
	Host      *string
	Port      *string
	User      *string
	Password  *string
	Database  *string
}

type envNotifyMQTT struct {
	Enable   *bool
	Broker   *string
	Topic    *string
	QoS      *int
	ClientID *string
	User     *string
	Password *string
}

func envConfigLoad() envConfigV19 {
	envConfig := envConfigV19{}

	envConfig.Credential = envConfigCredential()
	envConfig.Browser = envConfigBrowser()
	envConfig.Region = envConfigRegion()

	envConfig.Notify.AMQP = envConfigNotifyAMQPLoad()
	envConfig.Notify.NATS = envConfigNotifyNATSLoad()
	envConfig.Notify.ElasticSearch = envConfigNotifyElasticSearchLoad()
	envConfig.Notify.Redis = envConfigNotifyRedisLoad()
	envConfig.Notify.PostgreSQL = envConfigNotifyPostgreSQLLoad()
	envConfig.Notify.Kafka = envConfigNotifyKafkaLoad()
	envConfig.Notify.Webhook = envConfigNotifyWebhookLoad()
	envConfig.Notify.MySQL = envConfigNotifyMySQLLoad()
	envConfig.Notify.MQTT = envConfigNotifyMQTTLoad()

	return envConfig
}

func envConfigBrowser() *BrowserFlag {
	if browser := os.Getenv("MINIO_BROWSER"); browser != "" {
		browserFlag, err := ParseBrowserFlag(browser)
		if err != nil {
			fatalIf(errors.New("invalid value"), "Unknown value ‘%s’ in MINIO_BROWSER environment variable.", browser)
		}

		return &browserFlag
	}

	return nil
}

func envConfigCredential() *credential {
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if accessKey != "" && secretKey != "" {
		cred := credential{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}

		return &cred
	}

	return nil
}

func envConfigRegion() *string {
	if serverRegion := os.Getenv("MINIO_REGION"); serverRegion != "" {
		return &serverRegion
	}

	return nil
}

func envConfigNotifyAMQPLoad() envNotifyAMQP {
	amqp := envNotifyAMQP{}

	envLoaderBool(&amqp.Enable, "MINIO_AMQP_ENABLE")
	envLoaderString(&amqp.URL, "MINIO_AMQP_URL")
	envLoaderString(&amqp.Exchange, "MINIO_AMQP_EXCHANGE")
	envLoaderString(&amqp.RoutingKey, "MINIO_AMQP_ROUTING_KEY")
	envLoaderString(&amqp.ExchangeType, "MINIO_AMQP_EXCHANGE_TYPE")
	envLoaderUint8(&amqp.DeliveryMode, "MINIO_AMQP_DELIVERY_MODE")
	envLoaderBool(&amqp.Mandatory, "MINIO_AMQP_MANDATORY")
	envLoaderBool(&amqp.Immediate, "MINIO_AMQP_IMMEDIATE")
	envLoaderBool(&amqp.Durable, "MINIO_AMQP_DURABLE")
	envLoaderBool(&amqp.Internal, "MINIO_AMQP_INTERNAL")
	envLoaderBool(&amqp.NoWait, "MINIO_AMQP_NO_WAIT")
	envLoaderBool(&amqp.AutoDeleted, "MINIO_AMQP_AUTO_DELETED")

	return amqp
}

func envConfigNotifyNATSLoad() envNotifyNATS {
	nats := envNotifyNATS{}

	envLoaderBool(&nats.Enable, "MINIO_NATS_ENABLE")
	envLoaderString(&nats.Address, "MINIO_NATS_ADDRESS")
	envLoaderString(&nats.Subject, "MINIO_NATS_SUBJECT")
	envLoaderString(&nats.Username, "MINIO_NATS_USERNAME")
	envLoaderString(&nats.Password, "MINIO_NATS_PASSWORD")
	envLoaderString(&nats.Token, "MINIO_NATS_TOKEN")
	envLoaderBool(&nats.Secure, "MINIO_NATS_SECURE")
	envLoaderInt64(&nats.PingInterval, "MINIO_NATS_PING_INTERVAL")

	nats.Streaming = envConfigNotifyNATSStreamingLoad()

	return nats
}

func envConfigNotifyNATSStreamingLoad() envNotifyNATSStreaming {
	natsStreaming := envNotifyNATSStreaming{}

	envLoaderBool(&natsStreaming.Enable, "MINIO_NATS_STREAMING_ENABLE")
	envLoaderString(&natsStreaming.ClusterID, "MINIO_NATS_STREAMING_CLUSTER_ID")
	envLoaderString(&natsStreaming.ClientID, "MINIO_NATS_STREAMING_CLIENT_ID")
	envLoaderBool(&natsStreaming.Async, "MINIO_NATS_STREAMING_ASYNC")
	envLoaderInt(&natsStreaming.MaxPubAcksInflight, "MINIO_NATS_STREAMING_MAX_PUBACK_INFLIGHT")

	return natsStreaming
}

func envConfigNotifyElasticSearchLoad() envNotifyElasticSearch {
	elasticSearch := envNotifyElasticSearch{}

	envLoaderBool(&elasticSearch.Enable, "MINIO_ELASTICSEARCH_ENABLE")
	envLoaderString(&elasticSearch.Format, "MINIO_ELASTICSEARCH_FORMAT")
	envLoaderString(&elasticSearch.URL, "MINIO_ELASTICSEARCH_URL")
	envLoaderString(&elasticSearch.Index, "MINIO_ELASTICSEARCH_INDEX")

	return elasticSearch
}

func envConfigNotifyRedisLoad() envNotifyRedis {
	redis := envNotifyRedis{}

	envLoaderBool(&redis.Enable, "MINIO_REDIS_ENABLE")
	envLoaderString(&redis.Format, "MINIO_REDIS_FORMAT")
	envLoaderString(&redis.Addr, "MINIO_REDIS_ADDRESS")
	envLoaderString(&redis.Password, "MINIO_REDIS_PASSWORD")
	envLoaderString(&redis.Key, "MINIO_REDIS_KEY")

	return redis
}

func envConfigNotifyPostgreSQLLoad() envNotifyPostgreSQL {
	postgreSQL := envNotifyPostgreSQL{}

	envLoaderBool(&postgreSQL.Enable, "MINIO_POSTGRESQL_ENABLE")
	envLoaderString(&postgreSQL.Format, "MINIO_POSTGRESQL_FORMAT")
	envLoaderString(&postgreSQL.ConnectionString, "MINIO_POSTGRESQL_CONNECTION_STRING")
	envLoaderString(&postgreSQL.Table, "MINIO_POSTGRESQL_TABLE")
	envLoaderString(&postgreSQL.Host, "MINIO_POSTGRESQL_HOST")
	envLoaderString(&postgreSQL.Port, "MINIO_POSTGRESQL_PORT")
	envLoaderString(&postgreSQL.User, "MINIO_POSTGRESQL_USER")
	envLoaderString(&postgreSQL.Password, "MINIO_POSTGRESQL_PASSWORD")
	envLoaderString(&postgreSQL.Database, "MINIO_POSTGRESQL_DATABASE")

	return postgreSQL
}

func envConfigNotifyKafkaLoad() envNotifyKafka {
	kafka := envNotifyKafka{}

	envLoaderBool(&kafka.Enable, "MINIO_KAFKA_ENABLE")
	envLoaderString(&kafka.Topic, "MINIO_KAFKA_TOPIC")

	return kafka
}

func envConfigNotifyWebhookLoad() envNotifyWebhook {
	webhook := envNotifyWebhook{}

	envLoaderBool(&webhook.Enable, "MINIO_WEBHOOK_ENABLE")
	envLoaderString(&webhook.Endpoint, "MINIO_WEBHOOK_ENDPOINT")

	return webhook
}

func envConfigNotifyMySQLLoad() envNotifyMySQL {
	mySQL := envNotifyMySQL{}

	envLoaderBool(&mySQL.Enable, "MINIO_MYSQL_ENABLE")
	envLoaderString(&mySQL.Format, "MINIO_MYSQL_FORMAT")
	envLoaderString(&mySQL.DsnString, "MINIO_MYSQL_DNS_STRING")
	envLoaderString(&mySQL.Table, "MINIO_MYSQL_TABLE")
	envLoaderString(&mySQL.Host, "MINIO_MYSQL_HOST")
	envLoaderString(&mySQL.Port, "MINIO_MYSQL_PORT")
	envLoaderString(&mySQL.User, "MINIO_MYSQL_USER")
	envLoaderString(&mySQL.Password, "MINIO_MYSQL_PASSWORD")
	envLoaderString(&mySQL.Database, "MINIO_MYSQL_DATABASE")

	return mySQL
}

func envConfigNotifyMQTTLoad() envNotifyMQTT {
	mqtt := envNotifyMQTT{}

	envLoaderBool(&mqtt.Enable, "MINIO_MQTT_ENABLE")
	envLoaderString(&mqtt.Broker, "MINIO_MQTT_BROKER")
	envLoaderString(&mqtt.Topic, "MINIO_MQTT_TOPIC")
	envLoaderInt(&mqtt.QoS, "MINIO_MQTT_QOS")
	envLoaderString(&mqtt.ClientID, "MINIO_MQTT_CLIENT_ID")
	envLoaderString(&mqtt.User, "MINIO_MQTT_USERNAME")
	envLoaderString(&mqtt.Password, "MINIO_MQTT_PASSWORD")

	return mqtt
}

func envConfigApply(cfg *serverConfigV19, envConfig envConfigV19) {
	cfg.Lock()

	envApplyBrowserFlag(&cfg.Browser, envConfig.Browser)
	envApplyCredential(&cfg.Credential, envConfig.Credential)
	envApplyString(&cfg.Region, envConfig.Region)

	if cfg.Notify != nil {
		envConfigInitialiseNotify(cfg.Notify)
		envConfigApplyNotify(cfg.Notify, envConfig.Notify)
	}

	cfg.Unlock()
}

func envConfigInitialiseNotify(notify *notifier) {
	if notify.AMQP == nil {
		notify.AMQP = amqpConfigs{}
	}
	if notify.NATS == nil {
		notify.NATS = natsConfigs{}
	}
	if notify.ElasticSearch == nil {
		notify.ElasticSearch = elasticSearchConfigs{}
	}
	if notify.Redis == nil {
		notify.Redis = redisConfigs{}
	}
	if notify.PostgreSQL == nil {
		notify.PostgreSQL = postgreSQLConfigs{}
	}
	if notify.Kafka == nil {
		notify.Kafka = kafkaConfigs{}
	}
	if notify.Webhook == nil {
		notify.Webhook = webhookConfigs{}
	}
	if notify.MySQL == nil {
		notify.MySQL = mySQLConfigs{}
	}
	if notify.MQTT == nil {
		notify.MQTT = mqttConfigs{}
	}
}

func envConfigApplyNotify(notify *notifier, envNotify envNotifier) {
	notify.AMQP["1"] = envConfigApplyNotifyAMQP(notify.AMQP["1"], envNotify.AMQP)
	notify.NATS["1"] = envConfigApplyNotifyNATS(notify.NATS["1"], envNotify.NATS)
	notify.ElasticSearch["1"] = envConfigApplyNotifyElasticSearch(notify.ElasticSearch["1"], envNotify.ElasticSearch)
	notify.Redis["1"] = envConfigApplyNotifyRedis(notify.Redis["1"], envNotify.Redis)
	notify.PostgreSQL["1"] = envConfigApplyNotifyPostgreSQL(notify.PostgreSQL["1"], envNotify.PostgreSQL)
	notify.Kafka["1"] = envConfigApplyNotifyKafka(notify.Kafka["1"], envNotify.Kafka)
	notify.Webhook["1"] = envConfigApplyNotifyWebhook(notify.Webhook["1"], envNotify.Webhook)
	notify.MySQL["1"] = envConfigApplyNotifyMySQL(notify.MySQL["1"], envNotify.MySQL)
	notify.MQTT["1"] = envConfigApplyNotifyMQTT(notify.MQTT["1"], envNotify.MQTT)
}

func envConfigApplyNotifyAMQP(amqp amqpNotify, envAMQP envNotifyAMQP) amqpNotify {
	envApplyBool(&amqp.Enable, envAMQP.Enable)
	envApplyString(&amqp.URL, envAMQP.URL)
	envApplyString(&amqp.Exchange, envAMQP.Exchange)
	envApplyString(&amqp.RoutingKey, envAMQP.RoutingKey)
	envApplyString(&amqp.ExchangeType, envAMQP.ExchangeType)
	envApplyUint8(&amqp.DeliveryMode, envAMQP.DeliveryMode)
	envApplyBool(&amqp.Mandatory, envAMQP.Mandatory)
	envApplyBool(&amqp.Immediate, envAMQP.Immediate)
	envApplyBool(&amqp.Durable, envAMQP.Durable)
	envApplyBool(&amqp.Internal, envAMQP.Internal)
	envApplyBool(&amqp.NoWait, envAMQP.NoWait)
	envApplyBool(&amqp.AutoDeleted, envAMQP.AutoDeleted)

	return amqp
}

func envConfigApplyNotifyNATS(nats natsNotify, envNATS envNotifyNATS) natsNotify {
	envApplyBool(&nats.Enable, envNATS.Enable)
	envApplyString(&nats.Address, envNATS.Address)
	envApplyString(&nats.Subject, envNATS.Subject)
	envApplyString(&nats.Username, envNATS.Username)
	envApplyString(&nats.Password, envNATS.Password)
	envApplyString(&nats.Token, envNATS.Token)
	envApplyBool(&nats.Secure, envNATS.Secure)
	envApplyInt64(&nats.PingInterval, envNATS.PingInterval)

	nats.Streaming = envConfigApplyNotifyNATSStreaming(nats.Streaming, envNATS.Streaming)

	return nats
}

func envConfigApplyNotifyNATSStreaming(natsStreaming natsNotifyStreaming, envNATSStreaming envNotifyNATSStreaming) natsNotifyStreaming {
	envApplyBool(&natsStreaming.Enable, envNATSStreaming.Enable)
	envApplyString(&natsStreaming.ClusterID, envNATSStreaming.ClusterID)
	envApplyString(&natsStreaming.ClientID, envNATSStreaming.ClientID)
	envApplyBool(&natsStreaming.Async, envNATSStreaming.Async)
	envApplyInt(&natsStreaming.MaxPubAcksInflight, envNATSStreaming.MaxPubAcksInflight)

	return natsStreaming
}

func envConfigApplyNotifyElasticSearch(elasticSearch elasticSearchNotify, envElasticSearch envNotifyElasticSearch) elasticSearchNotify {
	envApplyBool(&elasticSearch.Enable, envElasticSearch.Enable)
	envApplyString(&elasticSearch.Format, envElasticSearch.Format)
	envApplyString(&elasticSearch.URL, envElasticSearch.URL)
	envApplyString(&elasticSearch.Index, envElasticSearch.Index)

	return elasticSearch
}

func envConfigApplyNotifyRedis(redis redisNotify, envRedis envNotifyRedis) redisNotify {
	envApplyBool(&redis.Enable, envRedis.Enable)
	envApplyString(&redis.Format, envRedis.Format)
	envApplyString(&redis.Addr, envRedis.Addr)
	envApplyString(&redis.Password, envRedis.Password)
	envApplyString(&redis.Key, envRedis.Key)

	return redis
}

func envConfigApplyNotifyPostgreSQL(postgreSQL postgreSQLNotify, envPostgreSQL envNotifyPostgreSQL) postgreSQLNotify {
	envApplyBool(&postgreSQL.Enable, envPostgreSQL.Enable)
	envApplyString(&postgreSQL.Format, envPostgreSQL.Format)
	envApplyString(&postgreSQL.ConnectionString, envPostgreSQL.ConnectionString)
	envApplyString(&postgreSQL.Table, envPostgreSQL.Table)
	envApplyString(&postgreSQL.Host, envPostgreSQL.Host)
	envApplyString(&postgreSQL.Port, envPostgreSQL.Port)
	envApplyString(&postgreSQL.User, envPostgreSQL.User)
	envApplyString(&postgreSQL.Password, envPostgreSQL.Password)
	envApplyString(&postgreSQL.Database, envPostgreSQL.Database)

	return postgreSQL
}

func envConfigApplyNotifyKafka(kafka kafkaNotify, envKafka envNotifyKafka) kafkaNotify {
	envApplyBool(&kafka.Enable, envKafka.Enable)
	envApplyString(&kafka.Topic, envKafka.Topic)

	return kafka
}

func envConfigApplyNotifyWebhook(webhook webhookNotify, envWebhook envNotifyWebhook) webhookNotify {
	envApplyBool(&webhook.Enable, envWebhook.Enable)
	envApplyString(&webhook.Endpoint, envWebhook.Endpoint)

	return webhook
}

func envConfigApplyNotifyMySQL(mySQL mySQLNotify, envMySQL envNotifyMySQL) mySQLNotify {
	envApplyBool(&mySQL.Enable, envMySQL.Enable)
	envApplyString(&mySQL.Format, envMySQL.Format)
	envApplyString(&mySQL.DsnString, envMySQL.DsnString)
	envApplyString(&mySQL.Table, envMySQL.Table)
	envApplyString(&mySQL.Host, envMySQL.Host)
	envApplyString(&mySQL.Port, envMySQL.Port)
	envApplyString(&mySQL.User, envMySQL.User)
	envApplyString(&mySQL.Password, envMySQL.Password)
	envApplyString(&mySQL.Database, envMySQL.Database)

	return mySQL
}

func envConfigApplyNotifyMQTT(mqtt mqttNotify, envMQTT envNotifyMQTT) mqttNotify {
	envApplyBool(&mqtt.Enable, envMQTT.Enable)
	envApplyString(&mqtt.Broker, envMQTT.Broker)
	envApplyString(&mqtt.Topic, envMQTT.Topic)
	envApplyInt(&mqtt.QoS, envMQTT.QoS)
	envApplyString(&mqtt.ClientID, envMQTT.ClientID)
	envApplyString(&mqtt.User, envMQTT.User)
	envApplyString(&mqtt.Password, envMQTT.Password)

	return mqtt
}

func envApplyBrowserFlag(dst, src *BrowserFlag) {
	if src != nil {
		*dst = *src
	}
}

func envApplyCredential(dst, src *credential) {
	if src != nil {
		*dst = *src
	}
}

func envApplyBool(dst, src *bool) {
	if src != nil {
		*dst = *src
	}
}

func envApplyUint8(dst, src *uint8) {
	if src != nil {
		*dst = *src
	}
}

func envApplyInt64(dst, src *int64) {
	if src != nil {
		*dst = *src
	}
}

func envApplyInt(dst, src *int) {
	if src != nil {
		*dst = *src
	}
}

func envApplyString(dst, src *string) {
	if src != nil {
		*dst = *src
	}
}

func envLoaderBool(target **bool, envVarName string) {
	env := os.Getenv(envVarName)
	if env == "" {
		return
	}

	b, err := strconv.ParseBool(env)
	fatalIf(err, "Invalid "+envVarName+" value, expected boolean")

	*target = &b
}

func envLoaderUint8(target **uint8, envVarName string) {
	env := os.Getenv(envVarName)
	if env == "" {
		return
	}

	u64, err := strconv.ParseUint(env, 10, 8)
	fatalIf(err, "Invalid "+envVarName+" value, expected uint8")

	u8 := uint8(u64)
	*target = &u8
}

func envLoaderInt64(target **int64, envVarName string) {
	env := os.Getenv(envVarName)
	if env == "" {
		return
	}

	i64, err := strconv.ParseInt(env, 10, 64)
	fatalIf(err, "Invalid "+envVarName+" value, expected int64")

	*target = &i64
}

func envLoaderInt(target **int, envVarName string) {
	env := os.Getenv(envVarName)
	if env == "" {
		return
	}

	i64, err := strconv.ParseInt(env, 10, 64)
	fatalIf(err, "Invalid "+envVarName+" value, expected int")

	i := int(i64)
	if int64(i) != i64 {
		err := errors.New("Integer overflow")
		fatalIf(err, "Invalid "+envVarName+" value, expected int")
	}

	*target = &i
}

func envLoaderString(target **string, envVarName string) {
	env := os.Getenv(envVarName)
	if env == "" {
		return
	}

	*target = &env
}
