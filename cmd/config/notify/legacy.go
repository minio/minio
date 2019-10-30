package notify

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/event/target"
)

// SetNotifyKafka - helper for config migration from older config.
func SetNotifyKafka(s config.Config, kName string, cfg target.KafkaArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyKafkaSubSys][kName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		target.KafkaBrokers: func() string {
			var brokers []string
			for _, broker := range cfg.Brokers {
				brokers = append(brokers, broker.String())
			}
			return strings.Join(brokers, config.ValueSeparator)
		}(),
		config.Comment:            "Settings for Kafka notification, after migrating config",
		target.KafkaTopic:         cfg.Topic,
		target.KafkaQueueDir:      cfg.QueueDir,
		target.KafkaQueueLimit:    strconv.Itoa(int(cfg.QueueLimit)),
		target.KafkaTLSEnable:     config.FormatBool(cfg.TLS.Enable),
		target.KafkaTLSSkipVerify: config.FormatBool(cfg.TLS.SkipVerify),
		target.KafkaTLSClientAuth: strconv.Itoa(int(cfg.TLS.ClientAuth)),
		target.KafkaSASLEnable:    config.FormatBool(cfg.SASL.Enable),
		target.KafkaSASLUsername:  cfg.SASL.User,
		target.KafkaSASLPassword:  cfg.SASL.Password,
	}
	return nil
}

// SetNotifyAMQP - helper for config migration from older config.
func SetNotifyAMQP(s config.Config, amqpName string, cfg target.AMQPArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyAMQPSubSys][amqpName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:          "Settings for AMQP notification, after migrating config",
		target.AmqpURL:          cfg.URL.String(),
		target.AmqpExchange:     cfg.Exchange,
		target.AmqpRoutingKey:   cfg.RoutingKey,
		target.AmqpExchangeType: cfg.ExchangeType,
		target.AmqpDeliveryMode: strconv.Itoa(int(cfg.DeliveryMode)),
		target.AmqpMandatory:    config.FormatBool(cfg.Mandatory),
		target.AmqpInternal:     config.FormatBool(cfg.Immediate),
		target.AmqpDurable:      config.FormatBool(cfg.Durable),
		target.AmqpNoWait:       config.FormatBool(cfg.NoWait),
		target.AmqpAutoDeleted:  config.FormatBool(cfg.AutoDeleted),
		target.AmqpQueueDir:     cfg.QueueDir,
		target.AmqpQueueLimit:   strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

// SetNotifyES - helper for config migration from older config.
func SetNotifyES(s config.Config, esName string, cfg target.ElasticsearchArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyESSubSys][esName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:           "Settings for Elasticsearch notification, after migrating config",
		target.ElasticFormat:     cfg.Format,
		target.ElasticURL:        cfg.URL.String(),
		target.ElasticIndex:      cfg.Index,
		target.ElasticQueueDir:   cfg.QueueDir,
		target.ElasticQueueLimit: strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

// SetNotifyRedis - helper for config migration from older config.
func SetNotifyRedis(s config.Config, redisName string, cfg target.RedisArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyRedisSubSys][redisName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:         "Settings for Redis notification, after migrating config",
		target.RedisFormat:     cfg.Format,
		target.RedisAddress:    cfg.Addr.String(),
		target.RedisPassword:   cfg.Password,
		target.RedisKey:        cfg.Key,
		target.RedisQueueDir:   cfg.QueueDir,
		target.RedisQueueLimit: strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

// SetNotifyWebhook - helper for config migration from older config.
func SetNotifyWebhook(s config.Config, whName string, cfg target.WebhookArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyWebhookSubSys][whName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:           "Settings for Webhook notification, after migrating config",
		target.WebhookEndpoint:   cfg.Endpoint.String(),
		target.WebhookAuthToken:  cfg.AuthToken,
		target.WebhookQueueDir:   cfg.QueueDir,
		target.WebhookQueueLimit: strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

// SetNotifyPostgres - helper for config migration from older config.
func SetNotifyPostgres(s config.Config, psqName string, cfg target.PostgreSQLArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyPostgresSubSys][psqName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:                  "Settings for Postgres notification, after migrating config",
		target.PostgresFormat:           cfg.Format,
		target.PostgresConnectionString: cfg.ConnectionString,
		target.PostgresTable:            cfg.Table,
		target.PostgresHost:             cfg.Host.String(),
		target.PostgresPort:             cfg.Port,
		target.PostgresUsername:         cfg.User,
		target.PostgresPassword:         cfg.Password,
		target.PostgresDatabase:         cfg.Database,
		target.PostgresQueueDir:         cfg.QueueDir,
		target.PostgresQueueLimit:       strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

// SetNotifyNSQ - helper for config migration from older config.
func SetNotifyNSQ(s config.Config, nsqName string, cfg target.NSQArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyNSQSubSys][nsqName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:          "Settings for NSQ notification, after migrating config",
		target.NSQAddress:       cfg.NSQDAddress.String(),
		target.NSQTopic:         cfg.Topic,
		target.NSQTLSEnable:     config.FormatBool(cfg.TLS.Enable),
		target.NSQTLSSkipVerify: config.FormatBool(cfg.TLS.SkipVerify),
		target.NSQQueueDir:      cfg.QueueDir,
		target.NSQQueueLimit:    strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

// SetNotifyNATS - helper for config migration from older config.
func SetNotifyNATS(s config.Config, natsName string, cfg target.NATSArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyNATSSubSys][natsName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:          "Settings for NATS notification, after migrating config",
		target.NATSAddress:      cfg.Address.String(),
		target.NATSSubject:      cfg.Subject,
		target.NATSUsername:     cfg.Username,
		target.NATSPassword:     cfg.Password,
		target.NATSToken:        cfg.Token,
		target.NATSSecure:       config.FormatBool(cfg.Secure),
		target.NATSPingInterval: strconv.FormatInt(cfg.PingInterval, 10),
		target.NATSQueueDir:     cfg.QueueDir,
		target.NATSQueueLimit:   strconv.Itoa(int(cfg.QueueLimit)),
		target.NATSStreamingEnable: func() string {
			if cfg.Streaming.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		target.NATSStreamingClusterID:          cfg.Streaming.ClusterID,
		target.NATSStreamingAsync:              config.FormatBool(cfg.Streaming.Async),
		target.NATSStreamingMaxPubAcksInFlight: strconv.Itoa(cfg.Streaming.MaxPubAcksInflight),
	}

	return nil
}

// SetNotifyMySQL - helper for config migration from older config.
func SetNotifyMySQL(s config.Config, sqlName string, cfg target.MySQLArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyMySQLSubSys][sqlName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:         "Settings for MySQL notification, after migrating config",
		target.MySQLFormat:     cfg.Format,
		target.MySQLDSNString:  cfg.DSN,
		target.MySQLTable:      cfg.Table,
		target.MySQLHost:       cfg.Host.String(),
		target.MySQLPort:       cfg.Port,
		target.MySQLUsername:   cfg.User,
		target.MySQLPassword:   cfg.Password,
		target.MySQLDatabase:   cfg.Database,
		target.MySQLQueueDir:   cfg.QueueDir,
		target.MySQLQueueLimit: strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}

// SetNotifyMQTT - helper for config migration from older config.
func SetNotifyMQTT(s config.Config, mqttName string, cfg target.MQTTArgs) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	s[config.NotifyMQTTSubSys][mqttName] = config.KVS{
		config.State: func() string {
			if cfg.Enable {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment:               "Settings for MQTT notification, after migrating config",
		target.MqttBroker:            cfg.Broker.String(),
		target.MqttTopic:             cfg.Topic,
		target.MqttQoS:               fmt.Sprintf("%d", cfg.QoS),
		target.MqttUsername:          cfg.User,
		target.MqttPassword:          cfg.Password,
		target.MqttReconnectInterval: cfg.MaxReconnectInterval.String(),
		target.MqttKeepAliveInterval: cfg.KeepAlive.String(),
		target.MqttQueueDir:          cfg.QueueDir,
		target.MqttQueueLimit:        strconv.Itoa(int(cfg.QueueLimit)),
	}

	return nil
}
