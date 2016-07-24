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

package main

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/minio/redigo/redis"
)

// redisLogger to send logs to Redis server
type redisLogger struct {
	Enable   bool   `json:"enable"`
	Level    string `json:"level"`
	Addr     string `json:"address"`
	Password string `json:"password"`
	Key      string `json:"key"`
}

type redisConn struct {
	*redis.Pool
	params redisLogger
}

// Dial a new connection to redis instance at addr, optionally with a password if any.
func dialRedis(rLogger redisLogger) (*redis.Pool, error) {
	// Return error if redis not enabled.
	if !rLogger.Enable {
		return nil, errLoggerNotEnabled
	}
	addr := rLogger.Addr
	password := rLogger.Password
	rPool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	// Test if connection with REDIS can be established.
	rConn := rPool.Get()
	defer rConn.Close()

	// Check connection.
	_, err := rConn.Do("PING")
	if err != nil {
		return nil, err
	}

	// Return pool.
	return rPool, nil
}

func enableRedisLogger() error {
	rLogger := serverConfig.GetRedisLogger()

	// Dial redis.
	rPool, err := dialRedis(rLogger)
	if err != nil {
		return err
	}

	rrConn := redisConn{
		Pool:   rPool,
		params: rLogger,
	}

	lvl, err := logrus.ParseLevel(rLogger.Level)
	fatalIf(err, "Unknown log level found in the config file.")

	// Add a elasticsearch hook.
	log.Hooks.Add(rrConn)

	// Set default JSON formatter.
	log.Formatter = new(logrus.JSONFormatter)

	// Set default log level to info.
	log.Level = lvl

	return nil
}

func (r redisConn) Fire(entry *logrus.Entry) error {
	rConn := r.Pool.Get()
	defer rConn.Close()

	data, err := entry.String()
	if err != nil {
		return err
	}

	_, err = rConn.Do("RPUSH", r.params.Key, data)
	if err != nil {
		return err
	}
	return nil
}

// Required for logrus hook implementation
func (r redisConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
