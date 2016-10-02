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
	"io/ioutil"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/minio/redigo/redis"
)

// redisNotify to send logs to Redis server
type redisNotify struct {
	Enable   bool   `json:"enable"`
	Addr     string `json:"address"`
	Password string `json:"password"`
	Key      string `json:"key"`
}

type redisConn struct {
	*redis.Pool
	params redisNotify
}

// Dial a new connection to redis instance at addr, optionally with a
// password if any.
func dialRedis(rNotify redisNotify) (*redis.Pool, error) {
	// Return error if redis not enabled.
	if !rNotify.Enable {
		return nil, errNotifyNotEnabled
	}
	addr := rNotify.Addr
	password := rNotify.Password
	rPool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second, // Time 2minutes.
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, derr := c.Do("AUTH", password); derr != nil {
					c.Close()
					return nil, derr
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

func newRedisNotify(accountID string) (*logrus.Logger, error) {
	rNotify := serverConfig.GetRedisNotifyByID(accountID)

	// Dial redis.
	rPool, err := dialRedis(rNotify)
	if err != nil {
		return nil, err
	}

	rrConn := redisConn{
		Pool:   rPool,
		params: rNotify,
	}

	redisLog := logrus.New()

	redisLog.Out = ioutil.Discard

	// Set default JSON formatter.
	redisLog.Formatter = new(logrus.JSONFormatter)

	redisLog.Hooks.Add(rrConn)

	// Success, redis enabled.
	return redisLog, nil
}

// Fire is called when an event should be sent to the message broker.
func (r redisConn) Fire(entry *logrus.Entry) error {
	rConn := r.Pool.Get()
	defer rConn.Close()

	// Fetch event type upon reflecting on its original type.
	entryStr, ok := entry.Data["EventType"].(string)
	if !ok {
		return nil
	}

	// Match the event if its a delete request, attempt to delete the key
	if eventMatch(entryStr, []string{"s3:ObjectRemoved:*"}) {
		if _, err := rConn.Do("DEL", entry.Data["Key"]); err != nil {
			return err
		}
		return nil
	} // else save this as new entry or update any existing ones.
	if _, err := rConn.Do("SET", entry.Data["Key"], map[string]interface{}{
		"Records": entry.Data["Records"],
	}); err != nil {
		return err
	}
	return nil
}

// Required for logrus hook implementation
func (r redisConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
