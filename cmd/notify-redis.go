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
	"encoding/json"
	"io/ioutil"
	"net"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
)

var (
	redisErrFunc = newNotificationErrorFactory("Redis")

	errRedisFormat   = redisErrFunc(`"format" value is invalid - it must be one of "access" or "namespace".`)
	errRedisKeyError = redisErrFunc("Key was not specified in the configuration.")
)

// redisNotify to send logs to Redis server
type redisNotify struct {
	Enable   bool   `json:"enable"`
	Format   string `json:"format"`
	Addr     string `json:"address"`
	Password string `json:"password"`
	Key      string `json:"key"`
}

func (r *redisNotify) Validate() error {
	if !r.Enable {
		return nil
	}
	if r.Format != formatNamespace && r.Format != formatAccess {
		return errRedisFormat
	}
	if _, _, err := net.SplitHostPort(r.Addr); err != nil {
		return err
	}
	if r.Key == "" {
		return errRedisKeyError
	}
	return nil
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
		return nil, redisErrFunc("Error connecting to server: %v", err)
	}

	// Test that Key is of desired type
	reply, err := redis.String(rConn.Do("TYPE", rNotify.Key))
	if err != nil {
		return nil, redisErrFunc("Error getting type of Key=%s: %v",
			rNotify.Key, err)
	}
	if reply != "none" {
		expectedType := "hash"
		if rNotify.Format == formatAccess {
			expectedType = "list"
		}
		if reply != expectedType {
			return nil, redisErrFunc(
				"Key=%s has type %s, but we expect it to be a %s",
				rNotify.Key, reply, expectedType)
		}
	}

	// Return pool.
	return rPool, nil
}

func newRedisNotify(accountID string) (*logrus.Logger, error) {
	rNotify := globalServerConfig.Notify.GetRedisByID(accountID)

	// Dial redis.
	rPool, err := dialRedis(rNotify)
	if err != nil {
		return nil, redisErrFunc("Error dialing server: %v", err)
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

	switch r.params.Format {
	case formatNamespace:
		// Match the event if its a delete request, attempt to delete the key
		if eventMatch(entryStr, []string{"s3:ObjectRemoved:*"}) {
			_, err := rConn.Do("HDEL", r.params.Key, entry.Data["Key"])
			if err != nil {
				return redisErrFunc("Error deleting entry: %v",
					err)
			}
			return nil
		} // else save this as new entry or update any existing ones.

		value, err := json.Marshal(map[string]interface{}{
			"Records": entry.Data["Records"],
		})
		if err != nil {
			return redisErrFunc(
				"Unable to encode event %v to JSON: %v",
				entry.Data["Records"], err)
		}
		_, err = rConn.Do("HSET", r.params.Key, entry.Data["Key"],
			value)
		if err != nil {
			return redisErrFunc("Error updating hash entry: %v",
				err)
		}
	case formatAccess:
		// eventTime is taken from the first entry in the
		// records.
		events, ok := entry.Data["Records"].([]NotificationEvent)
		if !ok {
			return redisErrFunc("unable to extract event time due to conversion error of entry.Data[\"Records\"]=%v", entry.Data["Records"])
		}
		eventTime := events[0].EventTime

		listEntry := []interface{}{eventTime, entry.Data["Records"]}
		jsonValue, err := json.Marshal(listEntry)
		if err != nil {
			return redisErrFunc("JSON encoding error: %v", err)
		}
		_, err = rConn.Do("RPUSH", r.params.Key, jsonValue)
		if err != nil {
			return redisErrFunc("Error appending to Redis list: %v",
				err)
		}
	}
	return nil
}

// Required for logrus hook implementation
func (r redisConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
