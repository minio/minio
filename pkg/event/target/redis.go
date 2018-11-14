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
 */

package target

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// RedisArgs - Redis target arguments.
type RedisArgs struct {
	Enable   bool      `json:"enable"`
	Format   string    `json:"format"`
	Addr     xnet.Host `json:"address"`
	Password string    `json:"password"`
	Key      string    `json:"key"`
}

// Validate RedisArgs fields
func (r RedisArgs) Validate() error {
	if !r.Enable {
		return nil
	}

	if r.Format != "" {
		f := strings.ToLower(r.Format)
		if f != event.NamespaceFormat && f != event.AccessFormat {
			return fmt.Errorf("unrecognized format")
		}
	}

	if r.Key == "" {
		return fmt.Errorf("empty key")
	}

	return nil
}

// RedisTarget - Redis target.
type RedisTarget struct {
	id   event.TargetID
	args RedisArgs
	pool *redis.Pool
}

// ID - returns target ID.
func (target *RedisTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to Redis.
func (target *RedisTarget) Send(eventData event.Event) error {
	conn := target.pool.Get()
	defer func() {
		// FIXME: log returned error. ignore time being.
		_ = conn.Close()
	}()

	if target.args.Format == event.NamespaceFormat {
		objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
		if err != nil {
			return err
		}
		key := eventData.S3.Bucket.Name + "/" + objectName

		if eventData.EventName == event.ObjectRemovedDelete {
			_, err = conn.Do("HDEL", target.args.Key, key)
		} else {
			var data []byte
			if data, err = json.Marshal(struct{ Records []event.Event }{[]event.Event{eventData}}); err != nil {
				return err
			}

			_, err = conn.Do("HSET", target.args.Key, key, data)
		}
		return err
	}

	if target.args.Format == event.AccessFormat {
		data, err := json.Marshal([]interface{}{eventData.EventTime, []event.Event{eventData}})
		if err != nil {
			return err
		}
		_, err = conn.Do("RPUSH", target.args.Key, data)
		return err
	}

	return nil
}

// Close - does nothing and available for interface compatibility.
func (target *RedisTarget) Close() error {
	return nil
}

// NewRedisTarget - creates new Redis target.
func NewRedisTarget(id string, args RedisArgs) (*RedisTarget, error) {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 2 * 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", args.Addr.String())
			if err != nil {
				return nil, err
			}

			if args.Password == "" {
				return conn, nil
			}

			if _, err = conn.Do("AUTH", args.Password); err != nil {
				// FIXME: log returned error. ignore time being.
				_ = conn.Close()
				return nil, err
			}

			return conn, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	conn := pool.Get()
	defer func() {
		// FIXME: log returned error. ignore time being.
		_ = conn.Close()
	}()

	if _, err := conn.Do("PING"); err != nil {
		return nil, err
	}

	typeAvailable, err := redis.String(conn.Do("TYPE", args.Key))
	if err != nil {
		return nil, err
	}

	if typeAvailable != "none" {
		expectedType := "hash"
		if args.Format == event.AccessFormat {
			expectedType = "list"
		}

		if typeAvailable != expectedType {
			return nil, fmt.Errorf("expected type %v does not match with available type %v", expectedType, typeAvailable)
		}
	}

	return &RedisTarget{
		id:   event.TargetID{ID: id, Name: "redis"},
		args: args,
		pool: pool,
	}, nil
}
