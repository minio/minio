/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/minio/minio/pkg/probe"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// mongoDB collection
type mongoDB struct {
	c *mgo.Collection
}

func log2Mongo(url, db, collection string) *probe.Error {
	mongoHook, e := newMongo(url, db, collection)
	if e != nil {
		return probe.NewError(e)
	}
	log.Hooks.Add(mongoHook)                // Add mongodb hook.
	log.Formatter = &logrus.JSONFormatter{} // JSON formatted log.
	log.Level = logrus.InfoLevel            // Minimum log level.
	return nil
}

// newMongo -
func newMongo(mgoEndpoint, db, collection string) (*mongoDB, error) {
	session, err := mgo.Dial(mgoEndpoint)
	if err != nil {
		return nil, err
	}
	return &mongoDB{c: session.DB(db).C(collection)}, nil
}

// Fire - the log event
func (h *mongoDB) Fire(entry *logrus.Entry) error {
	entry.Data["Level"] = entry.Level.String()
	entry.Data["Time"] = entry.Time
	entry.Data["Message"] = entry.Message
	mgoErr := h.c.Insert(bson.M(entry.Data))
	if mgoErr != nil {
		return fmt.Errorf("Failed to send log entry to mongodb: %s", mgoErr)
	}
	return nil
}

// Levels -
func (h *mongoDB) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
