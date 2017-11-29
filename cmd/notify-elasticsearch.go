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
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

var (
	esErrFunc = newNotificationErrorFactory("Elasticsearch")

	errESFormat = esErrFunc(`"format" value is invalid - it must be one of "%s" or "%s".`, formatNamespace, formatAccess)
	errESIndex  = esErrFunc("Index name was not specified in the configuration.")
)

// elasticQueue is a elasticsearch event notification queue.
type elasticSearchNotify struct {
	Enable bool   `json:"enable"`
	Format string `json:"format"`
	URL    string `json:"url"`
	Index  string `json:"index"`
}

func (e *elasticSearchNotify) Validate() error {
	if !e.Enable {
		return nil
	}
	if e.Format != formatNamespace && e.Format != formatAccess {
		return errESFormat
	}
	if _, err := checkURL(e.URL); err != nil {
		return err
	}
	if e.Index == "" {
		return errESIndex
	}
	return nil
}

type elasticClient struct {
	*elastic.Client
	params elasticSearchNotify
}

// Connects to elastic search instance at URL.
func dialElastic(esNotify elasticSearchNotify) (*elastic.Client, error) {
	if !esNotify.Enable {
		return nil, errNotifyNotEnabled
	}
	return elastic.NewClient(
		elastic.SetURL(esNotify.URL),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(10),
	)
}

func newElasticNotify(accountID string) (*logrus.Logger, error) {
	esNotify := globalServerConfig.Notify.GetElasticSearchByID(accountID)

	// Dial to elastic search.
	client, err := dialElastic(esNotify)
	if err != nil {
		return nil, esErrFunc("Error dialing the server: %v", err)
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(esNotify.Index).
		Do(context.Background())
	if err != nil {
		return nil, esErrFunc("Error checking if index exists: %v", err)
	}
	// Index does not exist, attempt to create it.
	if !exists {
		var createIndex *elastic.IndicesCreateResult
		createIndex, err = client.CreateIndex(esNotify.Index).
			Do(context.Background())
		if err != nil {
			return nil, esErrFunc("Error creating index `%s`: %v",
				esNotify.Index, err)
		}
		if !createIndex.Acknowledged {
			return nil, esErrFunc("Index not created")
		}
	}

	elasticCl := elasticClient{
		Client: client,
		params: esNotify,
	}

	elasticSearchLog := logrus.New()

	// Disable writing to console.
	elasticSearchLog.Out = ioutil.Discard

	// Add a elasticSearch hook.
	elasticSearchLog.Hooks.Add(elasticCl)

	// Set default JSON formatter.
	elasticSearchLog.Formatter = new(logrus.JSONFormatter)

	// Success, elastic search successfully initialized.
	return elasticSearchLog, nil
}

// Fire is required to implement logrus hook
func (q elasticClient) Fire(entry *logrus.Entry) (err error) {
	// Reflect on eventType and Key on their native type.
	entryStr, ok := entry.Data["EventType"].(string)
	if !ok {
		return nil
	}
	keyStr, ok := entry.Data["Key"].(string)
	if !ok {
		return nil
	}

	switch q.params.Format {
	case formatNamespace:
		// If event matches as delete, we purge the previous index.
		if eventMatch(entryStr, []string{"s3:ObjectRemoved:*"}) {
			_, err = q.Client.Delete().Index(q.params.Index).
				Type("event").Id(keyStr).Do(context.Background())
			break
		} // else we update elastic index or create a new one.
		_, err = q.Client.Index().Index(q.params.Index).
			Type("event").
			BodyJson(map[string]interface{}{
				"Records": entry.Data["Records"],
			}).Id(keyStr).Do(context.Background())
	case formatAccess:
		// eventTime is taken from the first entry in the
		// records.
		events, ok := entry.Data["Records"].([]NotificationEvent)
		if !ok {
			return esErrFunc("Unable to extract event time due to conversion error of entry.Data[\"Records\"]=%v", entry.Data["Records"])
		}
		var eventTime time.Time
		eventTime, err = time.Parse(timeFormatAMZ, events[0].EventTime)
		if err != nil {
			return esErrFunc("Unable to parse event time \"%s\": %v",
				events[0].EventTime, err)
		}
		// Extract event time in milliseconds for Elasticsearch.
		eventTimeStr := fmt.Sprintf("%d", eventTime.UnixNano()/1000000)
		_, err = q.Client.Index().Index(q.params.Index).Type("event").
			Timestamp(eventTimeStr).
			BodyJson(map[string]interface{}{
				"Records": entry.Data["Records"],
			}).Do(context.Background())
	}
	if err != nil {
		return esErrFunc("Error inserting/deleting entry: %v", err)
	}
	return nil
}

// Required for logrus hook implementation
func (q elasticClient) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
