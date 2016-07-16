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
	"errors"
	"net/url"

	"github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v3"
)

const (
	arnElasticQueue = "1:elasticsearch:"
)

// Returns true if queueArn is for an ElasticSearch queue.
func isElasticQueue(sqsArn minioSqsArn) bool {
	if sqsArn.sqsType == arnElasticQueue {
		// Sets to 'true' if total fields available are equal to expected number.
		return queueInputFields[arnElasticQueue] == len(sqsArn.inputs)
	}
	return false
}

// elasticQueue is a elasticsearch event notification queue.
type elasticQueue struct {
	client *elastic.Client
	url    elastic.ClientOptionFunc
	index  string
}

func connectElasticQueue(elasticParams []string) (*elastic.Client, elastic.ClientOptionFunc, error) {
	var url = new(url.URL)
	url.Host = elasticParams[0] + ":" + elasticParams[1]
	url.Scheme = elasticParams[2]
	eURL := elastic.SetURL(url.String())

	client, err := elastic.NewClient(eURL, elastic.SetSniff(false))
	if err != nil {
		return nil, nil, err
	}
	return client, eURL, nil
}

// enables elasticsearch queue.
func enableElasticQueue(elasticParams []string) error {
	client, eURL, err := connectElasticQueue(elasticParams)
	if err != nil {
		return err
	}

	// Elastic index.
	eIndex := elasticParams[3]

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(eIndex).Do()
	if err != nil {
		return err
	}
	if !exists {
		createIndex, err := client.CreateIndex(eIndex).Do()
		if err != nil {
			return err
		}
		if !createIndex.Acknowledged {
			return errors.New("cannot create index")
		}
	}

	elasticQ := &elasticQueue{
		client: client,
		url:    eURL,
		index:  eIndex,
	}

	// Add a elasticsearch hook.
	log.Hooks.Add(elasticQ)

	// Set default JSON formatter.
	log.Formatter = new(logrus.JSONFormatter)

	// Set default log level to info.
	log.Level = logrus.InfoLevel

	return nil
}

// Fire is required to implement logrus hook
func (q *elasticQueue) Fire(entry *logrus.Entry) error {
	_, err := q.client.Index().Index(q.index).
		Type("event").
		BodyJson(entry.Data).
		Do()

	return err
}

// Required for logrus hook implementation
func (q elasticQueue) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
