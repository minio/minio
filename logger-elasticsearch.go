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

	"github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v3"
)

// elasticQueue is a elasticsearch event notification queue.
type elasticSearchLogger struct {
	Enable bool   `json:"enable"`
	Level  string `json:"level"`
	URL    string `json:"url"`
	Index  string `json:"index"`
}

type elasticClient struct {
	*elastic.Client
	params elasticSearchLogger
}

// Connects to elastic search instance at URL.
func dialElastic(url string) (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetURL(url), elastic.SetSniff(false))
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Enables elasticsearch logger.
func enableElasticLogger() error {
	esLogger := serverConfig.GetElasticSearchLogger()
	if !esLogger.Enable {
		return errLoggerNotEnabled
	}
	client, err := dialElastic(esLogger.URL)
	if err != nil {
		return err
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(esLogger.Index).Do()
	if err != nil {
		return err
	}
	// Index does not exist, attempt to create it.
	if !exists {
		var createIndex *elastic.IndicesCreateResult
		createIndex, err = client.CreateIndex(esLogger.Index).Do()
		if err != nil {
			return err
		}
		if !createIndex.Acknowledged {
			return errors.New("index not created")
		}
	}

	elasticCl := elasticClient{
		Client: client,
		params: esLogger,
	}

	lvl, err := logrus.ParseLevel(esLogger.Level)
	fatalIf(err, "Unknown log level found in the config file.")

	// Add a elasticsearch hook.
	log.Hooks.Add(elasticCl)

	// Set default JSON formatter.
	log.Formatter = new(logrus.JSONFormatter)

	// Set default log level to info.
	log.Level = lvl

	return nil
}

// Fire is required to implement logrus hook
func (q elasticClient) Fire(entry *logrus.Entry) error {
	_, err := q.Client.Index().Index(q.params.Index).
		Type("event").
		BodyJson(entry.Data).
		Do()

	return err
}

// Required for logrus hook implementation
func (q elasticClient) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
