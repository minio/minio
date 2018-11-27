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
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"

	"gopkg.in/olivere/elastic.v5"
)

// ElasticsearchArgs - Elasticsearch target arguments.
type ElasticsearchArgs struct {
	Enable bool     `json:"enable"`
	Format string   `json:"format"`
	URL    xnet.URL `json:"url"`
	Index  string   `json:"index"`
}

// Validate ElasticsearchArgs fields
func (a ElasticsearchArgs) Validate() error {
	if !a.Enable {
		return nil
	}
	if a.URL.IsEmpty() {
		return errors.New("empty URL")
	}
	if a.Format != "" {
		f := strings.ToLower(a.Format)
		if f != event.NamespaceFormat && f != event.AccessFormat {
			return errors.New("format value unrecognized")
		}
	}
	if a.Index == "" {
		return errors.New("empty index value")
	}
	return nil
}

// ElasticsearchTarget - Elasticsearch target.
type ElasticsearchTarget struct {
	id     event.TargetID
	args   ElasticsearchArgs
	client *elastic.Client
}

// ID - returns target ID.
func (target *ElasticsearchTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to Elasticsearch.
func (target *ElasticsearchTarget) Send(eventData event.Event) error {
	var key string

	remove := func() error {
		_, err := target.client.Delete().Index(target.args.Index).Type("event").Id(key).Do(context.Background())
		return err
	}

	update := func() error {
		_, err := target.client.Index().Index(target.args.Index).Type("event").BodyJson(map[string]interface{}{"Records": []event.Event{eventData}}).Id(key).Do(context.Background())
		return err
	}

	add := func() error {
		_, err := target.client.Index().Index(target.args.Index).Type("event").BodyJson(map[string]interface{}{"Records": []event.Event{eventData}}).Do(context.Background())
		return err
	}

	if target.args.Format == event.NamespaceFormat {
		objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
		if err != nil {
			return err
		}

		key = eventData.S3.Bucket.Name + "/" + objectName
		if eventData.EventName == event.ObjectRemovedDelete {
			err = remove()
		} else {
			err = update()
		}

		return err
	}

	if target.args.Format == event.AccessFormat {
		return add()
	}

	return nil
}

// Close - does nothing and available for interface compatibility.
func (target *ElasticsearchTarget) Close() error {
	return nil
}

// NewElasticsearchTarget - creates new Elasticsearch target.
func NewElasticsearchTarget(id string, args ElasticsearchArgs) (*ElasticsearchTarget, error) {
	client, err := elastic.NewClient(elastic.SetURL(args.URL.String()), elastic.SetSniff(false), elastic.SetMaxRetries(10))
	if err != nil {
		return nil, err
	}

	exists, err := client.IndexExists(args.Index).Do(context.Background())
	if err != nil {
		return nil, err
	}

	if !exists {
		var createIndex *elastic.IndicesCreateResult
		if createIndex, err = client.CreateIndex(args.Index).Do(context.Background()); err != nil {
			return nil, err
		}

		if !createIndex.Acknowledged {
			return nil, fmt.Errorf("index %v not created", args.Index)
		}
	}

	return &ElasticsearchTarget{
		id:     event.TargetID{ID: id, Name: "elasticsearch"},
		args:   args,
		client: client,
	}, nil
}
