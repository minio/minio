/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/pkg/errors"

	"gopkg.in/olivere/elastic.v5"
)

// ElasticsearchArgs - Elasticsearch target arguments.
type ElasticsearchArgs struct {
	Enable     bool     `json:"enable"`
	Format     string   `json:"format"`
	URL        xnet.URL `json:"url"`
	Index      string   `json:"index"`
	QueueDir   string   `json:"queueDir"`
	QueueLimit uint16   `json:"queueLimit"`
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
	store  Store
}

// ID - returns target ID.
func (target *ElasticsearchTarget) ID() event.TargetID {
	return target.id
}

// Save - saves the events to the store which will be replayed when the elasticsearch connection is active.
func (target *ElasticsearchTarget) Save(eventData event.Event) error {
	return target.store.Put(eventData)
}

// Send - sends event to Elasticsearch.
func (target *ElasticsearchTarget) Send(eventKey string) error {

	var key string
	var err error

	if target.client == nil {
		target.client, err = newClient(target.args)
		if err != nil {
			return err
		}
	}

	if _, err := net.Dial("tcp", target.args.URL.Host); err != nil {
		return errNotConnected
	}

	eventData, eErr := target.store.Get(eventKey)
	if eErr != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and wouldve been already been sent successfully.
		if os.IsNotExist(eErr) {
			return nil
		}
		return eErr
	}

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

		if err == nil {
			return target.store.Del(eventKey)
		}
		return err
	}

	if target.args.Format == event.AccessFormat {
		if err := add(); err == nil {
			return target.store.Del(eventKey)
		}
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - does nothing and available for interface compatibility.
func (target *ElasticsearchTarget) Close() error {
	return nil
}

// createIndex - creates the index if it does not exist.
func createIndex(client *elastic.Client, args ElasticsearchArgs) error {
	exists, err := client.IndexExists(args.Index).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		var createIndex *elastic.IndicesCreateResult
		if createIndex, err = client.CreateIndex(args.Index).Do(context.Background()); err != nil {
			return err
		}

		if !createIndex.Acknowledged {
			return fmt.Errorf("index %v not created", args.Index)
		}
	}
	return nil
}

func newClient(args ElasticsearchArgs) (*elastic.Client, error) {
	client, clientErr := elastic.NewClient(elastic.SetURL(args.URL.String()), elastic.SetSniff(false), elastic.SetMaxRetries(10))
	if clientErr != nil {
		if !(errors.Cause(clientErr) == elastic.ErrNoClient) {
			return nil, clientErr
		}
	} else {
		if err := createIndex(client, args); err != nil {
			return nil, err
		}
	}
	return client, nil
}

// NewElasticsearchTarget - creates new Elasticsearch target.
func NewElasticsearchTarget(id string, args ElasticsearchArgs, doneCh <-chan struct{}) (*ElasticsearchTarget, error) {
	var client *elastic.Client
	var err error

	if _, derr := net.Dial("tcp", args.URL.Host); derr == nil {
		client, err = newClient(args)
		if err != nil {
			return nil, err
		}
	}

	var store Store

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-elasticsearch-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			return nil, oErr
		}
	} else {
		store = NewMemoryStore(args.QueueLimit)
	}

	target := &ElasticsearchTarget{
		id:     event.TargetID{ID: id, Name: "elasticsearch"},
		args:   args,
		client: client,
		store:  store,
	}

	// Replays the events from the store.
	eventKeyCh := replayEvents(target.store, doneCh)

	// Start replaying events from the store.
	go sendEvents(target, eventKeyCh, doneCh)

	return target, nil
}
