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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/pkg/errors"

	"gopkg.in/olivere/elastic.v5"
)

// Elastic constants
const (
	ElasticFormat     = "format"
	ElasticURL        = "url"
	ElasticIndex      = "index"
	ElasticQueueDir   = "queue_dir"
	ElasticQueueLimit = "queue_limit"

	EnvElasticEnable     = "MINIO_NOTIFY_ELASTICSEARCH_ENABLE"
	EnvElasticFormat     = "MINIO_NOTIFY_ELASTICSEARCH_FORMAT"
	EnvElasticURL        = "MINIO_NOTIFY_ELASTICSEARCH_URL"
	EnvElasticIndex      = "MINIO_NOTIFY_ELASTICSEARCH_INDEX"
	EnvElasticQueueDir   = "MINIO_NOTIFY_ELASTICSEARCH_QUEUE_DIR"
	EnvElasticQueueLimit = "MINIO_NOTIFY_ELASTICSEARCH_QUEUE_LIMIT"
)

// ElasticsearchArgs - Elasticsearch target arguments.
type ElasticsearchArgs struct {
	Enable     bool     `json:"enable"`
	Format     string   `json:"format"`
	URL        xnet.URL `json:"url"`
	Index      string   `json:"index"`
	QueueDir   string   `json:"queueDir"`
	QueueLimit uint64   `json:"queueLimit"`
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
	id         event.TargetID
	args       ElasticsearchArgs
	client     *elastic.Client
	store      Store
	loggerOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{})
}

// ID - returns target ID.
func (target *ElasticsearchTarget) ID() event.TargetID {
	return target.id
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *ElasticsearchTarget) HasQueueStore() bool {
	return target.store != nil
}

// IsActive - Return true if target is up and active
func (target *ElasticsearchTarget) IsActive() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if target.client == nil {
		client, err := newClient(target.args)
		if err != nil {
			return false, err
		}
		target.client = client
	}
	_, code, err := target.client.Ping(target.args.URL.String()).HttpHeadOnly(true).Do(ctx)
	if err != nil {
		if elastic.IsConnErr(err) || elastic.IsContextErr(err) || xnet.IsNetworkOrHostDown(err) {
			return false, errNotConnected
		}
		return false, err
	}
	return !(code >= http.StatusBadRequest), nil
}

// Save - saves the events to the store if queuestore is configured, which will be replayed when the elasticsearch connection is active.
func (target *ElasticsearchTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	err := target.send(eventData)
	if elastic.IsConnErr(err) || elastic.IsContextErr(err) || xnet.IsNetworkOrHostDown(err) {
		return errNotConnected
	}
	return err
}

// send - sends the event to the target.
func (target *ElasticsearchTarget) send(eventData event.Event) error {

	var key string

	exists := func() (bool, error) {
		return target.client.Exists().Index(target.args.Index).Type("event").Id(key).Do(context.Background())
	}

	remove := func() error {
		exists, err := exists()
		if err == nil && exists {
			_, err = target.client.Delete().Index(target.args.Index).Type("event").Id(key).Do(context.Background())
		}
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

// Send - reads an event from store and sends it to Elasticsearch.
func (target *ElasticsearchTarget) Send(eventKey string) error {
	var err error
	if target.client == nil {
		target.client, err = newClient(target.args)
		if err != nil {
			return err
		}
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

	if err := target.send(eventData); err != nil {
		if elastic.IsConnErr(err) || elastic.IsContextErr(err) || xnet.IsNetworkOrHostDown(err) {
			return errNotConnected
		}
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - does nothing and available for interface compatibility.
func (target *ElasticsearchTarget) Close() error {
	if target.client != nil {
		// Stops the background processes that the client is running.
		target.client.Stop()
	}
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

// newClient - creates a new elastic client with args provided.
func newClient(args ElasticsearchArgs) (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetURL(args.URL.String()), elastic.SetMaxRetries(10))
	if err != nil {
		// https://github.com/olivere/elastic/wiki/Connection-Errors
		if elastic.IsConnErr(err) || elastic.IsContextErr(err) || xnet.IsNetworkOrHostDown(err) {
			return nil, errNotConnected
		}
		return nil, err
	}
	if err = createIndex(client, args); err != nil {
		return nil, err
	}
	return client, nil
}

// NewElasticsearchTarget - creates new Elasticsearch target.
func NewElasticsearchTarget(id string, args ElasticsearchArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{}), test bool) (*ElasticsearchTarget, error) {
	target := &ElasticsearchTarget{
		id:         event.TargetID{ID: id, Name: "elasticsearch"},
		args:       args,
		loggerOnce: loggerOnce,
	}

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-elasticsearch-"+id)
		target.store = NewQueueStore(queueDir, args.QueueLimit)
		if err := target.store.Open(); err != nil {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
	}

	var err error
	target.client, err = newClient(args)
	if err != nil {
		if target.store == nil || err != errNotConnected {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
	}

	if target.store != nil && !test {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh, target.loggerOnce, target.ID())
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh, target.loggerOnce)
	}

	return target, nil
}
