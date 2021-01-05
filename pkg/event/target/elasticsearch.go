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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/pkg/errors"

	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/minio/highwayhash"
)

// Elastic constants
const (
	ElasticFormat     = "format"
	ElasticURL        = "url"
	ElasticIndex      = "index"
	ElasticQueueDir   = "queue_dir"
	ElasticQueueLimit = "queue_limit"
	ElasticUsername   = "username"
	ElasticPassword   = "password"

	EnvElasticEnable     = "MINIO_NOTIFY_ELASTICSEARCH_ENABLE"
	EnvElasticFormat     = "MINIO_NOTIFY_ELASTICSEARCH_FORMAT"
	EnvElasticURL        = "MINIO_NOTIFY_ELASTICSEARCH_URL"
	EnvElasticIndex      = "MINIO_NOTIFY_ELASTICSEARCH_INDEX"
	EnvElasticQueueDir   = "MINIO_NOTIFY_ELASTICSEARCH_QUEUE_DIR"
	EnvElasticQueueLimit = "MINIO_NOTIFY_ELASTICSEARCH_QUEUE_LIMIT"
	EnvElasticUsername   = "MINIO_NOTIFY_ELASTICSEARCH_USERNAME"
	EnvElasticPassword   = "MINIO_NOTIFY_ELASTICSEARCH_PASSWORD"
)

// ElasticsearchArgs - Elasticsearch target arguments.
type ElasticsearchArgs struct {
	Enable     bool            `json:"enable"`
	Format     string          `json:"format"`
	URL        xnet.URL        `json:"url"`
	Index      string          `json:"index"`
	QueueDir   string          `json:"queueDir"`
	QueueLimit uint64          `json:"queueLimit"`
	Transport  *http.Transport `json:"-"`
	Username   string          `json:"username"`
	Password   string          `json:"password"`
}

// magic HH-256 key as HH-256 hash of the first 100 decimals of π as utf-8 string with a zero key.
var magicHighwayHash256Key = []byte("\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0")

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

	if (a.Username == "" && a.Password != "") || (a.Username != "" && a.Password == "") {
		return errors.New("username and password should be set in pairs")
	}

	return nil
}

// ElasticsearchTarget - Elasticsearch target.
type ElasticsearchTarget struct {
	id         event.TargetID
	args       ElasticsearchArgs
	client     *elasticsearch7.Client
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

	// Check if cluster is running.
	resp, err := target.client.Ping(
		target.client.Ping.WithContext(ctx),
	)
	if err != nil {
		return false, errNotConnected
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	return !resp.IsError(), nil
}

// Save - saves the events to the store if queuestore is configured, which will be replayed when the elasticsearch connection is active.
func (target *ElasticsearchTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	err := target.send(eventData)
	if xnet.IsNetworkOrHostDown(err, false) {
		return errNotConnected
	}
	return err
}

// send - sends the event to the target.
func (target *ElasticsearchTarget) send(eventData event.Event) error {

	var keyHash string

	exists := func() (bool, error) {
		res, err := target.client.Exists(
			target.args.Index,
			keyHash,
		)
		if err != nil {
			return false, err
		}
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
		return !res.IsError(), nil
	}

	remove := func() error {
		exists, err := exists()
		if err == nil && exists {
			res, err := target.client.Delete(
				target.args.Index,
				keyHash,
			)
			if err != nil {
				return err
			}
			defer res.Body.Close()
			if res.IsError() {
				err := fmt.Errorf("Delete err: %s", res.String())
				return err
			}
			io.Copy(ioutil.Discard, res.Body)
			return nil
		}
		return err
	}

	update := func() error {
		doc := map[string]interface{}{
			"Records": []event.Event{eventData},
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		err := enc.Encode(doc)
		if err != nil {
			return err
		}
		res, err := target.client.Index(
			target.args.Index,
			&buf,
			target.client.Index.WithDocumentID(keyHash),
		)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			err := fmt.Errorf("Update err: %s", res.String())
			return err
		}
		io.Copy(ioutil.Discard, res.Body)
		return nil
	}

	add := func() error {
		doc := map[string]interface{}{
			"Records": []event.Event{eventData},
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		err := enc.Encode(doc)
		if err != nil {
			return err
		}
		res, err := target.client.Index(
			target.args.Index,
			&buf,
		)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			err := fmt.Errorf("Add err: %s", res.String())
			return err
		}
		io.Copy(ioutil.Discard, res.Body)
		return nil
	}

	if target.args.Format == event.NamespaceFormat {
		objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
		if err != nil {
			return err
		}

		// Calculate a hash of the key for the id of the ES document.
		// Id's are limited to 512 bytes, so we need to do this.
		key := eventData.S3.Bucket.Name + "/" + objectName
		hh, _ := highwayhash.New(magicHighwayHash256Key) // New will never return error since key is 256 bit
		hh.Write([]byte(key))
		hashBytes := hh.Sum(nil)
		keyHash = base64.URLEncoding.EncodeToString(hashBytes)

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
		if xnet.IsNetworkOrHostDown(err, false) {
			return errNotConnected
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
func createIndex(client *elasticsearch7.Client, args ElasticsearchArgs) error {
	res, err := client.Indices.ResolveIndex([]string{args.Index})
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var v map[string]interface{}
	found := false
	if err := json.NewDecoder(res.Body).Decode(&v); err != nil {
		return fmt.Errorf("Error parsing response body: %v", err)
	}

	indices := v["indices"].([]interface{})
	for _, index := range indices {
		name := index.(map[string]interface{})["name"]
		if name == args.Index {
			found = true
			break
		}
	}

	if !found {
		resp, err := client.Indices.Create(args.Index)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.IsError() {
			err := fmt.Errorf("Create index err: %s", res.String())
			return err
		}
		io.Copy(ioutil.Discard, resp.Body)
		return nil
	}
	return nil
}

// newClient - creates a new elastic client with args provided.
func newClient(args ElasticsearchArgs) (*elasticsearch7.Client, error) {
	// Client options
	elasticConfig := elasticsearch7.Config{
		Addresses:  []string{args.URL.String()},
		Transport:  args.Transport,
		MaxRetries: 10,
	}
	// Set basic auth
	if args.Username != "" && args.Password != "" {
		elasticConfig.Username = args.Username
		elasticConfig.Password = args.Password
	}
	// Create a client
	client, err := elasticsearch7.NewClient(elasticConfig)
	if err != nil {
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
