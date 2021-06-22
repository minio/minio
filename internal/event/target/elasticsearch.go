// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/event"
	xnet "github.com/minio/pkg/net"
	"github.com/pkg/errors"

	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/minio/highwayhash"
	"github.com/olivere/elastic/v7"
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

// ESSupportStatus is a typed string representing the support status for
// Elasticsearch
type ESSupportStatus string

const (
	// ESSUnknown is default value
	ESSUnknown ESSupportStatus = "ESSUnknown"
	// ESSDeprecated -> support will be removed in future
	ESSDeprecated ESSupportStatus = "ESSDeprecated"
	// ESSUnsupported -> we wont work with this ES server
	ESSUnsupported ESSupportStatus = "ESSUnsupported"
	// ESSSupported -> all good!
	ESSSupported ESSupportStatus = "ESSSupported"
)

func getESVersionSupportStatus(version string) (res ESSupportStatus, err error) {
	parts := strings.Split(version, ".")
	if len(parts) < 1 {
		err = fmt.Errorf("bad ES version string: %s", version)
		return
	}

	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		err = fmt.Errorf("bad ES version string: %s", version)
		return
	}

	switch {
	case majorVersion <= 4:
		res = ESSUnsupported
	case majorVersion <= 6:
		res = ESSDeprecated
	default:
		res = ESSSupported
	}
	return
}

// magic HH-256 key as HH-256 hash of the first 100 decimals of π as utf-8 string with a zero key.
var magicHighwayHash256Key = []byte("\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0")

// Interface for elasticsearch client objects
type esClient interface {
	isAtleastV7() bool
	createIndex(ElasticsearchArgs) error
	ping(context.Context, ElasticsearchArgs) (bool, error)
	stop()

	entryExists(context.Context, string, string) (bool, error)
	removeEntry(context.Context, string, string) error
	updateEntry(context.Context, string, string, event.Event) error
	addEntry(context.Context, string, event.Event) error
}

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
	client     esClient
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

	err := target.checkAndInitClient(ctx)
	if err != nil {
		return false, err
	}
	return target.client.ping(ctx, target.args)
}

// Save - saves the events to the store if queuestore is configured, which will be replayed when the elasticsearch connection is active.
func (target *ElasticsearchTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	err := target.send(eventData)
	if elastic.IsConnErr(err) || elastic.IsContextErr(err) || xnet.IsNetworkOrHostDown(err, false) {
		return errNotConnected
	}
	return err
}

// send - sends the event to the target.
func (target *ElasticsearchTarget) send(eventData event.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if target.args.Format == event.NamespaceFormat {
		objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
		if err != nil {
			return err
		}

		// Calculate a hash of the key for the id of the ES document.
		// Id's are limited to 512 bytes in V7+, so we need to do this.
		var keyHash string
		{
			key := eventData.S3.Bucket.Name + "/" + objectName
			if target.client.isAtleastV7() {
				hh, _ := highwayhash.New(magicHighwayHash256Key) // New will never return error since key is 256 bit
				hh.Write([]byte(key))
				hashBytes := hh.Sum(nil)
				keyHash = base64.URLEncoding.EncodeToString(hashBytes)
			} else {
				keyHash = key
			}
		}

		if eventData.EventName == event.ObjectRemovedDelete {
			err = target.client.removeEntry(ctx, target.args.Index, keyHash)
		} else {
			err = target.client.updateEntry(ctx, target.args.Index, keyHash, eventData)
		}
		return err
	}

	if target.args.Format == event.AccessFormat {
		return target.client.addEntry(ctx, target.args.Index, eventData)
	}

	return nil
}

// Send - reads an event from store and sends it to Elasticsearch.
func (target *ElasticsearchTarget) Send(eventKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := target.checkAndInitClient(ctx)
	if err != nil {
		return err
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
		if elastic.IsConnErr(err) || elastic.IsContextErr(err) || xnet.IsNetworkOrHostDown(err, false) {
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
		target.client.stop()
	}
	return nil
}

func (target *ElasticsearchTarget) checkAndInitClient(ctx context.Context) error {
	if target.client != nil {
		return nil
	}

	clientV7, err := newClientV7(target.args)
	if err != nil {
		return err
	}

	// Check es version to confirm if it is supported.
	serverSupportStatus, version, err := clientV7.getServerSupportStatus(ctx)
	if err != nil {
		return err
	}

	switch serverSupportStatus {
	case ESSUnknown:
		return errors.New("unable to determine support status of ES (should not happen)")

	case ESSDeprecated:
		fmt.Printf("DEPRECATION WARNING: Support for Elasticsearch version '%s' will be dropped in a future release. Please upgrade to a version >= 7.x.", version)
		target.client, err = newClientV56(target.args)
		if err != nil {
			return err
		}

	case ESSSupported:
		target.client = clientV7

	default:
		// ESSUnsupported case
		return fmt.Errorf("Elasticsearch version '%s' is not supported! Please use at least version 7.x.", version)
	}

	target.client.createIndex(target.args)
	return nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := target.checkAndInitClient(ctx)
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

// ES Client definitions and methods

type esClientV7 struct {
	*elasticsearch7.Client
}

func newClientV7(args ElasticsearchArgs) (*esClientV7, error) {
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
	clientV7 := &esClientV7{client}
	return clientV7, nil
}

func (c *esClientV7) getServerSupportStatus(ctx context.Context) (ESSupportStatus, string, error) {
	resp, err := c.Info(
		c.Info.WithContext(ctx),
	)
	if err != nil {
		return ESSUnknown, "", errNotConnected
	}

	defer resp.Body.Close()

	m := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&m)
	if err != nil {
		return ESSUnknown, "", fmt.Errorf("unable to get ES Server version - json parse error: %v", err)
	}

	if v, ok := m["version"].(map[string]interface{}); ok {
		if ver, ok := v["number"].(string); ok {
			status, err := getESVersionSupportStatus(ver)
			return status, ver, err
		}
	}
	return ESSUnknown, "", fmt.Errorf("Unable to get ES Server Version - got INFO response: %v", m)

}

func (c *esClientV7) isAtleastV7() bool {
	return true
}

// createIndex - creates the index if it does not exist.
func (c *esClientV7) createIndex(args ElasticsearchArgs) error {
	res, err := c.Indices.ResolveIndex([]string{args.Index})
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
		resp, err := c.Indices.Create(args.Index)
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

func (c *esClientV7) ping(ctx context.Context, _ ElasticsearchArgs) (bool, error) {
	resp, err := c.Ping(
		c.Ping.WithContext(ctx),
	)
	if err != nil {
		return false, errNotConnected
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	return !resp.IsError(), nil
}

func (c *esClientV7) entryExists(ctx context.Context, index string, key string) (bool, error) {
	res, err := c.Exists(
		index,
		key,
		c.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()
	return !res.IsError(), nil
}

func (c *esClientV7) removeEntry(ctx context.Context, index string, key string) error {
	exists, err := c.entryExists(ctx, index, key)
	if err == nil && exists {
		res, err := c.Delete(
			index,
			key,
			c.Delete.WithContext(ctx),
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

func (c *esClientV7) updateEntry(ctx context.Context, index string, key string, eventData event.Event) error {
	doc := map[string]interface{}{
		"Records": []event.Event{eventData},
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(doc)
	if err != nil {
		return err
	}
	res, err := c.Index(
		index,
		&buf,
		c.Index.WithDocumentID(key),
		c.Index.WithContext(ctx),
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

func (c *esClientV7) addEntry(ctx context.Context, index string, eventData event.Event) error {
	doc := map[string]interface{}{
		"Records": []event.Event{eventData},
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(doc)
	if err != nil {
		return err
	}
	res, err := c.Index(
		index,
		&buf,
		c.Index.WithContext(ctx),
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

func (c *esClientV7) stop() {
}

// For versions under 7
type esClientV56 struct {
	*elastic.Client
}

func newClientV56(args ElasticsearchArgs) (*esClientV56, error) {
	// Client options
	options := []elastic.ClientOptionFunc{elastic.SetURL(args.URL.String()),
		elastic.SetMaxRetries(10),
		elastic.SetSniff(false),
		elastic.SetHttpClient(&http.Client{Transport: args.Transport})}
	// Set basic auth
	if args.Username != "" && args.Password != "" {
		options = append(options, elastic.SetBasicAuth(args.Username, args.Password))
	}
	// Create a client
	client, err := elastic.NewClient(options...)
	if err != nil {
		// https://github.com/olivere/elastic/wiki/Connection-Errors
		if elastic.IsConnErr(err) || elastic.IsContextErr(err) || xnet.IsNetworkOrHostDown(err, false) {
			return nil, errNotConnected
		}
		return nil, err
	}
	return &esClientV56{client}, nil
}

func (c *esClientV56) isAtleastV7() bool {
	return false
}

// createIndex - creates the index if it does not exist.
func (c *esClientV56) createIndex(args ElasticsearchArgs) error {
	exists, err := c.IndexExists(args.Index).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		var createIndex *elastic.IndicesCreateResult
		if createIndex, err = c.CreateIndex(args.Index).Do(context.Background()); err != nil {
			return err
		}

		if !createIndex.Acknowledged {
			return fmt.Errorf("index %v not created", args.Index)
		}
	}
	return nil
}

func (c *esClientV56) ping(ctx context.Context, args ElasticsearchArgs) (bool, error) {
	_, code, err := c.Ping(args.URL.String()).HttpHeadOnly(true).Do(ctx)
	if err != nil {
		if elastic.IsConnErr(err) || elastic.IsContextErr(err) || xnet.IsNetworkOrHostDown(err, false) {
			return false, errNotConnected
		}
		return false, err
	}
	return !(code >= http.StatusBadRequest), nil

}

func (c *esClientV56) entryExists(ctx context.Context, index string, key string) (bool, error) {
	return c.Exists().Index(index).Type("event").Id(key).Do(ctx)
}

func (c *esClientV56) removeEntry(ctx context.Context, index string, key string) error {
	exists, err := c.entryExists(ctx, index, key)
	if err == nil && exists {
		_, err = c.Delete().Index(index).Type("event").Id(key).Do(ctx)
	}
	return err

}

func (c *esClientV56) updateEntry(ctx context.Context, index string, key string, eventData event.Event) error {
	_, err := c.Index().Index(index).Type("event").BodyJson(map[string]interface{}{"Records": []event.Event{eventData}}).Id(key).Do(ctx)
	return err
}

func (c *esClientV56) addEntry(ctx context.Context, index string, eventData event.Event) error {
	_, err := c.Index().Index(index).Type("event").BodyJson(map[string]interface{}{"Records": []event.Event{eventData}}).Do(ctx)
	return err

}

func (c *esClientV56) stop() {
	c.Stop()
}
