/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

package kv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/logger"

	etcd "github.com/coreos/etcd/clientv3"
)

var defaultContextTimeout = 30 * time.Second
var errEtcdUnreachable = errors.New("etcd is unreachable, please check your endpoints")

func etcdErrToErr(err error, etcdEndpoints []string) error {
	if err == nil {
		return nil
	}
	switch err {
	case context.DeadlineExceeded:
		return fmt.Errorf("%w %s", errEtcdUnreachable, etcdEndpoints)
	default:
		return fmt.Errorf("unexpected error %w from etcd, please check your endpoints %s", err, etcdEndpoints)
	}
}

// NewEtcdBackend creates a new etcd backend
func NewEtcdBackend(client *etcd.Client) *EtcdBackend {
	return &EtcdBackend{client: client}
}

// EtcdBackend a simple Etcd backend
type EtcdBackend struct {
	client *etcd.Client
}

// Save saves key/value pair
func (kv *EtcdBackend) Save(ctx context.Context, key string, data []byte) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	_, err := kv.client.Put(timeoutCtx, key, string(data))
	return etcdErrToErr(err, kv.client.Endpoints())
}

// Delete deletes key
func (kv *EtcdBackend) Delete(ctx context.Context, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	_, err := kv.client.Delete(timeoutCtx, key)
	return etcdErrToErr(err, kv.client.Endpoints())

}

// Keys returns child keys for given key
func (kv *EtcdBackend) Keys(ctx context.Context, key string) (set.StringSet, error) {
	return kv.KeysFilter(ctx, key, KeyTransformDefault)
}

// KeysFilter returns child keys for given key filtered by given KeyTransform
func (kv *EtcdBackend) KeysFilter(ctx context.Context, key string, transform KeyTransform) (set.StringSet, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	r, err := kv.client.Get(ctx, key, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	result := set.NewStringSet()
	for _, kv := range r.Kvs {
		newkey := transform(key, string(kv.Key))
		// skip key if transform returns a empty key
		if newkey != "" {
			result.Add(newkey)
		}
	}
	return result, nil
}

// Get get value for given key
func (kv *EtcdBackend) Get(ctx context.Context, key string) ([]byte, error) {

	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	resp, err := kv.client.Get(timeoutCtx, key)
	if err != nil {
		return nil, etcdErrToErr(err, kv.client.Endpoints())
	}
	if resp.Count == 0 {
		return nil, nil
	}
	for _, ev := range resp.Kvs {
		if string(ev.Key) == key {
			return ev.Value, nil
		}
	}
	return nil, nil
}

// Watch registers a event handler to key value store
func (kv *EtcdBackend) Watch(ctx context.Context, key string, handler EventHandler) {
	for {
	outerLoop:
		// Refresh IAMSys with etcd watch.
		watchCh := kv.client.Watch(ctx,
			key, etcd.WithPrefix(), etcd.WithKeysOnly())
		for {
			select {
			case <-ctx.Done():
				return
			case watchResp, ok := <-watchCh:
				if !ok {
					time.Sleep(1 * time.Second)
					// Upon an error on watch channel
					// re-init the watch channel.
					goto outerLoop
				}
				if err := watchResp.Err(); err != nil {
					logger.LogIf(ctx, err)
					// log and retry.
					time.Sleep(1 * time.Second)
					// Upon an error on watch channel
					// re-init the watch channel.
					goto outerLoop
				}
				for _, event := range watchResp.Events {
					eventCreate := event.IsModify() || event.IsCreate()
					eventDelete := event.Type == etcd.EventTypeDelete
					event := Event{
						Key: string(event.Kv.Key),
					}
					switch {
					case eventCreate:
						event.Type = EventTypeCreated
					case eventDelete:
						event.Type = EventTypeDeleted
					default:
						// log if event has not been handled?
						continue
					}
					handler(&event)
				}
			}
		}
	}
}

// Info returns backend information
func (kv *EtcdBackend) Info() string {
	return fmt.Sprintf("Etcd: %s", kv.client.Endpoints())
}
