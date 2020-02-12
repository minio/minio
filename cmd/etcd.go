/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"errors"
	"fmt"

	etcd "github.com/coreos/etcd/clientv3"
)

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

func saveKeyEtcd(ctx context.Context, client *etcd.Client, key string, data []byte) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	_, err := client.Put(timeoutCtx, key, string(data))
	return etcdErrToErr(err, client.Endpoints())
}

func deleteKeyEtcd(ctx context.Context, client *etcd.Client, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	_, err := client.Delete(timeoutCtx, key)
	return etcdErrToErr(err, client.Endpoints())
}

func readKeyEtcd(ctx context.Context, client *etcd.Client, key string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	resp, err := client.Get(timeoutCtx, key)
	if err != nil {
		return nil, etcdErrToErr(err, client.Endpoints())
	}
	if resp.Count == 0 {
		return nil, errConfigNotFound
	}
	for _, ev := range resp.Kvs {
		if string(ev.Key) == key {
			return ev.Value, nil
		}
	}
	return nil, errConfigNotFound
}
