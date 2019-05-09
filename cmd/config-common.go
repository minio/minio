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

package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
)

var errConfigNotFound = errors.New("config file not found")

func readConfig(ctx context.Context, objAPI ObjectLayer, configFile string) ([]byte, error) {
	var buffer bytes.Buffer
	// Read entire content by setting size to -1
	if err := objAPI.GetObject(ctx, minioMetaBucket, configFile, 0, -1, &buffer, "", ObjectOptions{}); err != nil {
		// Treat object not found as config not found.
		if isErrObjectNotFound(err) {
			return nil, errConfigNotFound
		}

		logger.GetReqInfo(ctx).AppendTags("configFile", configFile)
		logger.LogIf(ctx, err)
		return nil, err
	}

	// Return config not found on empty content.
	if buffer.Len() == 0 {
		return nil, errConfigNotFound
	}

	return buffer.Bytes(), nil
}

func deleteConfigEtcd(ctx context.Context, client *etcd.Client, configFile string) error {
	_, err := client.Delete(ctx, configFile)
	return err
}

func deleteConfig(ctx context.Context, objAPI ObjectLayer, configFile string) error {
	return objAPI.DeleteObject(ctx, minioMetaBucket, configFile)
}

func saveConfigEtcd(ctx context.Context, client *etcd.Client, configFile string, data []byte) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	_, err := client.Put(timeoutCtx, configFile, string(data))
	if err == context.DeadlineExceeded {
		return fmt.Errorf("etcd setup is unreachable, please check your endpoints %s", client.Endpoints())
	} else if err != nil {
		return fmt.Errorf("unexpected error %s returned by etcd setup, please check your endpoints %s", err, client.Endpoints())
	}
	return nil
}

func saveConfig(ctx context.Context, objAPI ObjectLayer, configFile string, data []byte) error {
	hashReader, err := hash.NewReader(bytes.NewReader(data), int64(len(data)), "", getSHA256Hash(data), int64(len(data)), globalCLIContext.StrictS3Compat)
	if err != nil {
		return err
	}

	_, err = objAPI.PutObject(ctx, minioMetaBucket, configFile, NewPutObjReader(hashReader, nil, nil), ObjectOptions{})
	return err
}

func readConfigEtcd(ctx context.Context, client *etcd.Client, configFile string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	resp, err := client.Get(timeoutCtx, configFile)
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, fmt.Errorf("etcd setup is unreachable, please check your endpoints %s", client.Endpoints())
		}
		return nil, fmt.Errorf("unexpected error %s returned by etcd setup, please check your endpoints %s", err, client.Endpoints())
	}
	if resp.Count == 0 {
		return nil, errConfigNotFound
	}
	for _, ev := range resp.Kvs {
		if string(ev.Key) == configFile {
			return ev.Value, nil
		}
	}
	return nil, errConfigNotFound
}

// watchConfigEtcd - watches for changes on `configFile` on etcd and loads them.
func watchConfigEtcd(objAPI ObjectLayer, configFile string, loadCfgFn func(ObjectLayer) error) {
	for {
		watchCh := globalEtcdClient.Watch(context.Background(), iamConfigPrefix)
		select {
		case <-GlobalServiceDoneCh:
			return
		case watchResp, ok := <-watchCh:
			if !ok {
				time.Sleep(1 * time.Second)
				continue
			}
			if err := watchResp.Err(); err != nil {
				logger.LogIf(context.Background(), err)
				// log and retry.
				time.Sleep(1 * time.Second)
				continue
			}
			for _, event := range watchResp.Events {
				if event.IsModify() || event.IsCreate() {
					loadCfgFn(objAPI)
				}
			}
		}
	}
}

func checkConfigEtcd(ctx context.Context, client *etcd.Client, configFile string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	resp, err := client.Get(timeoutCtx, configFile)
	if err != nil {
		if err == context.DeadlineExceeded {
			return fmt.Errorf("etcd setup is unreachable, please check your endpoints %s", client.Endpoints())
		}
		return fmt.Errorf("unexpected error %s returned by etcd setup, please check your endpoints %s", err, client.Endpoints())
	}
	if resp.Count == 0 {
		return errConfigNotFound
	}
	return nil
}

func checkConfig(ctx context.Context, objAPI ObjectLayer, configFile string) error {
	if globalEtcdClient != nil {
		return checkConfigEtcd(ctx, globalEtcdClient, configFile)
	}

	if _, err := objAPI.GetObjectInfo(ctx, minioMetaBucket, configFile, ObjectOptions{}); err != nil {
		// Treat object not found as config not found.
		if isErrObjectNotFound(err) {
			return errConfigNotFound
		}

		logger.GetReqInfo(ctx).AppendTags("configFile", configFile)
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}
