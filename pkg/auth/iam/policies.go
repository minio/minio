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

package iam

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/minio/minio/pkg/policy/iam"
	"github.com/minio/minio/pkg/quick"
)

// PolicyList - list of configured policies.
type PolicyList map[string]iam.Policy

// PolicyStore - policy store files.
type PolicyStore struct {
	sync.RWMutex
	Policies   PolicyList
	etcdClient *etcd.Client
	localFile  string
}

func loadPoliciesEtcdConfig(etcdClient *etcd.Client, p PolicyList) error {
	r, err := etcdClient.Get(context.Background(), "policies", etcd.WithPrefix())
	if err != nil {
		return err
	}
	if r.Count == 0 {
		return nil
	}
	for _, kv := range r.Kvs {
		var plc iam.Policy
		decoder := json.NewDecoder(bytes.NewReader(kv.Value))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&plc); err != nil {
			return err
		}
		p[string(kv.Key)] = plc
	}
	return nil
}

func loadPoliciesConfig(filename string, etcdClient *etcd.Client, p PolicyList) error {
	if etcdClient != nil {
		return loadPoliciesEtcdConfig(etcdClient, p)
	}
	of, err := os.Open(filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	defer of.Close()
	if os.IsNotExist(err) {
		return nil
	}
	decoder := json.NewDecoder(of)
	decoder.DisallowUnknownFields()
	return decoder.Decode(&p)
}

// NewPolicyStore - Initialize a new policy store.
func NewPolicyStore(localFile string, etcdClient *etcd.Client) (*PolicyStore, error) {
	p := &PolicyStore{
		Policies:   make(PolicyList),
		etcdClient: etcdClient,
		localFile:  localFile,
	}
	if err := loadPoliciesConfig(localFile, etcdClient, p.Policies); err != nil {
		return nil, err
	}
	go p.watch()
	return p, nil
}

func (p *PolicyStore) watch() {
	// No watch routine is needed if etcd is not configured.
	if p.etcdClient == nil {
		return
	}
	go func() {
		for watchResp := range p.etcdClient.Watch(context.Background(), "policies", etcd.WithPrefix()) {
			for _, event := range watchResp.Events {
				if event.IsCreate() || event.IsModify() {
					var pc iam.Policy

					if err := json.Unmarshal(event.Kv.Value, &pc); err != nil {
						continue
					}
					p.Lock()
					p.Policies[string(event.Kv.Key)] = pc
					p.Unlock()
				}
			}
		}
	}()
}

// Set - a new policy or overwrite an existing iam.
func (p *PolicyStore) Set(policyName string, pc iam.Policy) {
	p.Lock()
	defer p.Unlock()

	p.Policies[policyName] = pc

	if p.etcdClient != nil {
		pData, err := json.Marshal(pc)
		if err != nil {
			return
		}
		p.etcdClient.Put(context.Background(), fmt.Sprintf("policies/%s", policyName), string(pData))
	} else {
		quick.SaveConfig(p.Policies, p.localFile, nil)
	}
}

// Get - get iam.
func (p *PolicyStore) Get(policyName string) iam.Policy {
	p.RLock()
	defer p.RUnlock()

	return p.Policies[policyName]
}

// Delete - delete a policy
func (p *PolicyStore) Delete(policyName string) {
	p.Lock()
	defer p.Unlock()

	delete(p.Policies, policyName)
	if p.etcdClient != nil {
		p.etcdClient.Delete(context.Background(), fmt.Sprintf("policies/%s", policyName))
	} else {
		quick.SaveConfig(p.Policies, p.localFile, nil)
	}
}
