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
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/quick"
)

// UserInfo - users information.
type UserInfo struct {
	auth.Credentials
	Group  string `json:"group"`
	Policy string `json:"policy"`
	Role   string `json:"role"`
}

// UsersStore - users store implementation.
type UsersStore struct {
	sync.RWMutex
	Version    string              `json:"version"`
	Users      map[string]UserInfo `json:"users"`
	localFile  string
	etcdClient *etcd.Client
}

func loadUsersEtcdConfig(etcdClient *etcd.Client, us *UsersStore) error {
	r, err := etcdClient.Get(context.Background(), "users", etcd.WithPrefix())
	if err != nil {
		return err
	}
	if r.Count == 0 {
		return nil
	}
	for _, kv := range r.Kvs {
		var ui UserInfo
		decoder := json.NewDecoder(bytes.NewReader(kv.Value))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&ui); err != nil {
			return err
		}
		us.Users[string(kv.Key)] = ui
	}
	return nil
}

func loadUsersConfig(filename string, etcdClient *etcd.Client, us *UsersStore) error {
	if etcdClient != nil {
		return loadUsersEtcdConfig(etcdClient, us)
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
	return decoder.Decode(us)
}

// NewUsersStore - Initialize a new credetnials store.
func NewUsersStore(localFile string, etcdClient *etcd.Client) (*UsersStore, error) {
	s := &UsersStore{
		Version:    "1",
		Users:      make(map[string]UserInfo),
		etcdClient: etcdClient,
		localFile:  localFile,
	}
	if err := loadUsersConfig(localFile, etcdClient, s); err != nil {
		return nil, err
	}
	go s.watch()
	return s, nil
}

func (s *UsersStore) watch() {
	// No watch routine is needed if etcd is not configured.
	if s.etcdClient == nil {
		return
	}
	go func() {
		for watchResp := range s.etcdClient.Watch(context.Background(), "users", etcd.WithPrefix()) {
			for _, event := range watchResp.Events {
				if event.IsCreate() || event.IsModify() {
					var ui UserInfo
					if err := json.Unmarshal(event.Kv.Value, &ui); err != nil {
						continue
					}
					s.Lock()
					s.Users[ui.AccessKey] = ui
					s.Unlock()
				}
			}
		}
	}()
}

// Set - a new user info.
func (s *UsersStore) Set(ui UserInfo) {
	s.Lock()
	defer s.Unlock()

	s.Users[ui.AccessKey] = ui

	if s.etcdClient != nil {
		uiData, err := json.Marshal(&ui)
		if err != nil {
			return
		}
		s.etcdClient.Put(context.Background(), fmt.Sprintf("users/%s", ui.AccessKey), string(uiData))
	} else {
		quick.SaveConfig(s, s.localFile, nil)
	}
}

// Get - get a new user info, access key.
func (s *UsersStore) Get(accessKey string) (UserInfo, bool) {
	s.RLock()
	defer s.RUnlock()

	u, ok := s.Users[accessKey]
	return u, u.IsValid() && ok
}

// GetGroup - get group name of the given user.
func (s *UsersStore) GetGroup(accessKey string) string {
	ui, ok := s.Get(accessKey)
	if !ok {
		return ""
	}
	return ui.Group
}

// GetRole - get role name of the given user.
func (s *UsersStore) GetRole(accessKey string) string {
	ui, ok := s.Get(accessKey)
	if !ok {
		return ""
	}
	return ui.Role
}

// GetPolicy - get policy name associated with the given user.
func (s *UsersStore) GetPolicy(accessKey string) string {
	ui, ok := s.Get(accessKey)
	if !ok {
		return ""
	}
	return ui.Policy
}

// Delete - delete a user.
func (s *UsersStore) Delete(accessKey string) {
	s.Lock()
	defer s.Unlock()

	delete(s.Users, accessKey)
	if s.etcdClient != nil {
		s.etcdClient.Delete(context.Background(), fmt.Sprintf("users/%s", accessKey))
	} else {
		quick.SaveConfig(s, s.localFile, nil)
	}
}
