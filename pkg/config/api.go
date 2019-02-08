/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Key is the interface for each config key to provide a validation
// function and a help.
type Key interface {
	// Validate function will return no error, if the 'key' has
	// been already registered/valid. If not a registered/valid key, then
	// an error message will be returned.
	Validate(value string) error

	// Name represents key name for this interface.
	Name() string

	// Help function shows explanations and specs about the key.
	Help() string
}

// KV - holds the key and its corresponding value.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Server is where we keep the configuration file information
// about the content and the order of the configuration parameters/keys.
// Order array keeps the order of the entries as they appear in the
// configuration file.
// Registry map has the value and comment set for each and every key.
// Handlers map has all the Check and Help methods for each and
// every key.
type Server struct {
	sync.RWMutex
	config     map[string]map[string][]KV
	validators map[string]Key
}

// New - returns an initialized version of Server.
func New() *Server {
	return &Server{
		config:     make(map[string]map[string][]KV),
		validators: make(map[string]Key),
	}
}

// RegisterKey - registers a new key into config subsystem.
func (s *Server) RegisterKey(key Key) error {
	if key == nil || key.Name() == "" && key.Help() == "" {
		return errors.New("Key cannot be uninitialized")
	}
	s.Lock()
	defer s.Unlock()
	s.validators[key.Name()] = key
	return nil
}

// Delete deletes the in-memory value that was set for an given subsys.
// Unlike Set and Get, alias doesn't default to 'default' value. If
// alias is not explicitly provided then we delete the entire subsystem.
func (s *Server) Delete(subSys string, alias string) error {
	s.Lock()
	defer s.Unlock()

	_, ok := s.config[subSys]
	if !ok {
		return fmt.Errorf("Requested sub-system %s is not registered", subSys)
	}

	if alias != "" {
		_, ok = s.config[subSys][alias]
		if !ok {
			return fmt.Errorf("Requested sub-system alias %s:%s is not registered", subSys, alias)
		}
	}

	if alias != "" {
		delete(s.config[subSys], alias)
	} else {
		delete(s.config, subSys)
	}

	return nil
}

// Get returns the value that was set for an given subsys,
// alias is optional defaults to value 'default'
func (s *Server) Get(subSys string, alias string) ([]KV, error) {
	if alias == "" {
		alias = "default"
	}

	s.RLock()
	defer s.RUnlock()

	_, ok := s.config[subSys]
	if !ok {
		return nil, fmt.Errorf("Requested sub-system %s is not registered", subSys)
	}

	_, ok = s.config[subSys][alias]
	if !ok {
		return nil, fmt.Errorf("Requested sub-system alias %s:%s is not registered", subSys, alias)
	}

	return s.config[subSys][alias], nil
}

// Set saves incoming list of keys values for given subsystem.
// alias is optional defaults to value 'default'.
func (s *Server) Set(subSys string, kvs []KV, alias string) error {
	if alias == "" {
		alias = "default"
	}

	s.Lock()
	defer s.Unlock()

	for _, kv := range kvs {
		if err := s.validators[kv.Key].Validate(kv.Value); err != nil {
			return err
		}
	}

	s.config[subSys][alias] = kvs
	return nil
}

// UnmarshalJSON implements custom unmarshal for Server struct.
func (s *Server) UnmarshalJSON(b []byte) error {
	var config = map[string]map[string][]KV{}
	if err := json.Unmarshal(b, config); err != nil {
		return err
	}

	// Validate all key values before unmarshalling.
	for _, subV := range config {
		for _, kvs := range subV {
			for _, kv := range kvs {
				if err := s.validators[kv.Key].Validate(kv.Value); err != nil {
					return err
				}
			}
		}
	}
	return json.Unmarshal(b, &s.config)
}

// MarshalJSON implements custom marshal for Server struct.
func (s *Server) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.config)
}
