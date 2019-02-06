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
	"errors"
	"sync"
)

// KeyHandler is the interface for 2 methods
type KeyHandler interface {
	// 'Check' function will return nil (no error), if the 'key' has
	// been already registered/valid. If not a registered/valid key, then
	// an error message will be returned.
	// Check(key string) error
	Check(val string) error
	// Help' function shows explanations and specs about the key
	Help() (helpText string, err error)
}

//Value is to hold the value and comment for a configuration key/parameter
type Value struct {
	val, comment string
}

// Server is where we keep the configuration file information
// about the content and the order of the configuration parameters/keys.
// Order array keeps the order of the entries as they appear in the
// configuration file.
// Registry map has the value and comment set for each and every key.
// Handlers map has all the Check and Help methods for each and
// every key.
type Server struct {
	RWMutex  *sync.RWMutex
	order    []string
	handlers map[string]KeyHandler
	registry map[string]Value
}

// Init initializes Server structure elements
func (s *Server) Init() {
	s.RWMutex = &sync.RWMutex{}
	s.handlers = make(map[string]KeyHandler)
	s.registry = make(map[string]Value)
}

// RegisterKey registers/creates an entry for the
// key/configuration parameter in the order array and
// in the registry map of Server structure.
// Handler methods, Check & Help, are implemented outside
// of this package by the external user.
func (s *Server) RegisterKey(key string, handler KeyHandler) (err error) {
	s.order = append(s.order, key)
	s.handlers[key] = handler
	return nil
}

// Get method returns the value and comment, if one is set, of a
// key/configuration parameter.
// It returns "Invalid configuration parameter" error if the key
// is found to be invalid and "Configuration parameter not set yet"
// error if key/configuration parameter has not been set yet.
func (s *Server) Get(key string) (string, string, error) {
	if _, ok := s.handlers[key]; !ok {
		return "", "", errors.New("Invalid configuration parameter key: \"" + key + "\"")
	}
	if _, ok := s.registry[key]; !ok {
		return "", "", errors.New("Configuration parameter, \"" + key + "\", not set yet")
	}
	val := s.registry[key].val
	comment := s.registry[key].comment
	return val, comment, nil
}

// Set method is used to set a value for a
// configuration parameter/key.
func (s *Server) Set(key, value, comment string) error {
	handler, ok := s.handlers[key]
	if !ok {
		return errors.New("Invalid configuration parameter key: \"" +
			key + "\"")
	}
	err := handler.Check(value)
	if err != nil {
		return errors.New("Invalid value")
	}
	s.registry[key] = Value{val: value, comment: comment}
	return nil
}

// List lists all configuration parameters with their set values.
func (s *Server) List() ([]string, error) {
	var listArr []string
	for _, k := range s.order {
		if _, ok := s.registry[k]; ok {
			configEntry := k + " = " + s.registry[k].val
			if s.registry[k].comment != "" {
				configEntry = configEntry + "    " + s.registry[k].comment
			}
			listArr = append(listArr, configEntry)
		}
	}
	return listArr, nil
}
