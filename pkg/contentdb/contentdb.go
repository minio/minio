/*
 * mime-db: Mime Database, (C) 2015 Minio, Inc.
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

// Package contentdb is a database of file extension to mime content-type.
// Definitions are imported from NodeJS mime-db project under MIT license.
package contentdb

import (
	"fmt"
	"strings"

	"encoding/json"
)

var (
	// Make note of initialization.
	isInitialized = false

	// Database of extension:content-type.
	extDB map[string]string
)

// Load JSON data from gobindata and parse them into extDB.
func loadDB() error {
	// Structure of JSON data from mime-db project.
	type dbEntry struct {
		Source       string   `json:"source"`
		Compressible bool     `json:"compresible"`
		Extensions   []string `json:"extensions"`
	}

	// Access embedded "db.json" inside go-bindata.
	jsonDB, e := Asset("db/db.json")
	if e != nil {
		return e
	}

	// Convert db.json into go's typed structure.
	db := make(map[string]dbEntry)
	if e := json.Unmarshal(jsonDB, &db); e != nil {
		return e
	}

	// Generate a new database from mime-db.
	for key, val := range db {
		if len(val.Extensions) > 0 {
			/* Denormalize - each extension has its own
			unique content-type now. Looks will be fast. */
			for _, ext := range val.Extensions {
				/* Single extension type may map to
				multiple content-types. In that case,
				simply prefer the longest content-type
				to maintain some level of
				consistency. Only guarantee is,
				whatever content type is assigned, it
				is appropriate and valid type. */
				if strings.Compare(extDB[ext], key) < 0 {
					extDB[ext] = key
				}
			}
		}
	}
	return nil
}

// Init initializes contentdb for lookups. JSON structure is parsed into a simple map of extension and content-type.
func Init() error {
	var e error
	extDB = make(map[string]string)

	if !isInitialized {
		e = loadDB()
	}
	isInitialized = true
	return e
}

// Lookup returns matching content-type for known types of file extensions.
func Lookup(extension string) (contentType string, e error) {
	if !isInitialized {
		e = Init()
	}

	return extDB[extension], e
}

// MustLookup returns matching content-type for known types of file extensions. In case of error, it panics.
func MustLookup(extension string) (contentType string) {
	if !isInitialized {
		if e := Init(); e != nil {
			panic(fmt.Sprintf("Error loading contentdb: %s\n", e))
		}
	}

	return extDB[extension]
}
