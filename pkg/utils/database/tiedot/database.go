/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package tiedot

import (
	"encoding/json"
	"fmt"

	"github.com/HouzuoGuo/tiedot/db"
)

func NewDatabase(dirname string) (*Database, error) {
	data := Database{}
	data.setdbdir(dirname)
	if err := data.setDBhandle(); err != nil {
		return &Database{}, err
	}
	return &data, nil
}

func (data *Database) setdbdir(dirname string) {
	data.DBdir = dirname
}

func (data *Database) setDBhandle() error {
	var err error
	data.DBhandle, err = db.OpenDB(data.DBdir)
	if err != nil {
		return err
	}
	return nil
}

func (data *Database) InitCollection(colls ...string) {
	for _, str := range colls {
		data.DBhandle.Create(str)
	}
}

func (data *Database) GetAllCollections() []string {
	var colls []string
	for _, name := range data.DBhandle.AllCols() {
		colls = append(colls, name)
	}
	return colls
}

func (data *Database) getCollectionHandle(coll string) *db.Col {
	return data.DBhandle.Use(coll)
}

func (data *Database) InsertToCollection(coll string, model map[string]interface{}) (docid int, err error) {
	collHandle := data.getCollectionHandle(coll)
	return collHandle.Insert(model)
}

func (data *Database) InsertIndexToCollection(coll string, indexes []string) error {
	collHandle := data.getCollectionHandle(coll)
	return collHandle.Index(indexes)
}

func (data *Database) QueryDB(coll string, queryByte []byte) (map[int]struct{}, error) {
	if len(queryByte) <= 0 {
		return nil, fmt.Errorf("Invalid query string")
	}

	var query interface{}
	json.Unmarshal(queryByte, &query)

	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys
	err := db.EvalQuery(query, data.getCollectionHandle(coll), &queryResult)
	if err != nil {
		return nil, err
	}
	return queryResult, nil
}
