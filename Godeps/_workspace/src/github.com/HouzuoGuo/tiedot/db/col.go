/* Collection schema and index management. */
package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	DOC_DATA_FILE   = "dat_" // Prefix of partition collection data file name.
	DOC_LOOKUP_FILE = "id_"  // Prefix of partition hash table (ID lookup) file name.
	INDEX_PATH_SEP  = "!"    // Separator between index keys in index directory name.
)

// Collection has data partitions and some index meta information.
type Col struct {
	db         *DB
	name       string
	parts      []*data.Partition            // Collection partitions
	hts        []map[string]*data.HashTable // Index partitions
	indexPaths map[string][]string          // Index names and paths
}

// Open a collection and load all indexes.
func OpenCol(db *DB, name string) (*Col, error) {
	col := &Col{db: db, name: name}
	return col, col.load()
}

// Load collection schema including index schema.
func (col *Col) load() error {
	if err := os.MkdirAll(path.Join(col.db.path, col.name), 0700); err != nil {
		return err
	}
	col.parts = make([]*data.Partition, col.db.numParts)
	col.hts = make([]map[string]*data.HashTable, col.db.numParts)
	for i := 0; i < col.db.numParts; i++ {
		col.hts[i] = make(map[string]*data.HashTable)
	}
	col.indexPaths = make(map[string][]string)
	// Open collection document partitions
	for i := 0; i < col.db.numParts; i++ {
		var err error
		if col.parts[i], err = data.OpenPartition(
			path.Join(col.db.path, col.name, DOC_DATA_FILE+strconv.Itoa(i)),
			path.Join(col.db.path, col.name, DOC_LOOKUP_FILE+strconv.Itoa(i))); err != nil {
			return err
		}
	}
	// Look for index directories
	colDirContent, err := ioutil.ReadDir(path.Join(col.db.path, col.name))
	if err != nil {
		return err
	}
	for _, htDir := range colDirContent {
		if !htDir.IsDir() {
			continue
		}
		// Open index partitions
		idxName := htDir.Name()
		idxPath := strings.Split(idxName, INDEX_PATH_SEP)
		col.indexPaths[idxName] = idxPath
		for i := 0; i < col.db.numParts; i++ {
			if col.hts[i] == nil {
				col.hts[i] = make(map[string]*data.HashTable)
			}
			if col.hts[i][idxName], err = data.OpenHashTable(
				path.Join(col.db.path, col.name, idxName, strconv.Itoa(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close all collection files. Do not use the collection afterwards!
func (col *Col) close() error {
	errs := make([]error, 0, 0)
	for i := 0; i < col.db.numParts; i++ {
		col.parts[i].Lock.Lock()
		if err := col.parts[i].Close(); err != nil {
			errs = append(errs, err)
		}
		for _, ht := range col.hts[i] {
			if err := ht.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		col.parts[i].Lock.Unlock()
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

func (col *Col) forEachDoc(fun func(id int, doc []byte) (moveOn bool), placeSchemaLock bool) {
	if placeSchemaLock {
		col.db.schemaLock.RLock()
		defer col.db.schemaLock.RUnlock()
	}
	// Process approx.4k documents in each iteration
	partDiv := col.approxDocCount(false) / col.db.numParts / 4000
	if partDiv == 0 {
		partDiv++
	}
	for iteratePart := 0; iteratePart < col.db.numParts; iteratePart++ {
		part := col.parts[iteratePart]
		part.Lock.RLock()
		for i := 0; i < partDiv; i++ {
			if !part.ForEachDoc(i, partDiv, fun) {
				part.Lock.RUnlock()
				return
			}
		}
		part.Lock.RUnlock()
	}
}

// Do fun for all documents in the collection.
func (col *Col) ForEachDoc(fun func(id int, doc []byte) (moveOn bool)) {
	col.forEachDoc(fun, true)
}

// Create an index on the path.
func (col *Col) Index(idxPath []string) (err error) {
	col.db.schemaLock.Lock()
	defer col.db.schemaLock.Unlock()
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	if _, exists := col.indexPaths[idxName]; exists {
		return fmt.Errorf("Path %v is already indexed", idxPath)
	}
	col.indexPaths[idxName] = idxPath
	idxDir := path.Join(col.db.path, col.name, idxName)
	if err = os.MkdirAll(idxDir, 0700); err != nil {
		return err
	}
	for i := 0; i < col.db.numParts; i++ {
		if col.hts[i][idxName], err = data.OpenHashTable(path.Join(idxDir, strconv.Itoa(i))); err != nil {
			return err
		}
	}
	// Put all documents on the new index
	col.forEachDoc(func(id int, doc []byte) (moveOn bool) {
		var docObj map[string]interface{}
		if err := json.Unmarshal(doc, &docObj); err != nil {
			// Skip corrupted document
			return true
		}
		for _, idxVal := range GetIn(docObj, idxPath) {
			if idxVal != nil {
				hashKey := StrHash(fmt.Sprint(idxVal))
				col.hts[hashKey%col.db.numParts][idxName].Put(hashKey, id)
			}
		}
		return true
	}, false)
	return
}

// Return all indexed paths.
func (col *Col) AllIndexes() (ret [][]string) {
	col.db.schemaLock.RLock()
	defer col.db.schemaLock.RUnlock()
	ret = make([][]string, 0, len(col.indexPaths))
	for _, path := range col.indexPaths {
		pathCopy := make([]string, len(path))
		for i, p := range path {
			pathCopy[i] = p
		}
		ret = append(ret, pathCopy)
	}
	return ret
}

// Remove an index.
func (col *Col) Unindex(idxPath []string) error {
	col.db.schemaLock.Lock()
	defer col.db.schemaLock.Unlock()
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	if _, exists := col.indexPaths[idxName]; !exists {
		return fmt.Errorf("Path %v is not indexed", idxPath)
	}
	delete(col.indexPaths, idxName)
	for i := 0; i < col.db.numParts; i++ {
		col.hts[i][idxName].Close()
		delete(col.hts[i], idxName)
	}
	if err := os.RemoveAll(path.Join(col.db.path, col.name, idxName)); err != nil {
		return err
	}
	return nil
}

func (col *Col) approxDocCount(placeSchemaLock bool) int {
	if placeSchemaLock {
		col.db.schemaLock.RLock()
		defer col.db.schemaLock.RUnlock()
	}
	total := 0
	for _, part := range col.parts {
		part.Lock.RLock()
		total += part.ApproxDocCount()
		part.Lock.RUnlock()
	}
	return total
}

// Return approximate number of documents in the collection.
func (col *Col) ApproxDocCount() int {
	return col.approxDocCount(true)
}

// Divide the collection into roughly equally sized pages, and do fun on all documents in the specified page.
func (col *Col) ForEachDocInPage(page, total int, fun func(id int, doc []byte) bool) {
	col.db.schemaLock.RLock()
	defer col.db.schemaLock.RUnlock()
	for iteratePart := 0; iteratePart < col.db.numParts; iteratePart++ {
		part := col.parts[iteratePart]
		part.Lock.RLock()
		if !part.ForEachDoc(page, total, fun) {
			part.Lock.RUnlock()
			return
		}
		part.Lock.RUnlock()
	}
}
