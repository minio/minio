/* Collection and DB storage management. */
package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	PART_NUM_FILE = "number_of_partitions" // DB-collection-partition-number-configuration file name
)

// Database structures.
type DB struct {
	path       string          // Root path of database directory
	numParts   int             // Total number of partitions
	cols       map[string]*Col // All collections
	schemaLock *sync.RWMutex   // Control access to collection instances.
}

// Open database and load all collections & indexes.
func OpenDB(dbPath string) (*DB, error) {
	rand.Seed(time.Now().UnixNano()) // document ID generation relies on this RNG
	db := &DB{path: dbPath, schemaLock: new(sync.RWMutex)}
	return db, db.load()
}

// Load all collection schema.
func (db *DB) load() error {
	// Create DB directory and PART_NUM_FILE if necessary
	var numPartsAssumed = false
	numPartsFilePath := path.Join(db.path, PART_NUM_FILE)
	if err := os.MkdirAll(db.path, 0700); err != nil {
		return err
	}
	if partNumFile, err := os.Stat(numPartsFilePath); err != nil {
		// The new database has as many partitions as number of CPUs recognized by OS
		if err := ioutil.WriteFile(numPartsFilePath, []byte(strconv.Itoa(runtime.NumCPU())), 0600); err != nil {
			return err
		}
		numPartsAssumed = true
	} else if partNumFile.IsDir() {
		return fmt.Errorf("Database config file %s is actually a directory, is database path correct?", PART_NUM_FILE)
	}
	// Get number of partitions from the text file
	if numParts, err := ioutil.ReadFile(numPartsFilePath); err != nil {
		return err
	} else if db.numParts, err = strconv.Atoi(strings.Trim(string(numParts), "\r\n ")); err != nil {
		return err
	}
	// Look for collection directories and open the collections
	db.cols = make(map[string]*Col)
	dirContent, err := ioutil.ReadDir(db.path)
	if err != nil {
		return err
	}
	for _, maybeColDir := range dirContent {
		if !maybeColDir.IsDir() {
			continue
		}
		if numPartsAssumed {
			return fmt.Errorf("Please manually repair database partition number config file %s", numPartsFilePath)
		}
		if db.cols[maybeColDir.Name()], err = OpenCol(db, maybeColDir.Name()); err != nil {
			return err
		}
	}
	return err
}

// Close all database files. Do not use the DB afterwards!
func (db *DB) Close() error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	errs := make([]error, 0, 0)
	for _, col := range db.cols {
		if err := col.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Create a new collection.
func (db *DB) Create(name string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if _, exists := db.cols[name]; exists {
		return fmt.Errorf("Collection %s already exists", name)
	} else if err := os.MkdirAll(path.Join(db.path, name), 0700); err != nil {
		return err
	} else if db.cols[name], err = OpenCol(db, name); err != nil {
		return err
	}
	return nil
}

// Return all collection names.
func (db *DB) AllCols() (ret []string) {
	db.schemaLock.RLock()
	defer db.schemaLock.RUnlock()
	ret = make([]string, 0, len(db.cols))
	for name, _ := range db.cols {
		ret = append(ret, name)
	}
	return
}

// Use the return value to interact with collection. Return value may be nil if the collection does not exist.
func (db *DB) Use(name string) *Col {
	db.schemaLock.RLock()
	defer db.schemaLock.RUnlock()
	if col, exists := db.cols[name]; exists {
		return col
	}
	return nil
}

// Rename a collection.
func (db *DB) Rename(oldName, newName string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if _, exists := db.cols[oldName]; !exists {
		return fmt.Errorf("Collection %s does not exist", oldName)
	} else if _, exists := db.cols[newName]; exists {
		return fmt.Errorf("Collection %s already exists", newName)
	} else if newName == oldName {
		return fmt.Errorf("Old and new names are the same")
	} else if err := db.cols[oldName].close(); err != nil {
		return err
	} else if err := os.Rename(path.Join(db.path, oldName), path.Join(db.path, newName)); err != nil {
		return err
	} else if db.cols[newName], err = OpenCol(db, newName); err != nil {
		return err
	}
	delete(db.cols, oldName)
	return nil
}

// Truncate a collection - delete all documents and clear
func (db *DB) Truncate(name string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}
	col := db.cols[name]
	for i := 0; i < db.numParts; i++ {
		if err := col.parts[i].Clear(); err != nil {
			return err
		}
		for _, ht := range col.hts[i] {
			if err := ht.Clear(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Scrub a collection - fix corrupted documents and de-fragment free space.
func (db *DB) Scrub(name string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}
	// Prepare a temporary collection in file system
	tmpColName := fmt.Sprintf("scrub-%s-%d", name, time.Now().UnixNano())
	tmpColDir := path.Join(db.path, tmpColName)
	if err := os.MkdirAll(tmpColDir, 0700); err != nil {
		return err
	}
	// Mirror indexes from original collection
	for _, idxPath := range db.cols[name].indexPaths {
		if err := os.MkdirAll(path.Join(tmpColDir, strings.Join(idxPath, INDEX_PATH_SEP)), 0700); err != nil {
			return err
		}
	}
	// Iterate through all documents and put them into the temporary collection
	tmpCol, err := OpenCol(db, tmpColName)
	if err != nil {
		return err
	}
	db.cols[name].forEachDoc(func(id int, doc []byte) bool {
		var docObj map[string]interface{}
		if err := json.Unmarshal([]byte(doc), &docObj); err != nil {
			// Skip corrupted document
			return true
		}
		if err := tmpCol.InsertRecovery(id, docObj); err != nil {
			tdlog.Noticef("Scrub %s: failed to insert back document %v", name, docObj)
		}
		return true
	}, false)
	if err := tmpCol.close(); err != nil {
		return err
	}
	// Replace the original collection with the "temporary" one
	db.cols[name].close()
	if err := os.RemoveAll(path.Join(db.path, name)); err != nil {
		return err
	}
	if err := os.Rename(path.Join(db.path, tmpColName), path.Join(db.path, name)); err != nil {
		return err
	}
	if db.cols[name], err = OpenCol(db, name); err != nil {
		return err
	}
	return nil
}

// Drop a collection and lose all of its documents and indexes.
func (db *DB) Drop(name string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	} else if err := db.cols[name].close(); err != nil {
		return err
	} else if err := os.RemoveAll(path.Join(db.path, name)); err != nil {
		return err
	}
	delete(db.cols, name)
	return nil
}

// Copy this database into destination directory (for backup).
func (db *DB) Dump(dest string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	cpFun := func(currPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			relPath, err := filepath.Rel(db.path, currPath)
			if err != nil {
				return err
			}
			destDir := path.Join(dest, relPath)
			if err := os.MkdirAll(destDir, 0700); err != nil {
				return err
			}
			tdlog.Noticef("Dump: created directory %s", destDir)
		} else {
			src, err := os.Open(currPath)
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(db.path, currPath)
			if err != nil {
				return err
			}
			destPath := path.Join(dest, relPath)
			if _, fileExists := os.Open(destPath); fileExists == nil {
				return fmt.Errorf("Destination file %s already exists", destPath)
			}
			destFile, err := os.Create(destPath)
			if err != nil {
				return err
			}
			written, err := io.Copy(destFile, src)
			if err != nil {
				return err
			}
			tdlog.Noticef("Dump: copied file %s, size is %d", destPath, written)
		}
		return nil
	}
	return filepath.Walk(db.path, cpFun)
}
