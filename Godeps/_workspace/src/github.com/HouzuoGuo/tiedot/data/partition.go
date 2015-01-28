/*
(Collection) Partition is a collection data file accompanied by a hash table in order to allow addressing of a
document using an unchanging ID:
The hash table stores the unchanging ID as entry key and the physical document location as entry value.
*/
package data

import (
	"sync"

	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Partition associates a hash table with collection documents, allowing addressing of a document using an unchanging ID.
type Partition struct {
	col      *Collection
	lookup   *HashTable
	updating map[int]struct{}
	Lock     *sync.RWMutex
}

// Open a collection partition.
func OpenPartition(colPath, lookupPath string) (part *Partition, err error) {
	part = &Partition{updating: make(map[int]struct{}), Lock: new(sync.RWMutex)}
	if part.col, err = OpenCollection(colPath); err != nil {
		return
	} else if part.lookup, err = OpenHashTable(lookupPath); err != nil {
		return
	}
	return
}

// Insert a document. The ID may be used to retrieve/update/delete the document later on.
func (part *Partition) Insert(id int, data []byte) (physID int, err error) {
	physID, err = part.col.Insert(data)
	if err != nil {
		return
	}
	part.lookup.Put(id, physID)
	return
}

// Find and retrieve a document by ID.
func (part *Partition) Read(id int) ([]byte, error) {
	physID := part.lookup.Get(id, 1)

	if len(physID) == 0 {
		return nil, dberr.New(dberr.ErrorNoDoc, id)
	}

	data := part.col.Read(physID[0])

	if data == nil {
		return nil, dberr.New(dberr.ErrorNoDoc, id)
	}

	return data, nil
}

// Update a document.
func (part *Partition) Update(id int, data []byte) (err error) {
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}
	newID, err := part.col.Update(physID[0], data)
	if err != nil {
		return
	}
	if newID != physID[0] {
		part.lookup.Remove(id, physID[0])
		part.lookup.Put(id, newID)
	}
	return
}

// Lock a document for exclusive update.
func (part *Partition) LockUpdate(id int) (err error) {
	if _, alreadyLocked := part.updating[id]; alreadyLocked {
		return dberr.New(dberr.ErrorDocLocked, id)
	}
	part.updating[id] = struct{}{}
	return
}

// Unlock a document to make it ready for the next update.
func (part *Partition) UnlockUpdate(id int) {
	delete(part.updating, id)
}

// Delete a document.
func (part *Partition) Delete(id int) (err error) {
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}
	part.col.Delete(physID[0])
	part.lookup.Remove(id, physID[0])
	return
}

// Partition documents into roughly equally sized portions, and run the function on every document in the portion.
func (part *Partition) ForEachDoc(partNum, totalPart int, fun func(id int, doc []byte) bool) (moveOn bool) {
	ids, physIDs := part.lookup.GetPartition(partNum, totalPart)
	for i, id := range ids {
		data := part.col.Read(physIDs[i])
		if data != nil {
			if !fun(id, data) {
				return false
			}
		}
	}
	return true
}

// Return approximate number of documents in the partition.
func (part *Partition) ApproxDocCount() int {
	totalPart := 24 // not magic; a larger number makes estimation less accurate, but improves performance
	for {
		keys, _ := part.lookup.GetPartition(0, totalPart)
		if len(keys) == 0 {
			if totalPart < 8 {
				return 0 // the hash table is really really empty
			}
			// Try a larger partition size
			totalPart = totalPart / 2
		} else {
			return int(float64(len(keys)) * float64(totalPart))
		}
	}
}

// Clear data file and lookup hash table.
func (part *Partition) Clear() error {

	var err error

	if e := part.col.Clear(); e != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.col.Path, e)

		err = dberr.New(dberr.ErrorIO)
	}

	if e := part.lookup.Clear(); e != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.lookup.Path, e)

		err = dberr.New(dberr.ErrorIO)
	}

	return err
}

// Close all file handles.
func (part *Partition) Close() error {

	var err error

	if e := part.col.Close(); e != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.col.Path, e)
		err = dberr.New(dberr.ErrorIO)
	}
	if e := part.lookup.Close(); e != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.lookup.Path, e)
		err = dberr.New(dberr.ErrorIO)
	}
	return err
}
