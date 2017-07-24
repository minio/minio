/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	msgExt     = ".msg"
	tmpExt     = ".tmp"
	corruptExt = ".CORRUPT"
)

// FileStore implements the store interface using the filesystem to provide
// true persistence, even across client failure. This is designed to use a
// single directory per running client. If you are running multiple clients
// on the same filesystem, you will need to be careful to specify unique
// store directories for each.
type FileStore struct {
	sync.RWMutex
	directory string
	opened    bool
}

// NewFileStore will create a new FileStore which stores its messages in the
// directory provided.
func NewFileStore(directory string) *FileStore {
	store := &FileStore{
		directory: directory,
		opened:    false,
	}
	return store
}

// Open will allow the FileStore to be used.
func (store *FileStore) Open() {
	store.Lock()
	defer store.Unlock()
	// if no store directory was specified in ClientOpts, by default use the
	// current working directory
	if store.directory == "" {
		store.directory, _ = os.Getwd()
	}

	// if store dir exists, great, otherwise, create it
	if !exists(store.directory) {
		perms := os.FileMode(0770)
		merr := os.MkdirAll(store.directory, perms)
		chkerr(merr)
	}
	store.opened = true
	DEBUG.Println(STR, "store is opened at", store.directory)
}

// Close will disallow the FileStore from being used.
func (store *FileStore) Close() {
	store.Lock()
	defer store.Unlock()
	store.opened = false
	DEBUG.Println(STR, "store is closed")
}

// Put will put a message into the store, associated with the provided
// key value.
func (store *FileStore) Put(key string, m packets.ControlPacket) {
	store.Lock()
	defer store.Unlock()
	if !store.opened {
		ERROR.Println(STR, "Trying to use file store, but not open")
		return
	}
	full := fullpath(store.directory, key)
	write(store.directory, key, m)
	if !exists(full) {
		ERROR.Println(STR, "file not created:", full)
	}
}

// Get will retrieve a message from the store, the one associated with
// the provided key value.
func (store *FileStore) Get(key string) packets.ControlPacket {
	store.RLock()
	defer store.RUnlock()
	if !store.opened {
		ERROR.Println(STR, "Trying to use file store, but not open")
		return nil
	}
	filepath := fullpath(store.directory, key)
	if !exists(filepath) {
		return nil
	}
	mfile, oerr := os.Open(filepath)
	chkerr(oerr)
	msg, rerr := packets.ReadPacket(mfile)
	chkerr(mfile.Close())

	// Message was unreadable, return nil
	if rerr != nil {
		newpath := corruptpath(store.directory, key)
		WARN.Println(STR, "corrupted file detected:", rerr.Error(), "archived at:", newpath)
		os.Rename(filepath, newpath)
		return nil
	}
	return msg
}

// All will provide a list of all of the keys associated with messages
// currenly residing in the FileStore.
func (store *FileStore) All() []string {
	store.RLock()
	defer store.RUnlock()
	return store.all()
}

// Del will remove the persisted message associated with the provided
// key from the FileStore.
func (store *FileStore) Del(key string) {
	store.Lock()
	defer store.Unlock()
	store.del(key)
}

// Reset will remove all persisted messages from the FileStore.
func (store *FileStore) Reset() {
	store.Lock()
	defer store.Unlock()
	WARN.Println(STR, "FileStore Reset")
	for _, key := range store.all() {
		store.del(key)
	}
}

// lockless
func (store *FileStore) all() []string {
	if !store.opened {
		ERROR.Println(STR, "Trying to use file store, but not open")
		return nil
	}
	keys := []string{}
	files, rderr := ioutil.ReadDir(store.directory)
	chkerr(rderr)
	for _, f := range files {
		DEBUG.Println(STR, "file in All():", f.Name())
		name := f.Name()
		if name[len(name)-4:len(name)] != msgExt {
			DEBUG.Println(STR, "skipping file, doesn't have right extension: ", name)
			continue
		}
		key := name[0 : len(name)-4] // remove file extension
		keys = append(keys, key)
	}
	return keys
}

// lockless
func (store *FileStore) del(key string) {
	if !store.opened {
		ERROR.Println(STR, "Trying to use file store, but not open")
		return
	}
	DEBUG.Println(STR, "store del filepath:", store.directory)
	DEBUG.Println(STR, "store delete key:", key)
	filepath := fullpath(store.directory, key)
	DEBUG.Println(STR, "path of deletion:", filepath)
	if !exists(filepath) {
		WARN.Println(STR, "store could not delete key:", key)
		return
	}
	rerr := os.Remove(filepath)
	chkerr(rerr)
	DEBUG.Println(STR, "del msg:", key)
	if exists(filepath) {
		ERROR.Println(STR, "file not deleted:", filepath)
	}
}

func fullpath(store string, key string) string {
	p := path.Join(store, key+msgExt)
	return p
}

func tmppath(store string, key string) string {
	p := path.Join(store, key+tmpExt)
	return p
}

func corruptpath(store string, key string) string {
	p := path.Join(store, key+corruptExt)
	return p
}

// create file called "X.[messageid].tmp" located in the store
// the contents of the file is the bytes of the message, then
// rename it to "X.[messageid].msg", overwriting any existing
// message with the same id
// X will be 'i' for inbound messages, and O for outbound messages
func write(store, key string, m packets.ControlPacket) {
	temppath := tmppath(store, key)
	f, err := os.Create(temppath)
	chkerr(err)
	werr := m.Write(f)
	chkerr(werr)
	cerr := f.Close()
	chkerr(cerr)
	rerr := os.Rename(temppath, fullpath(store, key))
	chkerr(rerr)
}

func exists(file string) bool {
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return false
		}
		chkerr(err)
	}
	return true
}
