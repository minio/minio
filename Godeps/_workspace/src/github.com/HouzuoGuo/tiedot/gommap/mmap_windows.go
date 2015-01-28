// Copyright 2011 Evan Shaw. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gommap

import (
	"os"
	"sync"
	"syscall"
)

// mmap on Windows is a two-step process.
// First, we call CreateFileMapping to get a handle.
// Then, we call MapviewToFile to get an actual pointer into memory.
// Because we want to emulate a POSIX-style mmap, we don't want to expose
// the handle -- only the pointer. We also want to return only a byte slice,
// not a struct, so it's convenient to manipulate.

// We keep this map so that we can get back the original handle from the memory address.
var handleLock sync.Mutex
var handleMap = map[uintptr]syscall.Handle{}

// Windows mmap always mapes the entire file regardless of the specified length.
func mmap(length int, hfile uintptr) ([]byte, error) {
	h, errno := syscall.CreateFileMapping(syscall.Handle(hfile), nil, syscall.PAGE_READWRITE, 0, 0, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, 0)
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}
	handleLock.Lock()
	handleMap[addr] = h
	handleLock.Unlock()

	m := MMap{}
	dh := m.header()
	dh.Data = addr
	dh.Len = length
	dh.Cap = length

	return m, nil
}

func unmap(addr, len uintptr) error {
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return err
	}

	handleLock.Lock()
	defer handleLock.Unlock()
	handle := handleMap[addr]
	delete(handleMap, addr)

	return os.NewSyscallError("CloseHandle", syscall.CloseHandle(syscall.Handle(handle)))
}
