// Copyright 2016 (C) Mitchell Hashimoto
// Distributed under the MIT License.

// Package homedir implements a portable function to determine current user's homedir.
package homedir

import (
	"errors"
	"path/filepath"
	"sync"
)

// DisableCache will disable caching of the home directory. Caching is enabled
// by default.
var DisableCache bool

var homedirCache string
var cacheLock sync.Mutex

// Dir returns the home directory for the executing user.
//
// This uses an OS-specific method for discovering the home directory.
// An error is returned if a home directory cannot be detected.
func Dir() (string, error) {
	cacheLock.Lock()
	defer cacheLock.Unlock()

	// Return cached homedir if available.
	if !DisableCache {
		if homedirCache != "" {
			return homedirCache, nil
		}
	}

	// Determine OS speific current homedir.
	result, err := dir()
	if err != nil {
		return "", err
	}

	// Cache for future lookups.
	homedirCache = result
	return result, nil
}

// Expand expands the path to include the home directory if the path
// is prefixed with `~`. If it isn't prefixed with `~`, the path is
// returned as-is.
func Expand(path string) (string, error) {
	if len(path) == 0 {
		return path, nil
	}

	if path[0] != '~' {
		return path, nil
	}

	if len(path) > 1 && path[1] != '/' && path[1] != '\\' {
		return "", errors.New("cannot expand user-specific home dir")
	}

	dir, err := Dir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, path[1:]), nil
}
