// Copyright 2016 (C) Mitchell Hashimoto
// Distributed under the MIT License.

package homedir

import (
	"errors"
	"os"
)

// dir returns the homedir of current user for MS Windows OS.
func dir() (string, error) {
	drive := os.Getenv("HOMEDRIVE")
	path := os.Getenv("HOMEPATH")
	home := drive + path
	if drive == "" || path == "" {
		home = os.Getenv("USERPROFILE")
	}
	if home == "" {
		return "", errors.New("HOMEDRIVE, HOMEPATH, and USERPROFILE are blank")
	}

	return home, nil
}
