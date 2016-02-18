// +build !windows

// Copyright 2016 (C) Mitchell Hashimoto
// Distributed under the MIT License.

package homedir

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
)

// dir returns the homedir of current user for all POSIX compatible
// operating systems.
func dir() (string, error) {
	// First prefer the HOME environmental variable
	if home := os.Getenv("HOME"); home != "" {
		return home, nil
	}

	// user.Current is not implemented for i386 and PNaCL like environments.
	if currUser, err := user.Current(); err == nil {
		return currUser.HomeDir, nil
	}

	// If that fails, try getent
	var stdout bytes.Buffer
	cmd := exec.Command("getent", "passwd", strconv.Itoa(os.Getuid()))
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		// If "getent" is missing, ignore it
		if err == exec.ErrNotFound {
			return "", err
		}
	} else {
		if passwd := strings.TrimSpace(stdout.String()); passwd != "" {
			// username:password:uid:gid:gecos:home:shell
			passwdParts := strings.SplitN(passwd, ":", 7)
			if len(passwdParts) > 5 {
				return passwdParts[5], nil
			}
		}
	}

	// If all else fails, try the shell
	stdout.Reset()
	cmd = exec.Command("sh", "-c", "cd && pwd")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}

	result := strings.TrimSpace(stdout.String())
	if result == "" {
		return "", errors.New("blank output when reading home directory")
	}

	return result, nil
}
