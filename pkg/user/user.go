package user

import (
	"os"
	"os/user"
	"runtime"
	"strconv"
)

// Current is a portable implementation to determine the current user.
// Golang's user.Current does not work reliably under docker or 32bit linux
//
// Two issues this code handles :-
//
//   Docker Container - For static binaries NSS library will not be a part of the static binary hence user.Current() fails.
//   Linux Intel 32 bit - CGO is not enabled so it will not link with NSS library.
//
func Current() (*user.User, error) {
	if os.Getenv("DOCKERIMAGE") == "1" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		return &user.User{Uid: "0", Gid: "0", Username: "root", Name: "root", HomeDir: wd}, nil
	}
	if runtime.GOARCH == "386" && runtime.GOOS == "linux" {
		return &user.User{
			Uid:      strconv.Itoa(os.Getuid()),
			Gid:      strconv.Itoa(os.Getgid()),
			Username: os.Getenv("USER"),
			Name:     os.Getenv("USER"),
			HomeDir:  os.Getenv("HOME"),
		}, nil
	}
	user, e := user.Current()
	if e != nil {
		return nil, e
	}
	return user, nil
}

// HomeDir - return current home directory.
func HomeDir() (string, error) {
	user, err := Current()
	if err != nil {
		return "", err
	}
	return user.HomeDir, nil
}
