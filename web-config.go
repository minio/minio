package main

import (
	"os"
	"path/filepath"

	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/user"
)

var customWebConfigDir = ""

// getWebConfigDir get web config dir.
func getWebConfigDir() (string, *probe.Error) {
	if customWebConfigDir != "" {
		return customWebConfigDir, nil
	}
	homeDir, e := user.HomeDir()
	if e != nil {
		return "", probe.NewError(e)
	}
	webConfigDir := filepath.Join(homeDir, ".minio", "web")
	return webConfigDir, nil
}

func mustGetWebConfigDir() string {
	webConfigDir, err := getWebConfigDir()
	fatalIf(err.Trace(), "Unable to get config path.", nil)
	return webConfigDir
}

// createWebConfigDir create users config path
func createWebConfigDir() *probe.Error {
	webConfigDir, err := getWebConfigDir()
	if err != nil {
		return err.Trace()
	}
	if err := os.MkdirAll(webConfigDir, 0700); err != nil {
		return probe.NewError(err)
	}
	return nil
}

func mustGetPrivateKeyPath() string {
	webConfigDir, err := getWebConfigDir()
	fatalIf(err.Trace(), "Unable to get config path.", nil)
	return webConfigDir + "/private.key"
}

func mustGetPublicKeyPath() string {
	webConfigDir, err := getWebConfigDir()
	fatalIf(err.Trace(), "Unable to get config path.", nil)
	return webConfigDir + "/public.key"
}
