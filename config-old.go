package main

import (
	"os"
	"path/filepath"

	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/quick"
)

/////////////////// Config V1 ///////////////////
type configV1 struct {
	Version         string `json:"version"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

// loadConfigV1 load config
func loadConfigV1() (*configV1, *probe.Error) {
	configPath, err := getConfigPath()
	if err != nil {
		return nil, err.Trace()
	}
	configFile := filepath.Join(configPath, "fsUsers.json")
	if _, err := os.Stat(configFile); err != nil {
		return nil, probe.NewError(err)
	}
	a := &configV1{}
	a.Version = "1"
	qc, err := quick.New(a)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(configFile); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*configV1), nil
}

/////////////////// Config V2 ///////////////////
type configV2 struct {
	Version     string `json:"version"`
	Credentials struct {
		AccessKeyID     string `json:"accessKeyId"`
		SecretAccessKey string `json:"secretAccessKey"`
		Region          string `json:"region"`
	} `json:"credentials"`
	MongoLogger struct {
		Addr       string `json:"addr"`
		DB         string `json:"db"`
		Collection string `json:"collection"`
	} `json:"mongoLogger"`
	SyslogLogger struct {
		Network string `json:"network"`
		Addr    string `json:"addr"`
	} `json:"syslogLogger"`
	FileLogger struct {
		Filename string `json:"filename"`
	} `json:"fileLogger"`
}

// loadConfigV2 load config version '2'.
func loadConfigV2() (*configV2, *probe.Error) {
	configFile, err := getConfigFile()
	if err != nil {
		return nil, err.Trace()
	}
	if _, err := os.Stat(configFile); err != nil {
		return nil, probe.NewError(err)
	}
	a := &configV2{}
	a.Version = "2"
	qc, err := quick.New(a)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(configFile); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*configV2), nil
}
