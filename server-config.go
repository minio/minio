/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/minio/go-homedir"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/quick"
)

// configV1
type configV1 struct {
	Version         string `json:"version"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

// configV2
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

func (c *configV2) IsFileLoggingEnabled() bool {
	if c.FileLogger.Filename != "" {
		return true
	}
	return false
}

func (c *configV2) IsSysloggingEnabled() bool {
	if c.SyslogLogger.Network != "" && c.SyslogLogger.Addr != "" {
		return true
	}
	return false
}

func (c *configV2) IsMongoLoggingEnabled() bool {
	if c.MongoLogger.Addr != "" && c.MongoLogger.DB != "" && c.MongoLogger.Collection != "" {
		return true
	}
	return false
}

func (c *configV2) String() string {
	white := color.New(color.FgWhite, color.Bold).SprintfFunc()
	var str string
	if c.IsMongoLoggingEnabled() {
		str = fmt.Sprintf("Mongo -> %s", white("Addr: %s, DB: %s, Collection: %s",
			c.MongoLogger.Addr, c.MongoLogger.DB, c.MongoLogger.Collection))
	}
	if c.IsSysloggingEnabled() {
		str = fmt.Sprintf("Syslog -> %s", white("Addr: %s, Network: %s",
			c.SyslogLogger.Addr, c.SyslogLogger.Network))
	}
	if c.IsFileLoggingEnabled() {
		str = fmt.Sprintf("File -> %s", white("Filename: %s", c.FileLogger.Filename))
	}
	return str
}

func (c *configV2) JSON() string {
	type logger struct {
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
	loggerBytes, err := json.Marshal(logger{
		MongoLogger:  c.MongoLogger,
		SyslogLogger: c.SyslogLogger,
		FileLogger:   c.FileLogger,
	})
	fatalIf(probe.NewError(err), "Unable to marshal logger struct into JSON.", nil)
	return string(loggerBytes)
}

// configPath for custom config path only for testing purposes
var customConfigPath string

// Sets a new config path.
func setGlobalConfigPath(configPath string) {
	customConfigPath = configPath
}

// getConfigPath get users config path
func getConfigPath() (string, *probe.Error) {
	if customConfigPath != "" {
		return customConfigPath, nil
	}
	homeDir, e := homedir.Dir()
	if e != nil {
		return "", probe.NewError(e)
	}
	configPath := filepath.Join(homeDir, ".minio")
	return configPath, nil
}

func mustGetConfigPath() string {
	configPath, err := getConfigPath()
	fatalIf(err.Trace(), "Unable to get config path.", nil)
	return configPath
}

// createConfigPath create users config path
func createConfigPath() *probe.Error {
	configPath, err := getConfigPath()
	if err != nil {
		return err.Trace()
	}
	if e := os.MkdirAll(configPath, 0700); e != nil {
		return probe.NewError(e)
	}
	return nil
}

// getConfigFile get users config file
func getConfigFile() (string, *probe.Error) {
	configPath, err := getConfigPath()
	if err != nil {
		return "", err.Trace()
	}
	return filepath.Join(configPath, "config.json"), nil
}

// saveConfig save config
func saveConfig(a *configV2) *probe.Error {
	configFile, err := getConfigFile()
	if err != nil {
		return err.Trace()
	}
	qc, err := quick.New(a)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Save(configFile); err != nil {
		return err.Trace()
	}
	return nil
}

// loadConfigV2 load config
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

func newConfigV2() *configV2 {
	config := &configV2{}
	config.Version = "2"
	config.Credentials.AccessKeyID = ""
	config.Credentials.SecretAccessKey = ""
	config.Credentials.Region = "us-east-1"
	config.MongoLogger.Addr = ""
	config.MongoLogger.DB = ""
	config.MongoLogger.Collection = ""
	config.SyslogLogger.Network = ""
	config.SyslogLogger.Addr = ""
	config.FileLogger.Filename = ""
	return config
}

func migrateConfig() {
	migrateV1ToV2()
}

func migrateV1ToV2() {
	cv1, err := loadConfigV1()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			return
		}
	}
	fatalIf(err.Trace(), "Unable to load config version ‘1’.", nil)

	if cv1.Version != "1" {
		fatalIf(probe.NewError(errors.New("")), "Invalid version loaded ‘"+cv1.Version+"’.", nil)
	}

	cv2 := newConfigV2()
	cv2.Credentials.AccessKeyID = cv1.AccessKeyID
	cv2.Credentials.SecretAccessKey = cv1.SecretAccessKey
	err = saveConfig(cv2)
	fatalIf(err.Trace(), "Unable to save config version ‘2’.", nil)

	console.Println("Migration from version ‘1’ to ‘2’ completed successfully.")

	/// Purge old fsUsers.json file
	configPath, err := getConfigPath()
	fatalIf(err.Trace(), "Unable to retrieve config path.", nil)

	configFile := filepath.Join(configPath, "fsUsers.json")
	os.RemoveAll(configFile)
}
