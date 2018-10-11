/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
 *
 */

package configuration

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/minio/minio/cmd/logger"
)

var serverConfHandler ServerConfigHandlers

// var serverConf ServerConfig

type configKey interface {
	// Get honors only the new keys, not legacy keys, but
	// internally reads legacy settings and returns new
	// keys for legacy settings.
	//
	// Get(key string) (string, error)

	// Set honors both new and legacy keys. Check/validation logic
	// is also included in Set function.

	// The followings are the risks supporting both new and
	// legacy keys at the same time.
	// 1. The same config entry might have been set 2 different,
	// values, in which case, we'll honor the new key settings.
	// 2. Conflict btw GET and SET responses to legacy keys:
	// Get returns error message that the key is invalid while
	// Set sets the value for a legacy key.
	Set(key, value string, cfg ServerConfig) error
	Help(key string) (helpText string, err error) //template like mc --help
}

// ServerConfig kv has leaf node/key names and their values
type ServerConfig struct {
	rwMutex *sync.RWMutex
	kv      map[string]string
}

// ServerConfigHandlers is the mux that routes to the key
// method that is going to be executed
type ServerConfigHandlers map[string]configKey

// Define Set and Help functions for each leaf node
// =REGION=  >>>>>>
type regionKey string

func (r regionKey) Set(key, val string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (r regionKey) Help(key string) (string, error) {
	return "Display help information for \"region\"", nil
}

// >>>>>>> =REGION=

// =VERSION= >>>>>>
type versionKey string

func (r versionKey) Set(key, val string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (r versionKey) Help(key string) (string, error) {
	return "Display help information for \"version\"", nil
}

// >>>>>>> =VERSION=

// =LOG.CONSOLE.ENABLE= >>>>>>
type logConsoleEnableKey string

func (l logConsoleEnableKey) Set(key, val string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logConsoleEnableKey) Help(key string) (string, error) {
	return "Display help information for \"log.console.enable\"", nil
}

// >>>>>>> =LOG.HTTP.*.ENABLE=

// =LOG.HTTP.*.ENABLE= >>>>>>
type logHTTPAnyEnableKey string

func (l logHTTPAnyEnableKey) Set(key, val string, cfg ServerConfig) error {
	cfg.kv[key] = val
	return nil
}
func (l logHTTPAnyEnableKey) Help(key string) (string, error) {
	return "Display help information for \"log.http.*.enable\"", nil
}

// >>>>>>> =LOG.HTTP.*.ENABLE=

// =LOGGER.HTTP.*.ENABLE= >>>>>>
type loggerHTTPAnyEnableKey string

func (l loggerHTTPAnyEnableKey) Set(key, val string, cfg ServerConfig) error {
	// This is a deprecated key function. It'll still stay
	// active, but we save the value in "log.http.*enable"
	key = strings.Replace(key, "logger", "log", 1)
	cfg.kv[key] = val
	logger.LogIf(context.Background(), errors.New("Key name \"logger\" is DEPRECATED!\nWrote the value in \"log.http.*.enable\""))
	return nil
}
func (l loggerHTTPAnyEnableKey) Help(key string) (string, error) {
	return "No help information for DEPRECATED \"logger.http.*.enable\"", nil
}

// >>>>>>> =LOGGER.HTTP.*.ENABLE=

func checkRandomKeyValidity(key string, cfgHandler ServerConfigHandlers) (string, error) {
	// Keys with user specified random subkey will be
	// validated in this function. If valid, key will also be tranformed
	// to "transformedKey" with random subkey replaced with a "*"
	var transformedKey string
	// Number of characters allowed in a user specified
	// random subkey must be in between 3 and 64
	minNoOfChrs := "2"  // 1 less than the actual value, 3.
	maxNoOfChrs := "63" // 1 less than the actual value, 64.

	// Variable to hold the regular expression
	var r *regexp.Regexp
	// Base regular expression for both "log" and
	// "notify" keys with a user specified random subkey
	basePattern := "\\.([a-zA-Z][0-9a-zA-Z]{" + minNoOfChrs + "," + maxNoOfChrs + "})\\.(enable|"

	// Decide if the key is a child of "log" or "notify"
	splitKey := strings.Split(key, ".")
	switch splitKey[0] {
	case "log":
		// Log key regular expression is used to replace the
		// user specified random subKey with a "*"
		// Example:
		// key = "log.http.target1.enable" where "target1" is the user specified
		// random key. If key is valid, it'll be transformed into "log.http.*.enable",
		// which will  be used to get/set the value using ServerConfig
		suffixPattern := "endpoint)$"
		r, _ = regexp.Compile("(log\\.http)" + basePattern + suffixPattern)
	case "notify":
		// Second key after "notify" key
		switch splitKey[1] {
		case "amqp":
			suffixPattern := "url|exchange|routingKey|exchangeType|deliveryMode|mandatory|immediate|durable|internal|noWait|autoDeleted)$"
			r, _ = regexp.Compile("(notify\\.amqp)" + basePattern + suffixPattern)
		case "elasticsearch":
			suffixPattern := "format|url|index)$"
			r, _ = regexp.Compile("(notify\\.elasticsearch)" + basePattern + suffixPattern)
		case "kafka":
			suffixPattern := "brokers|topic|tls\\.enable|tls\\.skipVerify|tls\\.clientAuth|sasl\\.enable|sasl\\.username|sasl\\.password)$"
			r, _ = regexp.Compile("(notify\\.kafka)" + basePattern + suffixPattern)
		case "mqtt":
			suffixPattern := "broker|topic|qos|clientId|username|password|reconnectInterval|keepAliveInterval)$"
			r, _ = regexp.Compile("(notify\\.mqtt)" + basePattern + suffixPattern)
		case "mysql":
			suffixPattern := "format|dsnString|table|host|port|user|password|database)$"
			r, _ = regexp.Compile("(notify\\.mysql)" + basePattern + suffixPattern)
		case "nats":
			suffixPattern := "address|subject|username|password|token|secure|pingInterval|streaming\\.enable|streaming\\.clusterID|streaming\\.clientID|streaming\\.async|streaming\\.maxPubAcksInflight)$"
			r, _ = regexp.Compile("(notify\\.nats)" + basePattern + suffixPattern)
		case "postgresql":
			suffixPattern := "format|connectionString|table|host|port|user|password|database)$"
			r, _ = regexp.Compile("(notify\\.postgresql)" + basePattern + suffixPattern)
		case "redis":
			suffixPattern := "endpoint|format|address|password|key)$"
			r, _ = regexp.Compile("(notify\\.redis)" + basePattern + suffixPattern)
		case "webhook":
			suffixPattern := "endpoint)$"
			r, _ = regexp.Compile("(notify\\.webhook)" + basePattern + suffixPattern)
		}
	default:
		// Unexpected key name.
		logger.LogIf(context.Background(), errors.New("Invalid key:"+key))
		// Just assume to match the full key name
		r, _ = regexp.Compile(key)
	}

	// Get submatched subkeys using regular expression decided above
	// FindStringSubmatch is expected to generate a slice with the
	// following information:
	// [fullKey, keysBeforeRandomKey, randomKey, keysAfterRandomKey]
	matchedKeys := r.FindStringSubmatch(key)
	// Initialization
	randomKey := ""
	// Less than 3 matches in slice means key is invalid
	if len(matchedKeys) < 3 {
		logger.LogIf(context.Background(), errors.New("Invalid key:"+key))
		return "", errors.New("ERROR: Invalid key, " + key)
	}
	randomKey = matchedKeys[2]

	// Replace found user specified random subKey with a "*".
	// The transformedKey is the new key for ServerConfigHandlers map.
	transformedKey = strings.Replace(matchedKeys[0], randomKey, "*", 1)

	if _, ok := cfgHandler[transformedKey]; ok {
		return transformedKey, nil
	}
	return "", errors.New("ERROR: Invalid key, " + key)

}

func init() {
	var regionK regionKey
	var versionK versionKey
	var logConsoleEnableK logConsoleEnableKey
	var logHTTPAnyEnableK logHTTPAnyEnableKey
	var loggerHTTPAnyEnableK loggerHTTPAnyEnableKey
	// :
	// :
	// :

	serverConfHandler = ServerConfigHandlers{}

	// Register Set and Help commands for each leaf node
	serverConfHandler["version"] = versionK
	serverConfHandler["region"] = regionK
	serverConfHandler["log.console.enable"] = logConsoleEnableK
	serverConfHandler["log.http.*.enable"] = logHTTPAnyEnableK
	// Key "logger" is deprecated. The new key is "log"
	serverConfHandler["logger.http.*.enable"] = loggerHTTPAnyEnableK
	// :
	// :
	// :

}

// SetHandler sets key value in server configuration database
func (s *ServerConfig) SetHandler(key, val string) (map[string]string, error) {
	var err error
	// Validate assuming the key is a regular key with no user specified
	// random subkey in it. If this fails (else), try validating the key
	// assuming it has a user specified random subkey in it.
	if _, ok := serverConfHandler[key]; ok {
		// Load the configuration data from disk into memory
		if err = s.Load(); err != nil {
			return map[string]string{}, err
		}

		// Set the key/value pair in memory
		if err = serverConfHandler[key].Set(key, val, *s); err != nil {
			return map[string]string{}, err
		}

		// Save the set/modified configuration in memory to the disk
		if err = s.Save(s.kv); err != nil {
			return map[string]string{}, err
		}
		return s.kv, nil

	} else if transformedKey, err := checkRandomKeyValidity(key, serverConfHandler); err == nil {
		// Validity check for keys with user specified random subkey
		if err = serverConfHandler[transformedKey].Set(key, val, *s); err != nil {
			return map[string]string{}, err
		}
	}
	// Failed to validate. Couldn't find a Set command for the key
	err = errors.New("Failed to set key: " + key + ". No set function defined.")
	logger.LogIf(context.Background(), err)
	return map[string]string{}, err
}

// GetHandler gets single or multiple or full configuration info
func (s *ServerConfig) GetHandler() (map[string]string, error) {
	// Load the configuration data from disk into memory
	if err := s.Load(); err != nil {
		return map[string]string{}, err
	}
	fmt.Printf("Inside GetHandler > %+v\n\n", s.kv)
	return s.kv, nil
}

// HelpHandler displays key name, its type/syntax and
// a short description for its purpose
func (s *ServerConfig) HelpHandler(key string) (string, error) {
	// Validate assuming it is a regular key with no user specified
	// random subkey. If it fails, try validating the key assuming it
	// has a user specified random subkey
	var helpText string
	var err error
	if _, ok := serverConfHandler[key]; ok {
		if helpText, err = serverConfHandler[key].Help(key); err != nil {
			return "", err
		}
	} else if transformedKey, err := checkRandomKeyValidity(key, serverConfHandler); err == nil {
		// Validity check for keys with a user specified random subkey
		if helpText, err = serverConfHandler[transformedKey].Help(key); err != nil {
			return "", err
		}
	}
	return helpText, nil
}

// Load loads configuration from disk to memory (serverConfig.kv)
func (s *ServerConfig) Load() error {
	var elements []string
	s.kv = make(map[string]string)
	dataFile := "/home/ersan/work/src/github.com/minio/minio/pkg/configuration/examples/diskData.txt"
	// dataFile := "../diskData.txt"

	// Check if configuration file exists
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		// Do nothing. No configuration file found.
	} else {
		// Configuration file exists.
		// Read configuration data from etcd or file
		dat, err := ioutil.ReadFile(dataFile)
		if err != nil {
			return err
		}

		// Strip off equal sign and surrounding white spaces
		elements = strings.Fields(strings.Replace(string(dat), " = ", " ", -1))

		// Set the server configuration map, "s.kv",
		// by going through each slice element
		for i := 0; i < len(elements); i += 2 {
			// Set the key/value pair in memory
			key := elements[i]
			val := elements[i+1]
			// Validate assuming the key is a regular key with no user specified
			// random subkey in it. If this fails (else), try validating the key
			// assuming it has a user specified random subkey in it.
			if _, ok := serverConfHandler[key]; ok {
				if err := serverConfHandler[key].Set(key, val, *s); err != nil {
					// Report the error and continue with the next element
					logger.LogIf(context.Background(), err)
				}
				continue
			} else if transformedKey, err := checkRandomKeyValidity(key, serverConfHandler); err == nil {
				// Validity check for keys with user specified random subkey
				if err := serverConfHandler[transformedKey].Set(key, val, *s); err != nil {
					// Report the error and continue with the next element
					logger.LogIf(context.Background(), err)
				}
				continue
			}
			logger.LogIf(context.Background(), errors.New("Invalid key: "+key))
		}
	}
	fmt.Println("Inside Load > Loaded values in database.: ", s.kv)
	return nil
}

// Save saves configuration info into disk
func (s *ServerConfig) Save(kv map[string]string) error {
	var byteData []byte
	for k, v := range kv {
		byteData = append(byteData, []byte(k+" = "+v+"\n")...)
	}
	if err := ioutil.WriteFile("./diskDataNew.txt", byteData, 0744); err != nil {
		return err
	}
	return nil
}
