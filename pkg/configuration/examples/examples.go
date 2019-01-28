/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/configuration"
)

const (
	confFile    = "/var/tmp/ersan/.minio.sys/config/config.txt"
	commentChar = "##"
)

func np(key, val, comment string) {
	if comment != "" {
		fmt.Printf("%s = \"%s\"    \"%s\"\n", key, val, comment)
	} else {
		fmt.Printf("%s = \"%s\"\n", key, val)
	}
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// writeLines writes the lines to the given file.
func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func verifyConfigKeyFormat(entry string) (entryArr []string, isComment, validConfigFormat bool) {
	// Regexp to match full line comments, and the
	// comments which start in the middle of the line
	rComment, _ := regexp.Compile("^[\\s]*$|^[\\s]*(" + commentChar + ".*)|^[\\s]*([^\\s]+)([\\s]*=[\\s]*)([^\\s]+)[\\s]+(" + commentChar + ".*)")
	matchedComment := rComment.FindStringSubmatch(entry)

	if len(matchedComment) > 0 {
		// Line entry might be a comment.
		// Comments are handled in this block.
		for i, m := range matchedComment {
			// Cleanup leading and trailing white spaces
			matchedComment[i] = strings.TrimSpace(m)
			if matchedComment[0] == "" {
				// This is an empty line.
				// Treat them as if they are comments and return
				// true for both "isComment" and "validConfigFormat"
				return matchedComment, true, true
			}
			if matchedComment[1] == "" {
				// Mixed text and comment in the same line
				// It is also a key/value setting.
				// Return false for "isComment" and true for "validConfigFormat".
				return []string{matchedComment[2], matchedComment[4], matchedComment[5]}, false, true
			}
			// Pure comments are handled here
			return strings.Split(matchedComment[1], " "), true, true
		}
	}
	// Handle lines with only key=val pairs here
	r, _ := regexp.Compile("^[\\s]*([^\\s]+)([\\s]*=[\\s]*)([^\\s]*)")
	matchedKey := r.FindStringSubmatch(entry)
	if len(matchedKey) == 4 {
		// So, this is not a comment and a valid key=value format
		return []string{matchedKey[1], matchedKey[3]}, false, true
	}
	// The line is something we do not support
	return strings.Split(entry, " "), false, false
}

func load(configFile string, s *configuration.ServerConfig) map[string]error {
	// Check if configuration file exists
	if _, err := os.Stat(confFile); os.IsNotExist(err) {
		// Return right away if configuration file doesn't exist
		return map[string]error{"Error locating config file": err}
	}

	errMap := make(map[string]error)
	// Configuration file exists.
	// Read configuration data from etcd or file
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	lines, err := readLines(confFile)
	if err != nil {
		// Return right away if something is wrong
		// with reading the configuration file
		return map[string]error{"Error reading config file": err}
	}

	var key, val string
	// Go through each line of config file and
	// classify them as comments or key/value pairs.
	// Only empty lines, comment lines (<commentChar> xxxx xx x),
	// key/value pairs (key = value) and combination of
	// key/value pairs and comments in the same line
	// (key = value <commentChar> xxxx  xxx) are allowed valid
	// lines in a configuration file. Any other syntax are ignored.
	for ind, line := range lines {
		element, isComment, isValidConfigFormat := verifyConfigKeyFormat(line)

		if isComment {
			// We've decided not to support full comment lines
			// and empty lines. Skip it and continue.
			continue
		}

		key = element[0]
		val = element[1]
		if !isValidConfigFormat {
			logger.LogIf(context.Background(), errors.New("Invalid key, '"+key+"'."))
			errMap["Invalid key: "+key] = errors.New("Invalid key, '" +
				key + "'. (line:" + strconv.Itoa(ind+1) + ")")
			continue
		}
		// Valid configuration key/value format
		// Set the value for the key.
		// Set also does input validation for the given key value
		comment := ""
		if len(element) > 2 {
			comment = element[2]
		}
		if err := s.Set(key, val, comment); err != nil {
			logger.LogIf(context.Background(), errors.New("Invalid value, '"+key+" = "+val+"'."))
			errMap["Invalid value: "] = errors.New("Invalid value, '" +
				key + " = " + val + "'. (line:" + strconv.Itoa(ind+1) + ")")
			continue
		}
	}

	// Display error messages if errArr has errors collected
	// during loading process of configuration file into memory
	if len(errMap) > 0 {
		return errMap
	}

	return nil
}

// Define Handler methods Check (for input validation)
// and Help methods for all keys
type versionHandler struct{}

func (versionHandler) Check(val string) error {
	// fmt.Println("version Handler=>Check method registered")
	return nil
}
func (versionHandler) Help() (string, error) {
	// fmt.Println("version Handler=>Help method registered")
	return "", nil
}

type credentialAccessKeyHandler struct{}

func (credentialAccessKeyHandler) Check(val string) error {
	// fmt.Println("credentialAccessKey Handler=>Check method registered")
	return nil
}
func (credentialAccessKeyHandler) Help() (string, error) {
	// fmt.Println("credentialAccessKey Handler=>Help method registered")
	return "", nil
}

type credentialSecretKeyHandler struct{}

func (credentialSecretKeyHandler) Check(val string) error {
	// fmt.Println("credentialSecretKey Handler=>Check method registered")
	return nil
}
func (credentialSecretKeyHandler) Help() (string, error) {
	// fmt.Println("credentialSecretKey Handler=>Help method registered")
	return "", nil
}

type regionHandler struct{}

func (regionHandler) Check(val string) error {
	// fmt.Println("region Handler=>Check method registered")
	return nil
}
func (regionHandler) Help() (string, error) {
	// fmt.Println("region Handler=>Help method registered")
	return "", nil
}

type browserHandler struct{}

func (browserHandler) Check(val string) error {
	// fmt.Println("browser Handler=>Check method registered")
	return nil
}
func (browserHandler) Help() (string, error) {
	// fmt.Println("browser Handler=>Help method registered")
	return "", nil
}

type wormHandler struct{}

func (wormHandler) Check(val string) error {
	// fmt.Println("worm Handler=>Check method registered")
	return nil
}
func (wormHandler) Help() (string, error) {
	// fmt.Println("worm Handler=>Help method registered")
	return "", nil
}

type domainHandler struct{}

func (domainHandler) Check(val string) error {
	// fmt.Println("domain Handler=>Check method registered")
	return nil
}
func (domainHandler) Help() (string, error) {
	// fmt.Println("domain Handler=>Help method registered")
	return "", nil
}

type storageClassStandardHandler struct{}

func (storageClassStandardHandler) Check(val string) error {
	// fmt.Println("storage.class.Standard Handler=>Check method registered")
	return nil
}
func (storageClassStandardHandler) Help() (string, error) {
	// fmt.Println("storage.class.Standard Handler=>Help method registered")
	return "", nil
}

type storageClassRrsHandler struct{}

func (storageClassRrsHandler) Check(val string) error {
	// fmt.Println("storage.class.Rrs Handler=>Check method registered")
	return nil
}
func (storageClassRrsHandler) Help() (string, error) {
	// fmt.Println("storage.class.Rrs Handler=>Help method registered")
	return "", nil
}

type cacheDrivesHandler struct{}

func (cacheDrivesHandler) Check(val string) error {
	// fmt.Println("cacheDrives Handler=>Check method registered")
	return nil
}
func (cacheDrivesHandler) Help() (string, error) {
	// fmt.Println("cacheDrives Handler=>Help method registered")
	return "", nil
}

type cacheExpiryHandler struct{}

func (cacheExpiryHandler) Check(val string) error {
	// fmt.Println("cacheExpiry Handler=>Check method registered")
	return nil
}
func (cacheExpiryHandler) Help() (string, error) {
	// fmt.Println("cacheExpiry Handler=>Help method registered")
	return "", nil
}

type cacheMaxuseHandler struct{}

func (cacheMaxuseHandler) Check(val string) error {
	// fmt.Println("cacheMaxuse Handler=>Check method registered")
	return nil
}
func (cacheMaxuseHandler) Help() (string, error) {
	// fmt.Println("cacheMaxuse Handler=>Help method registered")
	return "", nil
}

type cacheExcludeHandler struct{}

func (cacheExcludeHandler) Check(val string) error {
	// fmt.Println("cacheExclude Handler=>Check method registered")
	return nil
}
func (cacheExcludeHandler) Help() (string, error) {
	// fmt.Println("cacheExclude Handler=>Help method registered")
	return "", nil
}

type kmsVaultEndpointHandler struct{}

func (kmsVaultEndpointHandler) Check(val string) error {
	// fmt.Println("kmsVaultEndpoint Handler=>Check method registered")
	return nil
}
func (kmsVaultEndpointHandler) Help() (string, error) {
	// fmt.Println("kmsVaultEndpoint Handler=>Help method registered")
	return "", nil
}

type kmsVaultAuthTypeHandler struct{}

func (kmsVaultAuthTypeHandler) Check(val string) error {
	// fmt.Println("kmsVaultAuthType Handler=>Check method registered")
	return nil
}
func (kmsVaultAuthTypeHandler) Help() (string, error) {
	// fmt.Println("kmsVaultAuthType Handler=>Help method registered")
	return "", nil
}

type kmsVaultAuthApproleIDHandler struct{}

func (kmsVaultAuthApproleIDHandler) Check(val string) error {
	// fmt.Println("kmsVaultAuthApproleID Handler=>Check method registered")
	return nil
}
func (kmsVaultAuthApproleIDHandler) Help() (string, error) {
	// fmt.Println("kmsVaultAuthApproleID Handler=>Help method registered")
	return "", nil
}

type kmsVaultAuthApproleSecretHandler struct{}

func (kmsVaultAuthApproleSecretHandler) Check(val string) error {
	// fmt.Println("kmsVaultAuthApproleSecret Handler=>Check method registered")
	return nil
}
func (kmsVaultAuthApproleSecretHandler) Help() (string, error) {
	// fmt.Println("kmsVaultAuthApproleSecret Handler=>Help method registered")
	return "", nil
}

type kmsVaultKeyIDNameHandler struct{}

func (kmsVaultKeyIDNameHandler) Check(val string) error {
	// fmt.Println("kmsVaultKeyIDName Handler=>Check method registered")
	return nil
}
func (kmsVaultKeyIDNameHandler) Help() (string, error) {
	// fmt.Println("kmsVaultKeyIDName Handler=>Help method registered")
	return "", nil
}

type kmsVaultKeyIDVersionHandler struct{}

func (kmsVaultKeyIDVersionHandler) Check(val string) error {
	// fmt.Println("kmsVaultKeyIDVersion Handler=>Check method registered")
	return nil
}
func (kmsVaultKeyIDVersionHandler) Help() (string, error) {
	// fmt.Println("kmsVaultKeyIDVersion Handler=>Help method registered")
	return "", nil
}

type notifyAmqpHandler struct{}

func (notifyAmqpHandler) Check(val string) error {
	// fmt.Println("notifyAmqp Handler=>Check method registered")
	return nil
}
func (notifyAmqpHandler) Help() (string, error) {
	// fmt.Println("notifyAmqp Handler=>Help method registered")
	return "", nil
}

type notifyAmqpURLHandler struct{}

func (notifyAmqpURLHandler) Check(val string) error {
	// fmt.Println("notifyAmqpURL Handler=>Check method registered")
	return nil
}
func (notifyAmqpURLHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpURL Handler=>Help method registered")
	return "", nil
}

type notifyAmqpExchangeHandler struct{}

func (notifyAmqpExchangeHandler) Check(val string) error {
	// fmt.Println("notifyAmqpExchange Handler=>Check method registered")
	return nil
}
func (notifyAmqpExchangeHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpExchange Handler=>Help method registered")
	return "", nil
}

type notifyAmqpRoutingKeyHandler struct{}

func (notifyAmqpRoutingKeyHandler) Check(val string) error {
	// fmt.Println("notifyAmqpRoutingKey Handler=>Check method registered")
	return nil
}
func (notifyAmqpRoutingKeyHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpRoutingKey Handler=>Help method registered")
	return "", nil
}

type notifyAmqpExchangeTypeHandler struct{}

func (notifyAmqpExchangeTypeHandler) Check(val string) error {
	// fmt.Println("notifyAmqpExchangeType Handler=>Check method registered")
	return nil
}
func (notifyAmqpExchangeTypeHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpExchangeType Handler=>Help method registered")
	return "", nil
}

type notifyAmqpDeliveryModeHandler struct{}

func (notifyAmqpDeliveryModeHandler) Check(val string) error {
	// fmt.Println("notifyAmqpDeliveryMode Handler=>Check method registered")
	return nil
}
func (notifyAmqpDeliveryModeHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpDeliveryMode Handler=>Help method registered")
	return "", nil
}

type notifyAmqpMandatoryHandler struct{}

func (notifyAmqpMandatoryHandler) Check(val string) error {
	// fmt.Println("notifyAmqpMandatory Handler=>Check method registered")
	return nil
}
func (notifyAmqpMandatoryHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpMandatory Handler=>Help method registered")
	return "", nil
}

type notifyAmqpImmediateHandler struct{}

func (notifyAmqpImmediateHandler) Check(val string) error {
	// fmt.Println("notifyAmqpImmediate Handler=>Check method registered")
	return nil
}
func (notifyAmqpImmediateHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpImmediate Handler=>Help method registered")
	return "", nil
}

type notifyAmqpDurableHandler struct{}

func (notifyAmqpDurableHandler) Check(val string) error {
	// fmt.Println("notifyAmqpDurable Handler=>Check method registered")
	return nil
}
func (notifyAmqpDurableHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpDurable Handler=>Help method registered")
	return "", nil
}

type notifyAmqpInternalHandler struct{}

func (notifyAmqpInternalHandler) Check(val string) error {
	// fmt.Println("notifyAmqpInternal Handler=>Check method registered")
	return nil
}
func (notifyAmqpInternalHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpInternal Handler=>Help method registered")
	return "", nil
}

type notifyAmqpNoWaitHandler struct{}

func (notifyAmqpNoWaitHandler) Check(val string) error {
	// fmt.Println("notifyAmqpNoWait Handler=>Check method registered")
	return nil
}
func (notifyAmqpNoWaitHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpNoWait Handler=>Help method registered")
	return "", nil
}

type notifyAmqpAutoDeletedHandler struct{}

func (notifyAmqpAutoDeletedHandler) Check(val string) error {
	// fmt.Println("notifyAmqpAutoDeleted Handler=>Check method registered")
	return nil
}
func (notifyAmqpAutoDeletedHandler) Help() (string, error) {
	// fmt.Println("notifyAmqpAutoDeleted Handler=>Help method registered")
	return "", nil
}

type notifyElasticsearchHandler struct{}

func (notifyElasticsearchHandler) Check(val string) error {
	// fmt.Println("notifyElasticsearch Handler=>Check method registered")
	return nil
}
func (notifyElasticsearchHandler) Help() (string, error) {
	// fmt.Println("notifyElasticsearch Handler=>Help method registered")
	return "", nil
}

type notifyElasticsearchFormatHandler struct{}

func (notifyElasticsearchFormatHandler) Check(val string) error {
	// fmt.Println("notifyElasticsearchFormat Handler=>Check method registered")
	return nil
}
func (notifyElasticsearchFormatHandler) Help() (string, error) {
	// fmt.Println("notifyElasticsearchFormat Handler=>Help method registered")
	return "", nil
}

type notifyElasticsearchURLHandler struct{}

func (notifyElasticsearchURLHandler) Check(val string) error {
	// fmt.Println("notifyElasticsearchURL Handler=>Check method registered")
	return nil
}
func (notifyElasticsearchURLHandler) Help() (string, error) {
	// fmt.Println("notifyElasticsearchURL Handler=>Help method registered")
	return "", nil
}

type notifyElasticsearchIndexHandler struct{}

func (notifyElasticsearchIndexHandler) Check(val string) error {
	// fmt.Println("notifyElasticsearchIndex Handler=>Check method registered")
	return nil
}
func (notifyElasticsearchIndexHandler) Help() (string, error) {
	// fmt.Println("notifyElasticsearchIndex Handler=>Help method registered")
	return "", nil
}

type notifyKafkaHandler struct{}

func (notifyKafkaHandler) Check(val string) error {
	// fmt.Println("notifyKafka Handler=>Check method registered")
	return nil
}
func (notifyKafkaHandler) Help() (string, error) {
	// fmt.Println("notifyKafka Handler=>Help method registered")
	return "", nil
}

type notifyKafkaBrokersHandler struct{}

func (notifyKafkaBrokersHandler) Check(val string) error {
	// fmt.Println("notifyKafkaBrokers Handler=>Check method registered")
	return nil
}
func (notifyKafkaBrokersHandler) Help() (string, error) {
	// fmt.Println("notifyKafkaBrokers Handler=>Help method registered")
	return "", nil
}

type notifyKafkaTopicHandler struct{}

func (notifyKafkaTopicHandler) Check(val string) error {
	// fmt.Println("notifyKafkaTopic Handler=>Check method registered")
	return nil
}
func (notifyKafkaTopicHandler) Help() (string, error) {
	// fmt.Println("notifyKafkaTopic Handler=>Help method registered")
	return "", nil
}

type notifyKafkaTLSHandler struct{}

func (notifyKafkaTLSHandler) Check(val string) error {
	// fmt.Println("notifyKafkaTLS Handler=>Check method registered")
	return nil
}
func (notifyKafkaTLSHandler) Help() (string, error) {
	// fmt.Println("notifyKafkaTLS Handler=>Help method registered")
	return "", nil
}

type notifyKafkaTLSSkipVerifyHandler struct{}

func (notifyKafkaTLSSkipVerifyHandler) Check(val string) error {
	// fmt.Println("notifyKafkaTLSSkipVerify Handler=>Check method registered")
	return nil
}
func (notifyKafkaTLSSkipVerifyHandler) Help() (string, error) {
	// fmt.Println("notifyKafkaTLSSkipVerify Handler=>Help method registered")
	return "", nil
}

type notifyKafkaTLSClientAuthHandler struct{}

func (notifyKafkaTLSClientAuthHandler) Check(val string) error {
	// fmt.Println("notifyKafkaTLSClientAuth Handler=>Check method registered")
	return nil
}
func (notifyKafkaTLSClientAuthHandler) Help() (string, error) {
	// fmt.Println("notifyKafkaTLSClientAuth Handler=>Help method registered")
	return "", nil
}

type notifyKafkaSaslHandler struct{}

func (notifyKafkaSaslHandler) Check(val string) error {
	// fmt.Println("notifyKafkaSasl Handler=>Check method registered")
	return nil
}
func (notifyKafkaSaslHandler) Help() (string, error) {
	// fmt.Println("notifyKafkaSasl Handler=>Help method registered")
	return "", nil
}

type notifyKafkaSaslUsernameHandler struct{}

func (notifyKafkaSaslUsernameHandler) Check(val string) error {
	// fmt.Println("notifyKafkaSaslUsername Handler=>Check method registered")
	return nil
}
func (notifyKafkaSaslUsernameHandler) Help() (string, error) {
	// fmt.Println("notifyKafkaSaslUsername Handler=>Help method registered")
	return "", nil
}

type notifyKafkaSaslPasswordHandler struct{}

func (notifyKafkaSaslPasswordHandler) Check(val string) error {
	// fmt.Println("notifyKafkaSaslPassword Handler=>Check method registered")
	return nil
}
func (notifyKafkaSaslPasswordHandler) Help() (string, error) {
	// fmt.Println("notifyKafkaSaslPassword Handler=>Help method registered")
	return "", nil
}

type notifyMqttHandler struct{}

func (notifyMqttHandler) Check(val string) error {
	// fmt.Println("notifyMqtt Handler=>Check method registered")
	return nil
}
func (notifyMqttHandler) Help() (string, error) {
	// fmt.Println("notifyMqtt Handler=>Help method registered")
	return "", nil
}

type notifyMqttBrokerHandler struct{}

func (notifyMqttBrokerHandler) Check(val string) error {
	// fmt.Println("notifyMqttBroker Handler=>Check method registered")
	return nil
}
func (notifyMqttBrokerHandler) Help() (string, error) {
	// fmt.Println("notifyMqttBroker Handler=>Help method registered")
	return "", nil
}

type notifyMqttTopicHandler struct{}

func (notifyMqttTopicHandler) Check(val string) error {
	// fmt.Println("notifyMqttTopic Handler=>Check method registered")
	return nil
}
func (notifyMqttTopicHandler) Help() (string, error) {
	// fmt.Println("notifyMqttTopic Handler=>Help method registered")
	return "", nil
}

type notifyMqttQosHandler struct{}

func (notifyMqttQosHandler) Check(val string) error {
	// fmt.Println("notifyMqttQos Handler=>Check method registered")
	return nil
}
func (notifyMqttQosHandler) Help() (string, error) {
	// fmt.Println("notifyMqttQos Handler=>Help method registered")
	return "", nil
}

type notifyMqttClientIDHandler struct{}

func (notifyMqttClientIDHandler) Check(val string) error {
	// fmt.Println("notifyMqttClientID Handler=>Check method registered")
	return nil
}
func (notifyMqttClientIDHandler) Help() (string, error) {
	// fmt.Println("notifyMqttClientID Handler=>Help method registered")
	return "", nil
}

type notifyMqttUsernameHandler struct{}

func (notifyMqttUsernameHandler) Check(val string) error {
	// fmt.Println("notifyMqttUsername Handler=>Check method registered")
	return nil
}
func (notifyMqttUsernameHandler) Help() (string, error) {
	// fmt.Println("notifyMqttUsername Handler=>Help method registered")
	return "", nil
}

type notifyMqttPasswordHandler struct{}

func (notifyMqttPasswordHandler) Check(val string) error {
	// fmt.Println("notifyMqttPassword Handler=>Check method registered")
	return nil
}
func (notifyMqttPasswordHandler) Help() (string, error) {
	// fmt.Println("notifyMqttPassword Handler=>Help method registered")
	return "", nil
}

type notifyMqttReconnectIntervalHandler struct{}

func (notifyMqttReconnectIntervalHandler) Check(val string) error {
	// fmt.Println("notifyMqttReconnectInterval Handler=>Check method registered")
	return nil
}
func (notifyMqttReconnectIntervalHandler) Help() (string, error) {
	// fmt.Println("notifyMqttReconnectInterval Handler=>Help method registered")
	return "", nil
}

type notifyMqttKeepAliveIntervalHandler struct{}

func (notifyMqttKeepAliveIntervalHandler) Check(val string) error {
	// fmt.Println("notifyMqttKeepAliveInterval Handler=>Check method registered")
	return nil
}
func (notifyMqttKeepAliveIntervalHandler) Help() (string, error) {
	// fmt.Println("notifyMqttKeepAliveInterval Handler=>Help method registered")
	return "", nil
}

type notifyMysqlHandler struct{}

func (notifyMysqlHandler) Check(val string) error {
	// fmt.Println("notifyMysql Handler=>Check method registered")
	return nil
}
func (notifyMysqlHandler) Help() (string, error) {
	// fmt.Println("notifyMysql Handler=>Help method registered")
	return "", nil
}

type notifyMysqlFormatHandler struct{}

func (notifyMysqlFormatHandler) Check(val string) error {
	// fmt.Println("notifyMysqlFormat Handler=>Check method registered")
	return nil
}
func (notifyMysqlFormatHandler) Help() (string, error) {
	// fmt.Println("notifyMysqlFormat Handler=>Help method registered")
	return "", nil
}

type notifyMysqlDsnStringHandler struct{}

func (notifyMysqlDsnStringHandler) Check(val string) error {
	// fmt.Println("notifyMysqlDsnString Handler=>Check method registered")
	return nil
}
func (notifyMysqlDsnStringHandler) Help() (string, error) {
	// fmt.Println("notifyMysqlDsnString Handler=>Help method registered")
	return "", nil
}

type notifyMysqlTableHandler struct{}

func (notifyMysqlTableHandler) Check(val string) error {
	// fmt.Println("notifyMysqlTable Handler=>Check method registered")
	return nil
}
func (notifyMysqlTableHandler) Help() (string, error) {
	// fmt.Println("notifyMysqlTable Handler=>Help method registered")
	return "", nil
}

type notifyMysqlHostHandler struct{}

func (notifyMysqlHostHandler) Check(val string) error {
	// fmt.Println("notifyMysqlHost Handler=>Check method registered")
	return nil
}
func (notifyMysqlHostHandler) Help() (string, error) {
	// fmt.Println("notifyMysqlHost Handler=>Help method registered")
	return "", nil
}

type notifyMysqlPortHandler struct{}

func (notifyMysqlPortHandler) Check(val string) error {
	// fmt.Println("notifyMysqlPort Handler=>Check method registered")
	return nil
}
func (notifyMysqlPortHandler) Help() (string, error) {
	// fmt.Println("notifyMysqlPort Handler=>Help method registered")
	return "", nil
}

type notifyMysqlUserHandler struct{}

func (notifyMysqlUserHandler) Check(val string) error {
	// fmt.Println("notifyMysqlUser Handler=>Check method registered")
	return nil
}
func (notifyMysqlUserHandler) Help() (string, error) {
	// fmt.Println("notifyMysqlUser Handler=>Help method registered")
	return "", nil
}

type notifyMysqlPasswordHandler struct{}

func (notifyMysqlPasswordHandler) Check(val string) error {
	// fmt.Println("notifyMysqlPassword Handler=>Check method registered")
	return nil
}
func (notifyMysqlPasswordHandler) Help() (string, error) {
	// fmt.Println("notifyMysqlPassword Handler=>Help method registered")
	return "", nil
}

type notifyMysqlDatabaseHandler struct{}

func (notifyMysqlDatabaseHandler) Check(val string) error {
	// fmt.Println("notifyMysqlDatabase Handler=>Check method registered")
	return nil
}
func (notifyMysqlDatabaseHandler) Help() (string, error) {
	// fmt.Println("notifyMysqlDatabase Handler=>Help method registered")
	return "", nil
}

type notifyNatsHandler struct{}

func (notifyNatsHandler) Check(val string) error {
	// fmt.Println("notifyNats Handler=>Check method registered")
	return nil
}
func (notifyNatsHandler) Help() (string, error) {
	// fmt.Println("notifyNats Handler=>Help method registered")
	return "", nil
}

type notifyNatsAddressHandler struct{}

func (notifyNatsAddressHandler) Check(val string) error {
	// fmt.Println("notifyNatsAddress Handler=>Check method registered")
	return nil
}
func (notifyNatsAddressHandler) Help() (string, error) {
	// fmt.Println("notifyNatsAddress Handler=>Help method registered")
	return "", nil
}

type notifyNatsSubjectHandler struct{}

func (notifyNatsSubjectHandler) Check(val string) error {
	// fmt.Println("notifyNatsSubject Handler=>Check method registered")
	return nil
}
func (notifyNatsSubjectHandler) Help() (string, error) {
	// fmt.Println("notifyNatsSubject Handler=>Help method registered")
	return "", nil
}

type notifyNatsUsernameHandler struct{}

func (notifyNatsUsernameHandler) Check(val string) error {
	// fmt.Println("notifyNatsUsername Handler=>Check method registered")
	return nil
}
func (notifyNatsUsernameHandler) Help() (string, error) {
	// fmt.Println("notifyNatsUsername Handler=>Help method registered")
	return "", nil
}

type notifyNatsPasswordHandler struct{}

func (notifyNatsPasswordHandler) Check(val string) error {
	// fmt.Println("notifyNatsPassword Handler=>Check method registered")
	return nil
}
func (notifyNatsPasswordHandler) Help() (string, error) {
	// fmt.Println("notifyNatsPassword Handler=>Help method registered")
	return "", nil
}

type notifyNatsTokenHandler struct{}

func (notifyNatsTokenHandler) Check(val string) error {
	// fmt.Println("notifyNatsToken Handler=>Check method registered")
	return nil
}
func (notifyNatsTokenHandler) Help() (string, error) {
	// fmt.Println("notifyNatsToken Handler=>Help method registered")
	return "", nil
}

type notifyNatsSecureHandler struct{}

func (notifyNatsSecureHandler) Check(val string) error {
	// fmt.Println("notifyNatsSecure Handler=>Check method registered")
	return nil
}
func (notifyNatsSecureHandler) Help() (string, error) {
	// fmt.Println("notifyNatsSecure Handler=>Help method registered")
	return "", nil
}

type notifyNatsPingIntervalHandler struct{}

func (notifyNatsPingIntervalHandler) Check(val string) error {
	// fmt.Println("notifyNatsPingInterval Handler=>Check method registered")
	return nil
}
func (notifyNatsPingIntervalHandler) Help() (string, error) {
	// fmt.Println("notifyNatsPingInterval Handler=>Help method registered")
	return "", nil
}

type notifyNatsStreamingHandler struct{}

func (notifyNatsStreamingHandler) Check(val string) error {
	// fmt.Println("notifyNatsStreaming Handler=>Check method registered")
	return nil
}
func (notifyNatsStreamingHandler) Help() (string, error) {
	// fmt.Println("notifyNatsStreaming Handler=>Help method registered")
	return "", nil
}

type notifyNatsStreamingClusterIDHandler struct{}

func (notifyNatsStreamingClusterIDHandler) Check(val string) error {
	// fmt.Println("notifyNatsStreamingClusterID Handler=>Check method registered")
	return nil
}
func (notifyNatsStreamingClusterIDHandler) Help() (string, error) {
	// fmt.Println("notifyNatsStreamingClusterID Handler=>Help method registered")
	return "", nil
}

type notifyNatsStreamingClientIDHandler struct{}

func (notifyNatsStreamingClientIDHandler) Check(val string) error {
	// fmt.Println("notifyNatsStreamingClientID Handler=>Check method registered")
	return nil
}
func (notifyNatsStreamingClientIDHandler) Help() (string, error) {
	// fmt.Println("notifyNatsStreamingClientID Handler=>Help method registered")
	return "", nil
}

type notifyNatsStreamingAsyncHandler struct{}

func (notifyNatsStreamingAsyncHandler) Check(val string) error {
	// fmt.Println("notifyNatsStreamingAsync Handler=>Check method registered")
	return nil
}
func (notifyNatsStreamingAsyncHandler) Help() (string, error) {
	// fmt.Println("notifyNatsStreamingAsync Handler=>Help method registered")
	return "", nil
}

type notifyNatsStreamingMaxPubAcksInflightHandler struct{}

func (notifyNatsStreamingMaxPubAcksInflightHandler) Check(val string) error {
	// fmt.Println("notifyNatsStreamingMaxPubAcksInflight Handler=>Check method registered")
	return nil
}
func (notifyNatsStreamingMaxPubAcksInflightHandler) Help() (string, error) {
	// fmt.Println("notifyNatsStreamingMaxPubAcksInflight Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlHandler struct{}

func (notifyPostgresqlHandler) Check(val string) error {
	// fmt.Println("notifyPostgresql Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresql Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlFormatHandler struct{}

func (notifyPostgresqlFormatHandler) Check(val string) error {
	// fmt.Println("notifyPostgresqlFormat Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlFormatHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresqlFormat Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlConnectionStringHandler struct{}

func (notifyPostgresqlConnectionStringHandler) Check(val string) error {
	// fmt.Println("notifyPostgresqlConnectionString Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlConnectionStringHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresqlConnectionString Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlTableHandler struct{}

func (notifyPostgresqlTableHandler) Check(val string) error {
	// fmt.Println("notifyPostgresqlTable Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlTableHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresqlTable Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlHostHandler struct{}

func (notifyPostgresqlHostHandler) Check(val string) error {
	// fmt.Println("notifyPostgresqlHost Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlHostHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresqlHost Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlPortHandler struct{}

func (notifyPostgresqlPortHandler) Check(val string) error {
	// fmt.Println("notifyPostgresqlPort Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlPortHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresqlPort Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlUserHandler struct{}

func (notifyPostgresqlUserHandler) Check(val string) error {
	// fmt.Println("notifyPostgresqlUser Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlUserHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresqlUser Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlPasswordHandler struct{}

func (notifyPostgresqlPasswordHandler) Check(val string) error {
	// fmt.Println("notifyPostgresqlPassword Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlPasswordHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresqlPassword Handler=>Help method registered")
	return "", nil
}

type notifyPostgresqlDatabaseHandler struct{}

func (notifyPostgresqlDatabaseHandler) Check(val string) error {
	// fmt.Println("notifyPostgresqlDatabase Handler=>Check method registered")
	return nil
}
func (notifyPostgresqlDatabaseHandler) Help() (string, error) {
	// fmt.Println("notifyPostgresqlDatabase Handler=>Help method registered")
	return "", nil
}

type notifyRedisHandler struct{}

func (notifyRedisHandler) Check(val string) error {
	// fmt.Println("notifyRedis Handler=>Check method registered")
	return nil
}
func (notifyRedisHandler) Help() (string, error) {
	// fmt.Println("notifyRedis Handler=>Help method registered")
	return "", nil
}

type notifyRedisFormatHandler struct{}

func (notifyRedisFormatHandler) Check(val string) error {
	// fmt.Println("notifyRedisFormat Handler=>Check method registered")
	return nil
}
func (notifyRedisFormatHandler) Help() (string, error) {
	// fmt.Println("notifyRedisFormat Handler=>Help method registered")
	return "", nil
}

type notifyRedisAddressHandler struct{}

func (notifyRedisAddressHandler) Check(val string) error {
	// fmt.Println("notifyRedisAddress Handler=>Check method registered")
	return nil
}
func (notifyRedisAddressHandler) Help() (string, error) {
	// fmt.Println("notifyRedisAddress Handler=>Help method registered")
	return "", nil
}

type notifyRedisPasswordHandler struct{}

func (notifyRedisPasswordHandler) Check(val string) error {
	// fmt.Println("notifyRedisPassword Handler=>Check method registered")
	return nil
}
func (notifyRedisPasswordHandler) Help() (string, error) {
	// fmt.Println("notifyRedisPassword Handler=>Help method registered")
	return "", nil
}

type notifyRedisKeyHandler struct{}

func (notifyRedisKeyHandler) Check(val string) error {
	// fmt.Println("notifyRedisKey Handler=>Check method registered")
	return nil
}
func (notifyRedisKeyHandler) Help() (string, error) {
	// fmt.Println("notifyRedisKey Handler=>Help method registered")
	return "", nil
}

type notifyWebhookHandler struct{}

func (notifyWebhookHandler) Check(val string) error {
	// fmt.Println("notifyWebhook Handler=>Check method registered")
	return nil
}
func (notifyWebhookHandler) Help() (string, error) {
	// fmt.Println("notifyWebhook Handler=>Help method registered")
	return "", nil
}

type notifyWebhookEndpointHandler struct{}

func (notifyWebhookEndpointHandler) Check(val string) error {
	// fmt.Println("notifyWebhookEndpoint Handler=>Check method registered")
	return nil
}
func (notifyWebhookEndpointHandler) Help() (string, error) {
	// fmt.Println("notifyWebhookEndpoint Handler=>Help method registered")
	return "", nil
}

type logHTTPHandler struct{}

func (logHTTPHandler) Check(val string) error {
	// fmt.Println("logHTTP Handler=>Check method registered")
	return nil
}
func (logHTTPHandler) Help() (string, error) {
	// fmt.Println("logHTTP Handler=>Help method registered")
	return "", nil
}

type logHTTPAnonymousHandler struct{}

func (logHTTPAnonymousHandler) Check(val string) error {
	// fmt.Println("logHTTPAnonymous Handler=>Check method registered")
	return nil
}
func (logHTTPAnonymousHandler) Help() (string, error) {
	// fmt.Println("logHTTPAnonymous Handler=>Help method registered")
	return "", nil
}

type logHTTPAuditHandler struct{}

func (logHTTPAuditHandler) Check(val string) error {
	// fmt.Println("logHTTPAudit Handler=>Check method registered")
	return nil
}
func (logHTTPAuditHandler) Help() (string, error) {
	// fmt.Println("logHTTPAudit Handler=>Help method registered")
	return "", nil
}

type logHTTPEndpointHandler struct{}

func (logHTTPEndpointHandler) Check(val string) error {
	// fmt.Println("logHTTPEndpoint Handler=>Check method registered")
	return nil
}
func (logHTTPEndpointHandler) Help() (string, error) {
	// fmt.Println("logHTTPEndpoint Handler=>Help method registered")
	return "", nil
}

type logSubnetHandler struct{}

func (logSubnetHandler) Check(val string) error {
	// fmt.Println("logSubnet Handler=>Check method registered")
	return nil
}
func (logSubnetHandler) Help() (string, error) {
	// fmt.Println("logSubnet Handler=>Help method registered")
	return "", nil
}

type logSubnetTokenHandler struct{}

func (logSubnetTokenHandler) Check(val string) error {
	// fmt.Println("logSubnetToken Handler=>Check method registered")
	return nil
}
func (logSubnetTokenHandler) Help() (string, error) {
	// fmt.Println("logSubnetToken Handler=>Help method registered")
	return "", nil
}

type logConsoleHandler struct{}

func (logConsoleHandler) Check(val string) error {
	// fmt.Println("logConsole Handler=>Check method registered")
	return nil
}
func (logConsoleHandler) Help() (string, error) {
	// fmt.Println("logConsole Handler=>Help method registered")
	return "", nil
}

type logConsoleAnonymousHandler struct{}

func (logConsoleAnonymousHandler) Check(val string) error {
	// fmt.Println("logConsoleAnonymous Handler=>Check method registered")
	return nil
}
func (logConsoleAnonymousHandler) Help() (string, error) {
	// fmt.Println("logConsoleAnonymous Handler=>Help method registered")
	return "", nil
}

type logConsoleAuditHandler struct{}

func (logConsoleAuditHandler) Check(val string) error {
	// fmt.Println("logConsoleAudit Handler=>Check method registered")
	return nil
}
func (logConsoleAuditHandler) Help() (string, error) {
	// fmt.Println("logConsoleAudit Handler=>Help method registered")
	return "", nil
}

func registerAllKeys(s *configuration.ServerConfig) error {
	s.RegisterKey("version", versionHandler{})
	s.RegisterKey("credential.accessKey", credentialAccessKeyHandler{})
	s.RegisterKey("credential.secretKey", credentialSecretKeyHandler{})
	s.RegisterKey("region", regionHandler{})
	s.RegisterKey("browser", browserHandler{})
	s.RegisterKey("worm", wormHandler{})
	s.RegisterKey("domain", domainHandler{})
	s.RegisterKey("storage.class.standard", storageClassStandardHandler{})
	s.RegisterKey("storage.class.rrs", storageClassRrsHandler{})
	s.RegisterKey("cache.drives", cacheDrivesHandler{})
	s.RegisterKey("cache.expiry", cacheExpiryHandler{})
	s.RegisterKey("cache.maxuse", cacheMaxuseHandler{})
	s.RegisterKey("cache.exclude", cacheExcludeHandler{})
	s.RegisterKey("kms.vault.endpoint", kmsVaultEndpointHandler{})
	s.RegisterKey("kms.vault.auth.type", kmsVaultAuthTypeHandler{})
	s.RegisterKey("kms.vault.auth.approle.id", kmsVaultAuthApproleIDHandler{})
	s.RegisterKey("kms.vault.auth.approle.secret", kmsVaultAuthApproleSecretHandler{})
	s.RegisterKey("kms.vault.key-id.name", kmsVaultKeyIDNameHandler{})
	s.RegisterKey("kms.vault.key-id.version", kmsVaultKeyIDVersionHandler{})
	s.RegisterKey("notify.amqp", notifyAmqpHandler{})
	s.RegisterKey("notify.amqp.url", notifyAmqpURLHandler{})
	s.RegisterKey("notify.amqp.exchange", notifyAmqpExchangeHandler{})
	s.RegisterKey("notify.amqp.routingKey", notifyAmqpRoutingKeyHandler{})
	s.RegisterKey("notify.amqp.exchangeType", notifyAmqpExchangeTypeHandler{})
	s.RegisterKey("notify.amqp.deliveryMode", notifyAmqpDeliveryModeHandler{})
	s.RegisterKey("notify.amqp.mandatory", notifyAmqpMandatoryHandler{})
	s.RegisterKey("notify.amqp.immediate", notifyAmqpImmediateHandler{})
	s.RegisterKey("notify.amqp.durable", notifyAmqpDurableHandler{})
	s.RegisterKey("notify.amqp.internal", notifyAmqpInternalHandler{})
	s.RegisterKey("notify.amqp.noWait", notifyAmqpNoWaitHandler{})
	s.RegisterKey("notify.amqp.autoDeleted", notifyAmqpAutoDeletedHandler{})
	s.RegisterKey("notify.elasticsearch", notifyElasticsearchHandler{})
	s.RegisterKey("notify.elasticsearch.format", notifyElasticsearchFormatHandler{})
	s.RegisterKey("notify.elasticsearch.url", notifyElasticsearchURLHandler{})
	s.RegisterKey("notify.elasticsearch.index", notifyElasticsearchIndexHandler{})
	s.RegisterKey("notify.kafka", notifyKafkaHandler{})
	s.RegisterKey("notify.kafka.brokers", notifyKafkaBrokersHandler{})
	s.RegisterKey("notify.kafka.topic", notifyKafkaTopicHandler{})
	s.RegisterKey("notify.kafka.tls", notifyKafkaTLSHandler{})
	s.RegisterKey("notify.kafka.tls.skipVerify", notifyKafkaTLSSkipVerifyHandler{})
	s.RegisterKey("notify.kafka.tls.clientAuth", notifyKafkaTLSClientAuthHandler{})
	s.RegisterKey("notify.kafka.sasl", notifyKafkaSaslHandler{})
	s.RegisterKey("notify.kafka.sasl.username", notifyKafkaSaslUsernameHandler{})
	s.RegisterKey("notify.kafka.sasl.password", notifyKafkaSaslPasswordHandler{})
	s.RegisterKey("notify.mqtt", notifyMqttHandler{})
	s.RegisterKey("notify.mqtt.broker", notifyMqttBrokerHandler{})
	s.RegisterKey("notify.mqtt.topic", notifyMqttTopicHandler{})
	s.RegisterKey("notify.mqtt.qos", notifyMqttQosHandler{})
	s.RegisterKey("notify.mqtt.clientId", notifyMqttClientIDHandler{})
	s.RegisterKey("notify.mqtt.username", notifyMqttUsernameHandler{})
	s.RegisterKey("notify.mqtt.password", notifyMqttPasswordHandler{})
	s.RegisterKey("notify.mqtt.reconnectInterval", notifyMqttReconnectIntervalHandler{})
	s.RegisterKey("notify.mqtt.keepAliveInterval", notifyMqttKeepAliveIntervalHandler{})
	s.RegisterKey("notify.mysql", notifyMysqlHandler{})
	s.RegisterKey("notify.mysql.format", notifyMysqlFormatHandler{})
	s.RegisterKey("notify.mysql.dsnString", notifyMysqlDsnStringHandler{})
	s.RegisterKey("notify.mysql.table", notifyMysqlTableHandler{})
	s.RegisterKey("notify.mysql.host", notifyMysqlHostHandler{})
	s.RegisterKey("notify.mysql.port", notifyMysqlPortHandler{})
	s.RegisterKey("notify.mysql.user", notifyMysqlUserHandler{})
	s.RegisterKey("notify.mysql.password", notifyMysqlPasswordHandler{})
	s.RegisterKey("notify.mysql.database", notifyMysqlDatabaseHandler{})
	s.RegisterKey("notify.nats", notifyNatsHandler{})
	s.RegisterKey("notify.nats.address", notifyNatsAddressHandler{})
	s.RegisterKey("notify.nats.subject", notifyNatsSubjectHandler{})
	s.RegisterKey("notify.nats.username", notifyNatsUsernameHandler{})
	s.RegisterKey("notify.nats.password", notifyNatsPasswordHandler{})
	s.RegisterKey("notify.nats.token", notifyNatsTokenHandler{})
	s.RegisterKey("notify.nats.secure", notifyNatsSecureHandler{})
	s.RegisterKey("notify.nats.pingInterval", notifyNatsPingIntervalHandler{})
	s.RegisterKey("notify.nats.streaming", notifyNatsStreamingHandler{})
	s.RegisterKey("notify.nats.streaming.clusterID", notifyNatsStreamingClusterIDHandler{})
	s.RegisterKey("notify.nats.streaming.clientID", notifyNatsStreamingClientIDHandler{})
	s.RegisterKey("notify.nats.streaming.async", notifyNatsStreamingAsyncHandler{})
	s.RegisterKey("notify.nats.streaming.maxPubAcksInflight", notifyNatsStreamingMaxPubAcksInflightHandler{})
	s.RegisterKey("notify.postgresql", notifyPostgresqlHandler{})
	s.RegisterKey("notify.postgresql.format", notifyPostgresqlFormatHandler{})
	s.RegisterKey("notify.postgresql.connectionString", notifyPostgresqlConnectionStringHandler{})
	s.RegisterKey("notify.postgresql.table", notifyPostgresqlTableHandler{})
	s.RegisterKey("notify.postgresql.host", notifyPostgresqlHostHandler{})
	s.RegisterKey("notify.postgresql.port", notifyPostgresqlPortHandler{})
	s.RegisterKey("notify.postgresql.user", notifyPostgresqlUserHandler{})
	s.RegisterKey("notify.postgresql.password", notifyPostgresqlPasswordHandler{})
	s.RegisterKey("notify.postgresql.database", notifyPostgresqlDatabaseHandler{})
	s.RegisterKey("notify.redis", notifyRedisHandler{})
	s.RegisterKey("notify.redis.format", notifyRedisFormatHandler{})
	s.RegisterKey("notify.redis.address", notifyRedisAddressHandler{})
	s.RegisterKey("notify.redis.password", notifyRedisPasswordHandler{})
	s.RegisterKey("notify.redis.key", notifyRedisKeyHandler{})
	s.RegisterKey("notify.webhook", notifyWebhookHandler{})
	s.RegisterKey("notify.webhook.endpoint", notifyWebhookEndpointHandler{})
	s.RegisterKey("log.http", logHTTPHandler{})
	s.RegisterKey("log.http.anonymous", logHTTPAnonymousHandler{})
	s.RegisterKey("log.http.audit", logHTTPAuditHandler{})
	s.RegisterKey("log.http.endpoint", logHTTPEndpointHandler{})
	s.RegisterKey("log.subnet", logSubnetHandler{})
	s.RegisterKey("log.subnet.token", logSubnetTokenHandler{})
	s.RegisterKey("log.console", logConsoleHandler{})
	s.RegisterKey("log.console.anonymous", logConsoleAnonymousHandler{})
	s.RegisterKey("log.console.audit", logConsoleAuditHandler{})
	return nil
}

func main() {
	var k, val, comment string
	var err error
	serverConfig := new(configuration.ServerConfig)
	serverConfig.Init()

	// Register all server configruation keys
	fmt.Printf("\n*******************************************\n")
	fmt.Printf("Register configuration keys/parameters\n")
	registerAllKeys(serverConfig)

	// Load configuration parameters from configuration file
	fmt.Printf("\n*******************************************\n")
	fmt.Printf("Load configuration parameters from configuration file\n")
	load(confFile, serverConfig)

	fmt.Printf("\n*******************************************\n")
	fmt.Printf("Get value of 'worm'\n")
	k = "worm"
	if val, comment, err = serverConfig.Get(k); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		np(k, val, comment)
	}

	fmt.Printf("\n*******************************************\n")
	fmt.Printf("SET wor = 12\n")
	k, val, comment = "wor", "12", ""
	if err := serverConfig.Set(k, val, comment); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		fmt.Println("Success! Yeay!")
		//save memory to disk by writing the whole
	}

	fmt.Printf("\n*******************************************\n")
	fmt.Printf("Get value of 'domain'\n")
	k = "domain"
	if val, comment, err = serverConfig.Get(k); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		np(k, val, comment)
	}

	fmt.Printf("\n*******************************************\n")
	fmt.Printf("Get value of 'version'\n")
	k = "version"
	if val, comment, err = serverConfig.Get(k); err != nil {
		fmt.Println("ERROR:", err)
	} else {
		np(k, val, comment)
	}

	fmt.Printf("\n*******************************************\n")
	fmt.Printf("List all values\n\n")
	listConf, err := serverConfig.List()
	if err != nil {
		fmt.Println("ERROR:", err)
	}
	for _, v := range listConf {
		fmt.Println(v)
	}
}
