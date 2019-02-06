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

package config_test

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/minio/minio/pkg/config"
)

const (
	confFile    = "./sample-config.txt"
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

func load(configFile string, s *config.Server) []error {
	// Check if configuration file exists
	if _, err := os.Stat(confFile); os.IsNotExist(err) {
		// Return right away if configuration file doesn't exist
		return []error{errors.New("Error locating config file: " + err.Error())}
	}

	// Configuration file exists.
	// Read configuration data from etcd or file
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	lines, err := readLines(confFile)
	if err != nil {
		// Return right away if something is wrong
		// with reading the configuration file
		return []error{errors.New("Error reading config file: " + err.Error())}
	}

	var errSlice []error
	var key, val string
	// Go through each line of config file and classify them
	// as comments, key/value pairs, or combination of both.
	// Only empty lines, comment lines (<commentChar> xxxx xx x),
	// key/value pairs (key = value) and combination of
	// key/value pairs and comments in the same line
	// (key = value <commentChar> xxxx  xxx) are allowed.
	// Any other syntax is ignored.
	for ind, line := range lines {
		element, isComment, isValidConfigFormat := verifyConfigKeyFormat(line)

		if isComment {
			// We've decided not to support full comment lines
			// and empty lines. Skip it and continue.
			continue
		}

		if !isValidConfigFormat {
			errSlice = append(errSlice, errors.New("Invalid config format, '"+
				line+"'. (Config file line#:"+strconv.Itoa(ind+1)+")"))
			continue
		}
		// Valid configuration key/value format
		// Set the value for the key.
		// Set also does validation for the given key value
		key = element[0]
		val = element[1]
		comment := ""
		if len(element) > 2 {
			comment = element[2]
		}
		if err := s.Set(key, val, comment); err != nil {
			errSlice = append(errSlice, errors.New(err.Error()+
				", key \""+key+"\", value \""+val+"\". (Config file line#:"+strconv.Itoa(ind+1)+")"))
			continue
		}
	}

	// Display error messages if errArr has errors collected
	// during loading process of configuration file into memory
	if len(errSlice) > 0 {
		return errSlice
	}

	return nil
}

func save(configLines []string, s *config.Server) error {
	// Lock for writing
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()

	if err := writeLines(configLines, confFile); err != nil {
		return err
	}
	return nil
}

// Define Handler methods: Check and Help
// for each configuration parameters/keys
type versionHandler struct{}

func (versionHandler) Check(val string) error {
	defaultValue := "31"
	// Input validation for value, 'val'
	// Value type, min/max limitations and other
	// requirements are going to be checked here.
	// If value validation checks pass, the key
	// and value will be set in the memory.
	if _, err := strconv.Atoi(val); err != nil {
		val = defaultValue
		return errors.New("Value is expected to be an integer")
	}
	return nil
}

func (versionHandler) Help() (string, error) {
	return "", nil
}

type credentialAccessKeyHandler struct{}

func (credentialAccessKeyHandler) Check(val string) error {
	return nil
}
func (credentialAccessKeyHandler) Help() (string, error) {
	return "", nil
}

type credentialSecretKeyHandler struct{}

func (credentialSecretKeyHandler) Check(val string) error {
	return nil
}
func (credentialSecretKeyHandler) Help() (string, error) {
	return "", nil
}

type regionHandler struct{}

func (regionHandler) Check(val string) error {
	return nil
}
func (regionHandler) Help() (string, error) {
	return "", nil
}

type wormHandler struct{}

func (wormHandler) Check(val string) error {
	return nil
}
func (wormHandler) Help() (string, error) {
	return "", nil
}

type domainHandler struct{}

func (domainHandler) Check(val string) error {
	return nil
}
func (domainHandler) Help() (string, error) {
	return "", nil
}

type storageClassStandardHandler struct{}

func (storageClassStandardHandler) Check(val string) error {
	return nil
}
func (storageClassStandardHandler) Help() (string, error) {
	return "", nil
}

type storageClassRrsHandler struct{}

func (storageClassRrsHandler) Check(val string) error {
	return nil
}
func (storageClassRrsHandler) Help() (string, error) {
	return "", nil
}

type cacheDrivesHandler struct{}

func (cacheDrivesHandler) Check(val string) error {
	return nil
}
func (cacheDrivesHandler) Help() (string, error) {
	return "", nil
}

type cacheExpiryHandler struct{}

func (cacheExpiryHandler) Check(val string) error {
	defaultValue := "90"
	// Value validation
	if _, err := strconv.Atoi(val); err != nil {
		val = defaultValue
		return errors.New("Value is expected to be an integer")
	}
	return nil
}
func (cacheExpiryHandler) Help() (string, error) {
	return "", nil
}

type cacheMaxuseHandler struct{}

func (cacheMaxuseHandler) Check(val string) error {
	defaultValue := "80"
	// Value validation
	if _, err := strconv.Atoi(val); err != nil {
		val = defaultValue
		return errors.New("Value is expected to be an integer")
	}
	return nil
}
func (cacheMaxuseHandler) Help() (string, error) {
	return "", nil
}

type cacheExcludeHandler struct{}

func (cacheExcludeHandler) Check(val string) error {
	return nil
}
func (cacheExcludeHandler) Help() (string, error) {
	return "", nil
}

type kmsVaultEndpointHandler struct{}

func (kmsVaultEndpointHandler) Check(val string) error {
	return nil
}
func (kmsVaultEndpointHandler) Help() (string, error) {
	return "", nil
}

type kmsVaultAuthTypeHandler struct{}

func (kmsVaultAuthTypeHandler) Check(val string) error {
	return nil
}
func (kmsVaultAuthTypeHandler) Help() (string, error) {
	return "", nil
}

type kmsVaultAuthApproleIDHandler struct{}

func (kmsVaultAuthApproleIDHandler) Check(val string) error {
	return nil
}
func (kmsVaultAuthApproleIDHandler) Help() (string, error) {
	return "", nil
}

type kmsVaultAuthApproleSecretHandler struct{}

func (kmsVaultAuthApproleSecretHandler) Check(val string) error {
	return nil
}
func (kmsVaultAuthApproleSecretHandler) Help() (string, error) {
	return "", nil
}

type kmsVaultKeyIDNameHandler struct{}

func (kmsVaultKeyIDNameHandler) Check(val string) error {
	return nil
}
func (kmsVaultKeyIDNameHandler) Help() (string, error) {
	return "", nil
}

type kmsVaultKeyIDVersionHandler struct{}

func (kmsVaultKeyIDVersionHandler) Check(val string) error {
	return nil
}
func (kmsVaultKeyIDVersionHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpHandler struct{}

func (notifyAmqpHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyAmqpHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpURLHandler struct{}

func (notifyAmqpURLHandler) Check(val string) error {
	return nil
}
func (notifyAmqpURLHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpExchangeHandler struct{}

func (notifyAmqpExchangeHandler) Check(val string) error {
	return nil
}
func (notifyAmqpExchangeHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpRoutingKeyHandler struct{}

func (notifyAmqpRoutingKeyHandler) Check(val string) error {
	return nil
}
func (notifyAmqpRoutingKeyHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpExchangeTypeHandler struct{}

func (notifyAmqpExchangeTypeHandler) Check(val string) error {
	return nil
}
func (notifyAmqpExchangeTypeHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpDeliveryModeHandler struct{}

func (notifyAmqpDeliveryModeHandler) Check(val string) error {
	return nil
}
func (notifyAmqpDeliveryModeHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpMandatoryHandler struct{}

func (notifyAmqpMandatoryHandler) Check(val string) error {
	return nil
}
func (notifyAmqpMandatoryHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpImmediateHandler struct{}

func (notifyAmqpImmediateHandler) Check(val string) error {
	return nil
}
func (notifyAmqpImmediateHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpDurableHandler struct{}

func (notifyAmqpDurableHandler) Check(val string) error {
	return nil
}
func (notifyAmqpDurableHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpInternalHandler struct{}

func (notifyAmqpInternalHandler) Check(val string) error {
	return nil
}
func (notifyAmqpInternalHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpNoWaitHandler struct{}

func (notifyAmqpNoWaitHandler) Check(val string) error {
	return nil
}
func (notifyAmqpNoWaitHandler) Help() (string, error) {
	return "", nil
}

type notifyAmqpAutoDeletedHandler struct{}

func (notifyAmqpAutoDeletedHandler) Check(val string) error {
	return nil
}
func (notifyAmqpAutoDeletedHandler) Help() (string, error) {
	return "", nil
}

type notifyElasticsearchHandler struct{}

func (notifyElasticsearchHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyElasticsearchHandler) Help() (string, error) {
	return "", nil
}

type notifyElasticsearchFormatHandler struct{}

func (notifyElasticsearchFormatHandler) Check(val string) error {
	return nil
}
func (notifyElasticsearchFormatHandler) Help() (string, error) {
	return "", nil
}

type notifyElasticsearchURLHandler struct{}

func (notifyElasticsearchURLHandler) Check(val string) error {
	return nil
}
func (notifyElasticsearchURLHandler) Help() (string, error) {
	return "", nil
}

type notifyElasticsearchIndexHandler struct{}

func (notifyElasticsearchIndexHandler) Check(val string) error {
	return nil
}
func (notifyElasticsearchIndexHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaHandler struct{}

func (notifyKafkaHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyKafkaHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaBrokersHandler struct{}

func (notifyKafkaBrokersHandler) Check(val string) error {
	return nil
}
func (notifyKafkaBrokersHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaTopicHandler struct{}

func (notifyKafkaTopicHandler) Check(val string) error {
	return nil
}
func (notifyKafkaTopicHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaTLSHandler struct{}

func (notifyKafkaTLSHandler) Check(val string) error {
	return nil
}
func (notifyKafkaTLSHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaTLSSkipVerifyHandler struct{}

func (notifyKafkaTLSSkipVerifyHandler) Check(val string) error {
	return nil
}
func (notifyKafkaTLSSkipVerifyHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaTLSClientAuthHandler struct{}

func (notifyKafkaTLSClientAuthHandler) Check(val string) error {
	return nil
}
func (notifyKafkaTLSClientAuthHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaSaslHandler struct{}

func (notifyKafkaSaslHandler) Check(val string) error {
	return nil
}
func (notifyKafkaSaslHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaSaslUsernameHandler struct{}

func (notifyKafkaSaslUsernameHandler) Check(val string) error {
	return nil
}
func (notifyKafkaSaslUsernameHandler) Help() (string, error) {
	return "", nil
}

type notifyKafkaSaslPasswordHandler struct{}

func (notifyKafkaSaslPasswordHandler) Check(val string) error {
	return nil
}
func (notifyKafkaSaslPasswordHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttHandler struct{}

func (notifyMqttHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyMqttHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttBrokerHandler struct{}

func (notifyMqttBrokerHandler) Check(val string) error {
	return nil
}
func (notifyMqttBrokerHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttTopicHandler struct{}

func (notifyMqttTopicHandler) Check(val string) error {
	return nil
}
func (notifyMqttTopicHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttQosHandler struct{}

func (notifyMqttQosHandler) Check(val string) error {
	return nil
}
func (notifyMqttQosHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttClientIDHandler struct{}

func (notifyMqttClientIDHandler) Check(val string) error {
	return nil
}
func (notifyMqttClientIDHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttUsernameHandler struct{}

func (notifyMqttUsernameHandler) Check(val string) error {
	return nil
}
func (notifyMqttUsernameHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttPasswordHandler struct{}

func (notifyMqttPasswordHandler) Check(val string) error {
	return nil
}
func (notifyMqttPasswordHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttReconnectIntervalHandler struct{}

func (notifyMqttReconnectIntervalHandler) Check(val string) error {
	return nil
}
func (notifyMqttReconnectIntervalHandler) Help() (string, error) {
	return "", nil
}

type notifyMqttKeepAliveIntervalHandler struct{}

func (notifyMqttKeepAliveIntervalHandler) Check(val string) error {
	return nil
}
func (notifyMqttKeepAliveIntervalHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlHandler struct{}

func (notifyMysqlHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyMysqlHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlFormatHandler struct{}

func (notifyMysqlFormatHandler) Check(val string) error {
	return nil
}
func (notifyMysqlFormatHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlDsnStringHandler struct{}

func (notifyMysqlDsnStringHandler) Check(val string) error {
	return nil
}
func (notifyMysqlDsnStringHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlTableHandler struct{}

func (notifyMysqlTableHandler) Check(val string) error {
	return nil
}
func (notifyMysqlTableHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlHostHandler struct{}

func (notifyMysqlHostHandler) Check(val string) error {
	return nil
}
func (notifyMysqlHostHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlPortHandler struct{}

func (notifyMysqlPortHandler) Check(val string) error {
	return nil
}
func (notifyMysqlPortHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlUserHandler struct{}

func (notifyMysqlUserHandler) Check(val string) error {
	return nil
}
func (notifyMysqlUserHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlPasswordHandler struct{}

func (notifyMysqlPasswordHandler) Check(val string) error {
	return nil
}
func (notifyMysqlPasswordHandler) Help() (string, error) {
	return "", nil
}

type notifyMysqlDatabaseHandler struct{}

func (notifyMysqlDatabaseHandler) Check(val string) error {
	return nil
}
func (notifyMysqlDatabaseHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsHandler struct{}

func (notifyNatsHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyNatsHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsAddressHandler struct{}

func (notifyNatsAddressHandler) Check(val string) error {
	return nil
}
func (notifyNatsAddressHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsSubjectHandler struct{}

func (notifyNatsSubjectHandler) Check(val string) error {
	return nil
}
func (notifyNatsSubjectHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsUsernameHandler struct{}

func (notifyNatsUsernameHandler) Check(val string) error {
	return nil
}
func (notifyNatsUsernameHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsPasswordHandler struct{}

func (notifyNatsPasswordHandler) Check(val string) error {
	return nil
}
func (notifyNatsPasswordHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsTokenHandler struct{}

func (notifyNatsTokenHandler) Check(val string) error {
	return nil
}
func (notifyNatsTokenHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsSecureHandler struct{}

func (notifyNatsSecureHandler) Check(val string) error {
	return nil
}
func (notifyNatsSecureHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsPingIntervalHandler struct{}

func (notifyNatsPingIntervalHandler) Check(val string) error {
	return nil
}
func (notifyNatsPingIntervalHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsStreamingHandler struct{}

func (notifyNatsStreamingHandler) Check(val string) error {
	return nil
}
func (notifyNatsStreamingHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsStreamingClusterIDHandler struct{}

func (notifyNatsStreamingClusterIDHandler) Check(val string) error {
	return nil
}
func (notifyNatsStreamingClusterIDHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsStreamingClientIDHandler struct{}

func (notifyNatsStreamingClientIDHandler) Check(val string) error {
	return nil
}
func (notifyNatsStreamingClientIDHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsStreamingAsyncHandler struct{}

func (notifyNatsStreamingAsyncHandler) Check(val string) error {
	return nil
}
func (notifyNatsStreamingAsyncHandler) Help() (string, error) {
	return "", nil
}

type notifyNatsStreamingMaxPubAcksInflightHandler struct{}

func (notifyNatsStreamingMaxPubAcksInflightHandler) Check(val string) error {
	return nil
}
func (notifyNatsStreamingMaxPubAcksInflightHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlHandler struct{}

func (notifyPostgresqlHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyPostgresqlHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlFormatHandler struct{}

func (notifyPostgresqlFormatHandler) Check(val string) error {
	return nil
}
func (notifyPostgresqlFormatHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlConnectionStringHandler struct{}

func (notifyPostgresqlConnectionStringHandler) Check(val string) error {
	return nil
}
func (notifyPostgresqlConnectionStringHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlTableHandler struct{}

func (notifyPostgresqlTableHandler) Check(val string) error {
	return nil
}
func (notifyPostgresqlTableHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlHostHandler struct{}

func (notifyPostgresqlHostHandler) Check(val string) error {
	return nil
}
func (notifyPostgresqlHostHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlPortHandler struct{}

func (notifyPostgresqlPortHandler) Check(val string) error {
	return nil
}
func (notifyPostgresqlPortHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlUserHandler struct{}

func (notifyPostgresqlUserHandler) Check(val string) error {
	return nil
}
func (notifyPostgresqlUserHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlPasswordHandler struct{}

func (notifyPostgresqlPasswordHandler) Check(val string) error {
	return nil
}
func (notifyPostgresqlPasswordHandler) Help() (string, error) {
	return "", nil
}

type notifyPostgresqlDatabaseHandler struct{}

func (notifyPostgresqlDatabaseHandler) Check(val string) error {
	return nil
}
func (notifyPostgresqlDatabaseHandler) Help() (string, error) {
	return "", nil
}

type notifyRedisHandler struct{}

func (notifyRedisHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyRedisHandler) Help() (string, error) {
	return "", nil
}

type notifyRedisFormatHandler struct{}

func (notifyRedisFormatHandler) Check(val string) error {
	return nil
}
func (notifyRedisFormatHandler) Help() (string, error) {
	return "", nil
}

type notifyRedisAddressHandler struct{}

func (notifyRedisAddressHandler) Check(val string) error {
	return nil
}
func (notifyRedisAddressHandler) Help() (string, error) {
	return "", nil
}

type notifyRedisPasswordHandler struct{}

func (notifyRedisPasswordHandler) Check(val string) error {
	return nil
}
func (notifyRedisPasswordHandler) Help() (string, error) {
	return "", nil
}

type notifyRedisKeyHandler struct{}

func (notifyRedisKeyHandler) Check(val string) error {
	return nil
}
func (notifyRedisKeyHandler) Help() (string, error) {
	return "", nil
}

type notifyWebhookHandler struct{}

func (notifyWebhookHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (notifyWebhookHandler) Help() (string, error) {
	return "", nil
}

type notifyWebhookEndpointHandler struct{}

func (notifyWebhookEndpointHandler) Check(val string) error {
	return nil
}
func (notifyWebhookEndpointHandler) Help() (string, error) {
	return "", nil
}

type logHTTPHandler struct{}

func (logHTTPHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (logHTTPHandler) Help() (string, error) {
	return "", nil
}

type logHTTPAnonymousHandler struct{}

func (logHTTPAnonymousHandler) Check(val string) error {
	return nil
}
func (logHTTPAnonymousHandler) Help() (string, error) {
	return "", nil
}

type logHTTPAuditHandler struct{}

func (logHTTPAuditHandler) Check(val string) error {
	return nil
}
func (logHTTPAuditHandler) Help() (string, error) {
	return "", nil
}

type logHTTPEndpointHandler struct{}

func (logHTTPEndpointHandler) Check(val string) error {
	return nil
}
func (logHTTPEndpointHandler) Help() (string, error) {
	return "", nil
}

type logSubnetHandler struct{}

func (logSubnetHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (logSubnetHandler) Help() (string, error) {
	return "", nil
}

type logSubnetTokenHandler struct{}

func (logSubnetTokenHandler) Check(val string) error {
	return nil
}
func (logSubnetTokenHandler) Help() (string, error) {
	return "", nil
}

type logConsoleHandler struct{}

func (logConsoleHandler) Check(val string) error {
	// default := "off"
	return nil
}
func (logConsoleHandler) Help() (string, error) {
	return "", nil
}

type logConsoleAnonymousHandler struct{}

func (logConsoleAnonymousHandler) Check(val string) error {
	return nil
}
func (logConsoleAnonymousHandler) Help() (string, error) {
	return "", nil
}

type logConsoleAuditHandler struct{}

func (logConsoleAuditHandler) Check(val string) error {
	return nil
}
func (logConsoleAuditHandler) Help() (string, error) {
	return "", nil
}

func registerAllKeys(s *config.Server) error {
	// fmt.Printf("%+v\n", configMap)
	type regStruct struct {
		key     string
		handler config.KeyHandler
	}

	var configSlice = []regStruct{}

	configSlice = append(configSlice, regStruct{key: "version", handler: versionHandler{}})
	configSlice = append(configSlice, regStruct{key: "credential.accessKey", handler: credentialAccessKeyHandler{}})
	configSlice = append(configSlice, regStruct{key: "credential.secretKey", handler: credentialSecretKeyHandler{}})
	configSlice = append(configSlice, regStruct{key: "region", handler: regionHandler{}})
	configSlice = append(configSlice, regStruct{key: "worm", handler: wormHandler{}})
	configSlice = append(configSlice, regStruct{key: "domain", handler: domainHandler{}})
	configSlice = append(configSlice, regStruct{key: "storage.class.standard", handler: storageClassStandardHandler{}})
	configSlice = append(configSlice, regStruct{key: "storage.class.rrs", handler: storageClassRrsHandler{}})
	configSlice = append(configSlice, regStruct{key: "cache.drives", handler: cacheDrivesHandler{}})
	configSlice = append(configSlice, regStruct{key: "cache.expiry", handler: cacheExpiryHandler{}})
	configSlice = append(configSlice, regStruct{key: "cache.maxuse", handler: cacheMaxuseHandler{}})
	configSlice = append(configSlice, regStruct{key: "cache.exclude", handler: cacheExcludeHandler{}})
	configSlice = append(configSlice, regStruct{key: "kms.vault.endpoint", handler: kmsVaultEndpointHandler{}})
	configSlice = append(configSlice, regStruct{key: "kms.vault.auth.type", handler: kmsVaultAuthTypeHandler{}})
	configSlice = append(configSlice, regStruct{key: "kms.vault.auth.approle.id", handler: kmsVaultAuthApproleIDHandler{}})
	configSlice = append(configSlice, regStruct{key: "kms.vault.auth.approle.secret", handler: kmsVaultAuthApproleSecretHandler{}})
	configSlice = append(configSlice, regStruct{key: "kms.vault.key-id.name", handler: kmsVaultKeyIDNameHandler{}})
	configSlice = append(configSlice, regStruct{key: "kms.vault.key-id.version", handler: kmsVaultKeyIDVersionHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp", handler: notifyAmqpHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.url", handler: notifyAmqpURLHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.exchange", handler: notifyAmqpExchangeHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.routingKey", handler: notifyAmqpRoutingKeyHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.exchangeType", handler: notifyAmqpExchangeTypeHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.deliveryMode", handler: notifyAmqpDeliveryModeHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.mandatory", handler: notifyAmqpMandatoryHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.immediate", handler: notifyAmqpImmediateHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.durable", handler: notifyAmqpDurableHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.internal", handler: notifyAmqpInternalHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.noWait", handler: notifyAmqpNoWaitHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.amqp.autoDeleted", handler: notifyAmqpAutoDeletedHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.elasticsearch", handler: notifyElasticsearchHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.elasticsearch.format", handler: notifyElasticsearchFormatHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.elasticsearch.url", handler: notifyElasticsearchURLHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.elasticsearch.index", handler: notifyElasticsearchIndexHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka", handler: notifyKafkaHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka.brokers", handler: notifyKafkaBrokersHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka.topic", handler: notifyKafkaTopicHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka.tls", handler: notifyKafkaTLSHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka.tls.skipVerify", handler: notifyKafkaTLSSkipVerifyHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka.tls.clientAuth", handler: notifyKafkaTLSClientAuthHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka.sasl", handler: notifyKafkaSaslHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka.sasl.username", handler: notifyKafkaSaslUsernameHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.kafka.sasl.password", handler: notifyKafkaSaslPasswordHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt", handler: notifyMqttHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt.broker", handler: notifyMqttBrokerHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt.topic", handler: notifyMqttTopicHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt.qos", handler: notifyMqttQosHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt.clientId", handler: notifyMqttClientIDHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt.username", handler: notifyMqttUsernameHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt.password", handler: notifyMqttPasswordHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt.reconnectInterval", handler: notifyMqttReconnectIntervalHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mqtt.keepAliveInterval", handler: notifyMqttKeepAliveIntervalHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql", handler: notifyMysqlHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql.format", handler: notifyMysqlFormatHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql.dsnString", handler: notifyMysqlDsnStringHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql.table", handler: notifyMysqlTableHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql.host", handler: notifyMysqlHostHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql.port", handler: notifyMysqlPortHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql.user", handler: notifyMysqlUserHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql.password", handler: notifyMysqlPasswordHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.mysql.database", handler: notifyMysqlDatabaseHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats", handler: notifyNatsHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.address", handler: notifyNatsAddressHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.subject", handler: notifyNatsSubjectHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.username", handler: notifyNatsUsernameHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.password", handler: notifyNatsPasswordHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.token", handler: notifyNatsTokenHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.secure", handler: notifyNatsSecureHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.pingInterval", handler: notifyNatsPingIntervalHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.streaming", handler: notifyNatsStreamingHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.streaming.clusterID", handler: notifyNatsStreamingClusterIDHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.streaming.clientID", handler: notifyNatsStreamingClientIDHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.streaming.async", handler: notifyNatsStreamingAsyncHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.nats.streaming.maxPubAcksInflight", handler: notifyNatsStreamingMaxPubAcksInflightHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql", handler: notifyPostgresqlHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql.format", handler: notifyPostgresqlFormatHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql.connectionString", handler: notifyPostgresqlConnectionStringHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql.table", handler: notifyPostgresqlTableHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql.host", handler: notifyPostgresqlHostHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql.port", handler: notifyPostgresqlPortHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql.user", handler: notifyPostgresqlUserHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql.password", handler: notifyPostgresqlPasswordHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.postgresql.database", handler: notifyPostgresqlDatabaseHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.redis", handler: notifyRedisHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.redis.format", handler: notifyRedisFormatHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.redis.address", handler: notifyRedisAddressHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.redis.password", handler: notifyRedisPasswordHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.redis.key", handler: notifyRedisKeyHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.webhook", handler: notifyWebhookHandler{}})
	configSlice = append(configSlice, regStruct{key: "notify.webhook.endpoint", handler: notifyWebhookEndpointHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.http", handler: logHTTPHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.http.anonymous", handler: logHTTPAnonymousHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.http.audit", handler: logHTTPAuditHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.http.endpoint", handler: logHTTPEndpointHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.subnet", handler: logSubnetHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.subnet.token", handler: logSubnetTokenHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.console", handler: logConsoleHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.console.anonymous", handler: logConsoleAnonymousHandler{}})
	configSlice = append(configSlice, regStruct{key: "log.console.audit", handler: logConsoleAuditHandler{}})

	for _, v := range configSlice {
		if err := s.RegisterKey(v.key, v.handler); err != nil {
			return err
		}
	}
	return nil
}

func prepForTest() (*config.Server, error) {
	serverConfig := new(config.Server)
	serverConfig.Init()
	// Register all server configuration keys
	if err := registerAllKeys(serverConfig); err != nil {
		return nil, err
	}

	// Load configuration parameters from configuration file
	if errSlice := load(confFile, serverConfig); len(errSlice) > 0 {
		for _, err := range errSlice {
			return nil, err
		}
	}
	return serverConfig, nil
}

// Test to get the value of a valid key
func TestGetValueForValidKey(t *testing.T) {
	fmt.Println("*******************************************")
	fmt.Println("Test:  TestGetValueForValidKey")
	fmt.Println("Command: 'get worm'")
	fmt.Println("*******************************************")
	s, err := prepForTest()
	if err != nil {
		t.Error(err)
		return
	}
	key := "worm"
	if val, comment, err := s.Get(key); err != nil {
		t.Error(err)
	} else {
		np(key, val, comment)
	}
}

// Test to set the value for an invalid key (negative test)
func TestSetValueForInvalidKey(t *testing.T) {
	fmt.Println("*******************************************")
	fmt.Println("Test:  TestSetValueForInvalidKey")
	fmt.Println("Command: 'set wor 12'")
	fmt.Println("Negative Test, expects \"Invalid configuration parameter key\" error")
	fmt.Println("*******************************************")
	s, err := prepForTest()
	if err != nil {
		t.Error(err)
		return
	}
	key, val, comment := "wor", "12", ""
	if err := s.Set(key, val, comment); err != nil {
		// Check if err is the exact error message
		fmt.Println("Error:", err)
		if err.Error() == "Invalid configuration parameter key: \""+key+"\"" {
			return
		}
	}
	t.Error("TestSetValueForInvalidKey test failed to detect invalid key: \"" + key + "\"")
}

// Test to set an invalid value for a valid key
func TestSetInvalidValue(t *testing.T) {
	fmt.Println("*******************************************")
	fmt.Println("Test:  TestSetInvalidValue")
	fmt.Println("Command: set version abc")
	fmt.Println("*******************************************")
	s, err := prepForTest()
	if err != nil {
		t.Error(err)
		return
	}
	key, val, comment := "version", "abc", ""
	if err := s.Set(key, val, comment); err != nil {
		// Check if the error message matches the expected message
		fmt.Println("Error:", err, "\""+val+"\"")
		if err.Error() == "Invalid value" {
			return
		}
	}
	t.Error("TestSetInvalidValue test failed to detect invalid value: \"" + val + "\"")
}

// Test to set a valid value for a valid key
// and SAVE it in the config file
func TestSetValidKeyValueAndSave(t *testing.T) {
	fmt.Println("*******************************************")
	fmt.Println("Test:  TestSetValidKeyValueAndSave")
	fmt.Println("Command: set version 31")
	fmt.Println("*******************************************")
	s, err := prepForTest()
	if err != nil {
		t.Error(err)
		return
	}
	key, val, comment := "version", "31", ""
	if err := s.Set(key, val, comment); err != nil {
		t.Error(err)
	} else {
		// Get (List) the whole configuration and save it on the disk
		lines, err := s.List()
		if err != nil {
			t.Error(err)
		}
		if err := save(lines, s); err != nil {
			t.Error(err)
		}
	}
}

// Test to get the value for a valid key not set yet
func TestGetValueValidKeyNotSetYet(t *testing.T) {
	fmt.Println("*******************************************")
	fmt.Println("Test:  TestGetValueValidKeyNotSetYet")
	fmt.Println("Command: get domain")
	fmt.Println("*******************************************")
	s, err := prepForTest()
	if err != nil {
		t.Error(err)
		return
	}
	key := "domain"
	if val, comment, err := s.Get(key); err == nil {
		t.Error(errors.New("Failed to raise 'not set yet' error message"))
	} else {
		fmt.Println(key, "=", val, comment)
		fmt.Println(err)
	}
}

// Test to set & then to get version information
// to make sure set and read values match
func TestSetAndGetValueMatch(t *testing.T) {
	fmt.Println("*******************************************")
	fmt.Println("Test: TestSetAndGetValueMatch")
	fmt.Println("Command: set version 32")
	s, err := prepForTest()
	if err != nil {
		t.Error(err)
		return
	}
	key, setVal, comment := "version", "32", ""
	if err := s.Set(key, setVal, comment); err != nil {
		t.Error(err)
		return
	} else {
		// List configuration and save it on the disk
		lines, err := s.List()
		if err != nil {
			t.Error(err)
			return
		}
		if err := save(lines, s); err != nil {
			t.Error(err)
			return
		}
	}
	fmt.Println("Command: get version")
	fmt.Println("*******************************************")
	if readVal, _, err := s.Get(key); err != nil {
		t.Error(err)
	} else {
		if readVal != setVal {
			t.Error(errors.New("Failed to match the set value:'" + setVal + "' with the read value: '" + readVal + "'"))
		}
		fmt.Println(key, " = ", readVal, comment)

	}
}

// TestListAllConfig tests the List command
func TestListAllConfig(t *testing.T) {
	fmt.Println("*******************************************")
	fmt.Println("Test:  TestListAllConfig")
	fmt.Println("Command: List")
	fmt.Println("*******************************************")
	s, err := prepForTest()
	if err != nil {
		t.Error(err)
		return
	}
	listConf, err := s.List()
	if err != nil {
		t.Error(err)
	}
	for _, v := range listConf {
		fmt.Println(v)
	}
}
