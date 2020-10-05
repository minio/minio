/*
*
*  Mint, (C) 2019 Minio, Inc.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software

*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
 */

package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	log "github.com/sirupsen/logrus"
)

const (
	pass           = "PASS" // Indicate that a test passed
	fail           = "FAIL" // Indicate that a test failed
	livenessPath   = "/minio/health/live"
	readinessPath  = "/minio/health/ready"
	prometheusPath = "/minio/prometheus/metrics"
	timeout        = time.Duration(30 * time.Second)
)

type mintJSONFormatter struct {
}

func (f *mintJSONFormatter) Format(entry *log.Entry) ([]byte, error) {
	data := make(log.Fields, len(entry.Data))
	for k, v := range entry.Data {
		switch v := v.(type) {
		case error:
			// Otherwise errors are ignored by `encoding/json`
			// https://github.com/sirupsen/logrus/issues/137
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}

	serialized, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal fields to JSON, %w", err)
	}
	return append(serialized, '\n'), nil
}

// log successful test runs
func successLogger(function string, args map[string]interface{}, startTime time.Time) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	// log with the fields as per mint
	fields := log.Fields{"name": "healthcheck", "function": function, "args": args, "duration": duration.Nanoseconds() / 1000000, "status": pass}
	return log.WithFields(fields)
}

// log failed test runs
func failureLog(function string, args map[string]interface{}, startTime time.Time, alert string, message string, err error) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	var fields log.Fields
	// log with the fields as per mint
	if err != nil {
		fields = log.Fields{"name": "healthcheck", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": fail, "alert": alert, "message": message, "error": err}
	} else {
		fields = log.Fields{"name": "healthcheck", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": fail, "alert": alert, "message": message}
	}
	return log.WithFields(fields)
}

func testLivenessEndpoint(endpoint string) {
	startTime := time.Now()
	function := "testLivenessEndpoint"

	u, err := url.Parse(fmt.Sprintf("%s%s", endpoint, livenessPath))
	if err != nil {
		// Could not parse URL successfully
		failureLog(function, nil, startTime, "", "URL Parsing for Healthcheck Liveness handler failed", err).Fatal()
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: u.Scheme == "https"},
	}
	client := &http.Client{Transport: tr, Timeout: timeout}
	resp, err := client.Get(u.String())
	if err != nil {
		// GET request errored
		failureLog(function, nil, startTime, "", "GET request failed", err).Fatal()
	}
	if resp.StatusCode != http.StatusOK {
		// Status not 200 OK
		failureLog(function, nil, startTime, "", fmt.Sprintf("GET /minio/health/live returned %s", resp.Status), err).Fatal()
	}

	defer resp.Body.Close()
	defer successLogger(function, nil, startTime).Info()
}

func testReadinessEndpoint(endpoint string) {
	startTime := time.Now()
	function := "testReadinessEndpoint"

	u, err := url.Parse(fmt.Sprintf("%s%s", endpoint, readinessPath))
	if err != nil {
		// Could not parse URL successfully
		failureLog(function, nil, startTime, "", "URL Parsing for Healthcheck Readiness handler failed", err).Fatal()
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: u.Scheme == "https"},
	}
	client := &http.Client{Transport: tr, Timeout: timeout}
	resp, err := client.Get(u.String())
	if err != nil {
		// GET request errored
		failureLog(function, nil, startTime, "", "GET request to Readiness endpoint failed", err).Fatal()
	}
	if resp.StatusCode != http.StatusOK {
		// Status not 200 OK
		failureLog(function, nil, startTime, "", "GET /minio/health/ready returned non OK status", err).Fatal()
	}

	defer resp.Body.Close()
	defer successLogger(function, nil, startTime).Info()
}

const (
	defaultPrometheusJWTExpiry = 100 * 365 * 24 * time.Hour
)

func testPrometheusEndpoint(endpoint string) {
	startTime := time.Now()
	function := "testPrometheusEndpoint"

	u, err := url.Parse(fmt.Sprintf("%s%s", endpoint, prometheusPath))
	if err != nil {
		// Could not parse URL successfully
		failureLog(function, nil, startTime, "", "URL Parsing for Healthcheck Prometheus handler failed", err).Fatal()
	}

	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.StandardClaims{
		ExpiresAt: time.Now().UTC().Add(defaultPrometheusJWTExpiry).Unix(),
		Subject:   os.Getenv("ACCESS_KEY"),
		Issuer:    "prometheus",
	})

	token, err := jwt.SignedString([]byte(os.Getenv("SECRET_KEY")))
	if err != nil {
		failureLog(function, nil, startTime, "", "jwt generation failed", err).Fatal()
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: u.Scheme == "https"},
	}
	client := &http.Client{Transport: tr, Timeout: timeout}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		failureLog(function, nil, startTime, "", "Initializing GET request to Prometheus endpoint failed", err).Fatal()
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	resp, err := client.Do(req)
	if err != nil {
		// GET request errored
		failureLog(function, nil, startTime, "", "GET request to Prometheus endpoint failed", err).Fatal()
	}

	if resp.StatusCode != http.StatusOK {
		// Status not 200 OK
		failureLog(function, nil, startTime, "", "GET /minio/prometheus/metrics returned non OK status", err).Fatal()
	}

	defer resp.Body.Close()
	defer successLogger(function, nil, startTime).Info()
}

func main() {
	endpoint := os.Getenv("SERVER_ENDPOINT")
	secure := os.Getenv("ENABLE_HTTPS")
	if secure == "1" {
		endpoint = "https://" + endpoint
	} else {
		endpoint = "http://" + endpoint
	}

	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)
	// create custom formatter
	mintFormatter := mintJSONFormatter{}
	// set custom formatter
	log.SetFormatter(&mintFormatter)
	// log Info or above -- success cases are Info level, failures are Fatal level
	log.SetLevel(log.InfoLevel)
	// execute tests
	testLivenessEndpoint(endpoint)
	testReadinessEndpoint(endpoint)
	testPrometheusEndpoint(endpoint)
}
