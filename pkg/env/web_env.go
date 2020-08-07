/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package env

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"time"
)

const (
	webEnvScheme       = "env"
	webEnvSchemeSecure = "env+tls"
)

var (
	globalRootCAs *x509.CertPool
)

// RegisterGlobalCAs register the global root CAs
func RegisterGlobalCAs(CAs *x509.CertPool) {
	globalRootCAs = CAs
}

func isValidEnvScheme(scheme string) bool {
	switch scheme {
	case webEnvScheme:
		fallthrough
	case webEnvSchemeSecure:
		return true
	}
	return false
}

var (
	hostKeys = regexp.MustCompile("^(https?://)(.*?):(.*?)@(.*?)$")
)

func fetchEnvHTTP(envKey string, u *url.URL) (string, error) {
	switch u.Scheme {
	case webEnvScheme:
		u.Scheme = "http"
	case webEnvSchemeSecure:
		u.Scheme = "https"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var (
		username, password string
	)

	envURL := u.String()
	if hostKeys.MatchString(envURL) {
		parts := hostKeys.FindStringSubmatch(envURL)
		if len(parts) != 5 {
			return "", errors.New("invalid arguments")
		}
		username = parts[2]
		password = parts[3]
		envURL = fmt.Sprintf("%s%s", parts[1], parts[4])
	}

	if username == "" && password == "" && u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, envURL+"?key="+envKey, nil)
	if err != nil {
		return "", err
	}

	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}

	clnt := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 5 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: 3 * time.Second,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 3 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: globalRootCAs,
			},
			// Go net/http automatically unzip if content-type is
			// gzip disable this feature, as we are always interested
			// in raw stream.
			DisableCompression: true,
		},
	}

	resp, err := clnt.Do(req)
	if err != nil {
		return "", err
	}

	envValueBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(envValueBytes), nil
}

// Environ returns a copy of strings representing the
// environment, in the form "key=value".
func Environ() []string {
	return os.Environ()
}

// LookupEnv retrieves the value of the environment variable
// named by the key. If the variable is present in the
// environment the value (which may be empty) is returned
// and the boolean is true. Otherwise the returned value
// will be empty and the boolean will be false.
//
// Additionally if the input is env://username:password@remote:port/
// to fetch ENV values for the env value from a remote server.
func LookupEnv(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	if ok {
		u, err := url.Parse(v)
		if err != nil {
			return v, true
		}
		if !isValidEnvScheme(u.Scheme) {
			return v, true
		}
		v, err = fetchEnvHTTP(key, u)
		if err != nil {
			return "", false
		}
		return v, true
	}
	return "", false
}
