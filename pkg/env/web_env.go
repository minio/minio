// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
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

var (
	hostKeys = regexp.MustCompile("^(https?://)(.*?):(.*?)@(.*?)$")
)

func fetchHTTPConstituentParts(u *url.URL) (username string, password string, envURL string, err error) {
	envURL = u.String()
	if hostKeys.MatchString(envURL) {
		parts := hostKeys.FindStringSubmatch(envURL)
		if len(parts) != 5 {
			return "", "", "", errors.New("invalid arguments")
		}
		username = parts[2]
		password = parts[3]
		envURL = fmt.Sprintf("%s%s", parts[1], parts[4])
	}

	if username == "" && password == "" && u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}
	return username, password, envURL, nil
}

func getEnvValueFromHTTP(urlStr, envKey string) (string, string, string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", "", "", err
	}

	switch u.Scheme {
	case webEnvScheme:
		u.Scheme = "http"
	case webEnvSchemeSecure:
		u.Scheme = "https"
	default:
		return "", "", "", errors.New("invalid arguments")
	}

	username, password, envURL, err := fetchHTTPConstituentParts(u)
	if err != nil {
		return "", "", "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, envURL+"?key="+envKey, nil)
	if err != nil {
		return "", "", "", err
	}

	claims := &jwt.StandardClaims{
		ExpiresAt: int64(15 * time.Minute),
		Issuer:    username,
		Subject:   envKey,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	ss, err := token.SignedString([]byte(password))
	if err != nil {
		return "", "", "", err
	}

	req.Header.Set("Authorization", "Bearer "+ss)

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
		return "", "", "", err
	}

	envValueBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", "", "", err
	}

	return string(envValueBytes), username, password, nil
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
// In this case, it also returns the credentials username and password
func LookupEnv(key string) (string, string, string, bool) {
	v, ok := os.LookupEnv(key)
	if ok && strings.HasPrefix(v, webEnvScheme) {
		// If env value starts with `env*://`
		// continue to parse and fetch from remote
		var err error
		v, user, pwd, err := getEnvValueFromHTTP(strings.TrimSpace(v), key)
		if err != nil {
			env, eok := os.LookupEnv("_" + key)
			if eok {
				// fallback to cached value if-any.
				return env, user, pwd, eok
			}
		}
		// Set the ENV value to _env value,
		// this value is a fallback in-case of
		// server restarts when webhook server
		// is down.
		os.Setenv("_"+key, v)
		return v, user, pwd, true
	}
	return v, "", "", ok
}
