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
 */

package validator

import (
	"crypto"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	xnet "github.com/minio/minio/pkg/net"
)

// JWKSArgs - RSA authentication target arguments
type JWKSArgs struct {
	URL       *xnet.URL `json:"url"`
	publicKey crypto.PublicKey
}

// Validate JWT authentication target arguments
func (r *JWKSArgs) Validate() error {
	return nil
}

// PopulatePublicKey - populates a new publickey from the JWKS URL.
func (r *JWKSArgs) PopulatePublicKey() error {
	insecureClient := &http.Client{Transport: newCustomHTTPTransport(true)}
	client := &http.Client{Transport: newCustomHTTPTransport(false)}
	resp, err := client.Get(r.URL.String())
	if err != nil {
		resp, err = insecureClient.Get(r.URL.String())
		if err != nil {
			return err
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	var jwk JWKS
	if err = json.NewDecoder(resp.Body).Decode(&jwk); err != nil {
		return err
	}

	r.publicKey, err = jwk.Keys[0].DecodePublicKey()
	if err != nil {
		return err
	}

	return nil
}

// UnmarshalJSON - decodes JSON data.
func (r *JWKSArgs) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subJWKSArgs JWKSArgs
	var sr subJWKSArgs

	// IAM related envs.
	if jwksURL, ok := os.LookupEnv("MINIO_IAM_JWKS_URL"); ok {
		u, err := xnet.ParseURL(jwksURL)
		if err != nil {
			return err
		}
		sr.URL = u
	} else {
		if err := json.Unmarshal(data, &sr); err != nil {
			return err
		}
	}

	ar := JWKSArgs(sr)
	if ar.URL == nil || ar.URL.String() == "" {
		*r = ar
		return nil
	}
	if err := ar.Validate(); err != nil {
		return err
	}

	if err := ar.PopulatePublicKey(); err != nil {
		return err
	}

	*r = ar
	return nil
}

// JWT - rs client grants provider details.
type JWT struct {
	args JWKSArgs
}

func expToInt64(expI interface{}) (expAt int64, err error) {
	switch exp := expI.(type) {
	case float64:
		expAt = int64(exp)
	case int64:
		expAt = exp
	case json.Number:
		expAt, err = exp.Int64()
		if err != nil {
			return 0, err
		}
	default:
		return 0, errors.New("invalid expiry value")
	}
	return expAt, nil
}

func getDefaultExpiration(dsecs string) (time.Duration, error) {
	defaultExpiryDuration := time.Duration(60) * time.Minute // Defaults to 1hr.
	if dsecs != "" {
		expirySecs, err := strconv.ParseInt(dsecs, 10, 64)
		if err != nil {
			return 0, err
		}
		// The duration, in seconds, of the role session.
		// The value can range from 900 seconds (15 minutes)
		// to 12 hours.
		if expirySecs < 900 || expirySecs > 43200 {
			return 0, errors.New("out of range value for duration in seconds")
		}

		defaultExpiryDuration = time.Duration(expirySecs) * time.Second
	}
	return defaultExpiryDuration, nil
}

// newCustomHTTPTransport returns a new http configuration
// used while communicating with the cloud backends.
// This sets the value for MaxIdleConnsPerHost from 2 (go default)
// to 100.
func newCustomHTTPTransport(insecure bool) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: insecure},
		DisableCompression:    true,
	}
}

// Validate - validates the access token.
func (p *JWT) Validate(token, dsecs string) (map[string]interface{}, error) {
	keyFuncCallback := func(jwtToken *jwtgo.Token) (interface{}, error) {
		if _, ok := jwtToken.Method.(*jwtgo.SigningMethodRSA); !ok {
			if _, ok = jwtToken.Method.(*jwtgo.SigningMethodECDSA); ok {
				return p.args.publicKey, nil
			}
			return nil, fmt.Errorf("Unexpected signing method: %v", jwtToken.Header["alg"])
		}
		return p.args.publicKey, nil
	}

	var claims jwtgo.MapClaims
	jwtToken, err := jwtgo.ParseWithClaims(token, &claims, keyFuncCallback)
	if err != nil {
		if err = p.args.PopulatePublicKey(); err != nil {
			return nil, err
		}
		jwtToken, err = jwtgo.ParseWithClaims(token, &claims, keyFuncCallback)
		if err != nil {
			return nil, err
		}
	}

	if !jwtToken.Valid {
		return nil, fmt.Errorf("Invalid token: %v", token)
	}

	expAt, err := expToInt64(claims["exp"])
	if err != nil {
		return nil, err
	}

	defaultExpiryDuration, err := getDefaultExpiration(dsecs)
	if err != nil {
		return nil, err
	}

	if time.Unix(expAt, 0).UTC().Sub(time.Now().UTC()) < defaultExpiryDuration {
		defaultExpiryDuration = time.Unix(expAt, 0).UTC().Sub(time.Now().UTC())
	}

	expiry := time.Now().UTC().Add(defaultExpiryDuration).Unix()
	if expAt < expiry {
		claims["exp"] = strconv.FormatInt(expAt, 64)
	}

	return claims, nil

}

// ID returns the provider name and authentication type.
func (p *JWT) ID() ID {
	return "jwt"
}

// NewJWT - initialize new jwt authenticator.
func NewJWT(args JWKSArgs) *JWT {
	return &JWT{
		args: args,
	}
}
