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
 */

package validator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/minio/minio/pkg/auth"
	xnet "github.com/minio/minio/pkg/net"
)

// IDPArgs - provides IDP url, client id
//and secret to fetch admin credentials
type IDPArgs struct {
	URL                    *xnet.URL `json:"url"`
	clientID, clientSecret string
}

// IDP - provides IDP
type IDP struct {
	args IDPArgs
}

// jwtToken - parses the output from IDP access token.
type jwtToken struct {
	AccessToken string `json:"access_token"`
	Expiry      int    `json:"expires_in"`
}

func (idp *IDP) getTokenExpiry() (*jwtToken, error) {
	insecureClient := &http.Client{Transport: newCustomHTTPTransport(true)}
	client := &http.Client{Transport: newCustomHTTPTransport(false)}

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, idp.args.URL.String(), strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(idp.args.clientID, idp.args.clientSecret)
	resp, err := client.Do(req)
	if err != nil {
		resp, err = insecureClient.Do(req)
		if err != nil {
			return nil, err
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}

	var idpToken = &jwtToken{}
	if err = json.NewDecoder(resp.Body).Decode(idpToken); err != nil {
		return nil, err
	}

	return idpToken, nil
}

// GetCredentials - get credentials auth
func (idp *IDP) GetCredentials(validate func(token, durationSecs string) (map[string]interface{}, error)) (auth.Credentials, error) {
	grants, err := idp.getTokenExpiry()
	if err != nil {
		return auth.Credentials{}, err
	}

	m, err := validate(grants.AccessToken, fmt.Sprintf("%s", grants.Expiry))
	if err != nil {
		return auth.Credentials{}, err
	}

	m["policy"] = "admin"
	return auth.GetNewCredentialsWithMetadata(m, idp.args.clientSecret)
}

// Validate IDP authentication target arguments
func (r *IDPArgs) Validate() error {
	return nil
}

// UnmarshalJSON - decodes JSON data.
func (r *IDPArgs) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subIDPArgs IDPArgs
	var sr subIDPArgs

	// IAM related envs.
	if idpURL, ok := os.LookupEnv("MINIO_IAM_IDP_URL"); ok {
		u, err := xnet.ParseURL(idpURL)
		if err != nil {
			return err
		}
		sr.URL = u
	} else {
		if err := json.Unmarshal(data, &sr); err != nil {
			return err
		}
	}

	ar := IDPArgs(sr)
	if ar.URL == nil || ar.URL.String() == "" {
		*r = ar
		return nil
	}
	if err := ar.Validate(); err != nil {
		return err
	}

	*r = ar
	return nil
}

// NewIDP - initialize new IDP
func NewIDP(args IDPArgs) *IDP {
	return &IDP{
		args: args,
	}
}
