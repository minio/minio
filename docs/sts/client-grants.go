// +build ignore

/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	minio "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
)

// JWTToken - parses the output from IDP access token.
type JWTToken struct {
	AccessToken string `json:"access_token"`
	Expiry      int    `json:"expires_in"`
}

var (
	stsEndpoint  string
	idpEndpoint  string
	clientID     string
	clientSecret string
)

func init() {
	flag.StringVar(&stsEndpoint, "sts-ep", "http://localhost:9000", "STS endpoint")
	flag.StringVar(&idpEndpoint, "idp-ep", "https://localhost:9443/oauth2/token", "IDP endpoint")
	flag.StringVar(&clientID, "cid", "", "Client ID")
	flag.StringVar(&clientSecret, "csec", "", "Client secret")
}

func getTokenExpiry() (*credentials.ClientGrantsToken, error) {
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, idpEndpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(clientID, clientSecret)
	t := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	hclient := http.Client{
		Transport: t,
	}
	resp, err := hclient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s", resp.Status)
	}

	var idpToken JWTToken
	if err = json.NewDecoder(resp.Body).Decode(&idpToken); err != nil {
		return nil, err
	}

	return &credentials.ClientGrantsToken{Token: idpToken.AccessToken, Expiry: idpToken.Expiry}, nil
}

func main() {
	flag.Parse()
	if clientID == "" || clientSecret == "" {
		flag.PrintDefaults()
		return
	}

	sts, err := credentials.NewSTSClientGrants(stsEndpoint, getTokenExpiry)
	if err != nil {
		log.Fatal(err)
	}

	// Uncommend this to use MinIO API operations by initializing minio
	// client with obtained credentials.

	opts := &minio.Options{
		Creds:        sts,
		BucketLookup: minio.BucketLookupAuto,
	}

	u, err := url.Parse(stsEndpoint)
	if err != nil {
		log.Fatal(err)
	}

	clnt, err := minio.NewWithOptions(u.Host, opts)
	if err != nil {
		log.Fatal(err)
	}

	d := bytes.NewReader([]byte("Hello, World"))
	n, err := clnt.PutObject("my-bucketname", "my-objectname", d, d.Size(), minio.PutObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Uploaded", "my-objectname", " of size: ", n, "Successfully.")
}
