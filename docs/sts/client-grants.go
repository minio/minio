//go:build ignore
// +build ignore

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

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// JWTToken - parses the output from IDP id_token.
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
	flag.StringVar(&idpEndpoint, "idp-ep", "http://localhost:8080/auth/realms/minio/protocol/openid-connect/token", "IDP token endpoint")
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

	// Uncomment this to use MinIO API operations by initializing minio
	// client with obtained credentials.

	opts := &minio.Options{
		Creds:        sts,
		BucketLookup: minio.BucketLookupAuto,
	}

	u, err := url.Parse(stsEndpoint)
	if err != nil {
		log.Fatal(err)
	}

	clnt, err := minio.New(u.Host, opts)
	if err != nil {
		log.Fatal(err)
	}

	d := bytes.NewReader([]byte("Hello, World"))
	n, err := clnt.PutObject(context.Background(), "my-bucketname", "my-objectname", d, d.Size(), minio.PutObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Uploaded", "my-objectname", " of size: ", n, "Successfully.")
}
