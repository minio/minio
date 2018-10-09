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

package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	minio "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
	"github.com/minio/minio/pkg/auth"
)

// AssumedRoleUser - The identifiers for the temporary security credentials that
// the operation returns. Please also see https://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumedRoleUser
type AssumedRoleUser struct {
	Arn           string
	AssumedRoleID string `xml:"AssumeRoleId"`
	// contains filtered or unexported fields
}

// AssumeRoleWithClientGrantsResponse contains the result of successful AssumeRoleWithClientGrants request.
type AssumeRoleWithClientGrantsResponse struct {
	XMLName          xml.Name           `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleWithClientGrantsResponse" json:"-"`
	Result           ClientGrantsResult `xml:"AssumeRoleWithClientGrantsResult"`
	ResponseMetadata struct {
		RequestID string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

// ClientGrantsResult - Contains the response to a successful AssumeRoleWithClientGrants
// request, including temporary credentials that can be used to make Minio API requests.
type ClientGrantsResult struct {
	AssumedRoleUser              AssumedRoleUser  `xml:",omitempty"`
	Audience                     string           `xml:",omitempty"`
	Credentials                  auth.Credentials `xml:",omitempty"`
	PackedPolicySize             int              `xml:",omitempty"`
	Provider                     string           `xml:",omitempty"`
	SubjectFromClientGrantsToken string           `xml:",omitempty"`
}

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

func main() {
	flag.Parse()
	if clientID == "" || clientSecret == "" {
		flag.PrintDefaults()
		return
	}

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, idpEndpoint, strings.NewReader(data.Encode()))
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatal(resp.Status)
	}

	var idpToken JWTToken
	if err = json.NewDecoder(resp.Body).Decode(&idpToken); err != nil {
		log.Fatal(err)
	}

	v := url.Values{}
	v.Set("Action", "AssumeRoleWithClientGrants")
	v.Set("Token", idpToken.AccessToken)
	v.Set("DurationSeconds", fmt.Sprintf("%d", idpToken.Expiry))

	u, err := url.Parse(stsEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	u.RawQuery = v.Encode()

	req, err = http.NewRequest("POST", u.String(), nil)
	if err != nil {
		log.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatal(resp.Status)
	}

	a := AssumeRoleWithClientGrantsResponse{}
	if err = xml.NewDecoder(resp.Body).Decode(&a); err != nil {
		log.Fatal(err)
	}

	fmt.Println("##### Credentials")
	c, err := json.MarshalIndent(a.Result.Credentials, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(c))

	// Uncommend this to use Minio API operations by initializin minio
	// client with obtained credentials.

	opts := &minio.Options{
		Creds: credentials.NewStaticV4(a.Result.Credentials.AccessKey,
			a.Result.Credentials.SecretKey,
			a.Result.Credentials.SessionToken,
		),
		BucketLookup: minio.BucketLookupAuto,
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
