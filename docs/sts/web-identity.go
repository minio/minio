// +build ignore

/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/minio/minio/pkg/auth"
)

// AssumedRoleUser - The identifiers for the temporary security credentials that
// the operation returns. Please also see https://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumedRoleUser
type AssumedRoleUser struct {
	Arn           string
	AssumedRoleID string `xml:"AssumeRoleId"`
	// contains filtered or unexported fields
}

// AssumeRoleWithWebIdentityResponse contains the result of successful AssumeRoleWithWebIdentity request.
type AssumeRoleWithWebIdentityResponse struct {
	XMLName          xml.Name          `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleWithWebIdentityResponse" json:"-"`
	Result           WebIdentityResult `xml:"AssumeRoleWithWebIdentityResult"`
	ResponseMetadata struct {
		RequestID string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

// WebIdentityResult - Contains the response to a successful AssumeRoleWithWebIdentity
// request, including temporary credentials that can be used to make MinIO API requests.
type WebIdentityResult struct {
	AssumedRoleUser             AssumedRoleUser  `xml:",omitempty"`
	Audience                    string           `xml:",omitempty"`
	Credentials                 auth.Credentials `xml:",omitempty"`
	PackedPolicySize            int              `xml:",omitempty"`
	Provider                    string           `xml:",omitempty"`
	SubjectFromWebIdentityToken string           `xml:",omitempty"`
}

// Returns a base64 encoded random 32 byte string.
func randomState() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

var (
	stsEndpoint    string
	configEndpoint string
	clientID       string
	clientSec      string
	clientScopes   string
	port           int
)

// DiscoveryDoc - parses the output from openid-configuration
// for example https://accounts.google.com/.well-known/openid-configuration
type DiscoveryDoc struct {
	Issuer                           string   `json:"issuer,omitempty"`
	AuthEndpoint                     string   `json:"authorization_endpoint,omitempty"`
	TokenEndpoint                    string   `json:"token_endpoint,omitempty"`
	UserInfoEndpoint                 string   `json:"userinfo_endpoint,omitempty"`
	RevocationEndpoint               string   `json:"revocation_endpoint,omitempty"`
	JwksURI                          string   `json:"jwks_uri,omitempty"`
	ResponseTypesSupported           []string `json:"response_types_supported,omitempty"`
	SubjectTypesSupported            []string `json:"subject_types_supported,omitempty"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported,omitempty"`
	ScopesSupported                  []string `json:"scopes_supported,omitempty"`
	TokenEndpointAuthMethods         []string `json:"token_endpoint_auth_methods_supported,omitempty"`
	ClaimsSupported                  []string `json:"claims_supported,omitempty"`
	CodeChallengeMethodsSupported    []string `json:"code_challenge_methods_supported,omitempty"`
}

func parseDiscoveryDoc(ustr string) (DiscoveryDoc, error) {
	d := DiscoveryDoc{}
	req, err := http.NewRequest(http.MethodGet, ustr, nil)
	if err != nil {
		return d, err
	}
	clnt := http.Client{
		Transport: http.DefaultTransport,
	}
	resp, err := clnt.Do(req)
	if err != nil {
		return d, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return d, err
	}
	dec := json.NewDecoder(resp.Body)
	if err = dec.Decode(&d); err != nil {
		return d, err
	}
	return d, nil
}

func init() {
	flag.StringVar(&stsEndpoint, "sts-ep", "http://localhost:9000", "STS endpoint")
	flag.StringVar(&configEndpoint, "config-ep",
		"https://accounts.google.com/.well-known/openid-configuration",
		"OpenID discovery document endpoint")
	flag.StringVar(&clientID, "cid", "", "Client ID")
	flag.StringVar(&clientSec, "csec", "", "Client Secret")
	flag.StringVar(&clientScopes, "cscopes", "openid", "Client Scopes")
	flag.IntVar(&port, "port", 8080, "Port")
}

func main() {
	flag.Parse()
	if clientID == "" || clientSec == "" {
		flag.PrintDefaults()
		return
	}

	ddoc, err := parseDiscoveryDoc(configEndpoint)
	if err != nil {
		log.Println(fmt.Errorf("Failed to parse OIDC discovery document %s", err))
		fmt.Println(err)
		return
	}

	scopes :=  ddoc.ScopesSupported
	if clientScopes != "" {
		scopes = strings.Split(clientScopes, ",");
	}

	ctx := context.Background()

	config := oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSec,
		Endpoint: oauth2.Endpoint{
			AuthURL:  ddoc.AuthEndpoint,
			TokenURL: ddoc.TokenEndpoint,
		},
		RedirectURL: fmt.Sprintf("http://localhost:%d/oauth2/callback", port),
		Scopes:      scopes,
	}

	state := randomState()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.RequestURI)
		if r.RequestURI != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, config.AuthCodeURL(state), http.StatusFound)
	})

	http.HandleFunc("/oauth2/callback", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.RequestURI)
		if r.URL.Query().Get("state") != state {
			http.Error(w, "state did not match", http.StatusBadRequest)
			return
		}

		getWebTokenExpiry := func() (*credentials.WebIdentityToken, error) {
			oauth2Token, err := config.Exchange(ctx, r.URL.Query().Get("code"))
			if err != nil {
				return nil, err
			}
			if !oauth2Token.Valid() {
				return nil, errors.New("invalid token")
			}

			return &credentials.WebIdentityToken{
				Token:  oauth2Token.Extra("id_token").(string),
				Expiry: int(oauth2Token.Expiry.Sub(time.Now().UTC()).Seconds()),
			}, nil
		}

		sts, err := credentials.NewSTSWebIdentity(stsEndpoint, getWebTokenExpiry)
		if err != nil {
			log.Println(fmt.Errorf("Could not get STS credentials: %s", err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		opts := &minio.Options{
			Creds:        sts,
			BucketLookup: minio.BucketLookupAuto,
		}

		u, err := url.Parse(stsEndpoint)
		if err != nil {
			log.Println(fmt.Errorf("Failed to parse STS Endpoint: %s", err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		clnt, err := minio.NewWithOptions(u.Host, opts)
		if err != nil {
			log.Println(fmt.Errorf("Error while initializing Minio client, %s", err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		buckets, err := clnt.ListBuckets()
		if err != nil {
			log.Println(fmt.Errorf("Error while listing buckets, %s", err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		creds, _ := sts.Get()

		bucketNames := []string{}

		for _, bucket := range buckets {
			log.Println(fmt.Sprintf("Bucket discovered: %s", bucket.Name))
			bucketNames = append(bucketNames, bucket.Name)
		}
		response := make(map[string]interface{})
		response["credentials"] = creds
		response["buckets"] = bucketNames
		c, err := json.MarshalIndent(response, "", "\t")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(c)
	})

	address := fmt.Sprintf("localhost:%v", port)
	log.Printf("listening on http://%s/", address)
	log.Fatal(http.ListenAndServe(address, nil))
}
