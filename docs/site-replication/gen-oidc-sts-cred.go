//go:build ignore
// +build ignore

// Copyright (c) 2015-2022 MinIO, Inc.
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

// This programs mocks user interaction against Dex IDP and generates STS
// credentials. It is for MinIO testing purposes only.
//
// Run like:
//
// $ MINIO_ENDPOINT=http://localhost:9000 go run gen-oidc-sts-cred.go

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	cr "github.com/minio/minio-go/v7/pkg/credentials"
	cmd "github.com/minio/minio/cmd"
)

func main() {
	ctx := context.Background()

	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		log.Fatalf("Please specify a MinIO server endpoint environment variable like:\n\n\texport MINIO_ENDPOINT=http://localhost:9000")
	}

	appParams := cmd.OpenIDClientAppParams{
		ClientID:     "minio-client-app",
		ClientSecret: "minio-client-app-secret",
		ProviderURL:  "http://127.0.0.1:5556/dex",
		RedirectURL:  "http://127.0.0.1:10000/oauth_callback",
	}

	oidcToken, err := cmd.MockOpenIDTestUserInteraction(ctx, appParams, "dillon@example.io", "dillon")
	if err != nil {
		log.Fatalf("Failed to generate OIDC token: %v", err)
	}

	roleARN := os.Getenv("ROLE_ARN")
	webID := cr.STSWebIdentity{
		Client:      &http.Client{},
		STSEndpoint: endpoint,
		GetWebIDTokenExpiry: func() (*cr.WebIdentityToken, error) {
			return &cr.WebIdentityToken{
				Token: oidcToken,
			}, nil
		},
		RoleARN: roleARN,
	}

	value, err := webID.Retrieve()
	if err != nil {
		log.Fatalf("Expected to generate credentials: %v", err)
	}

	// Print credentials separated by colons:
	fmt.Printf("%s:%s:%s\n", value.AccessKeyID, value.SecretAccessKey, value.SessionToken)
}
