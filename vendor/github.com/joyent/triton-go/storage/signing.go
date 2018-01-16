//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package storage

import (
	"bytes"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// SignURLInput represents parameters to a SignURL operation.
type SignURLInput struct {
	ValidityPeriod time.Duration
	Method         string
	ObjectPath     string
}

// SignURLOutput contains the outputs of a SignURL operation. To simply
// access the signed URL, use the SignedURL method.
type SignURLOutput struct {
	host       string
	objectPath string
	Method     string
	Algorithm  string
	Signature  string
	Expires    string
	KeyID      string
}

// SignedURL returns a signed URL for the given scheme. Valid schemes are
// `http` and `https`.
func (output *SignURLOutput) SignedURL(scheme string) string {
	query := &url.Values{}
	query.Set("algorithm", output.Algorithm)
	query.Set("expires", output.Expires)
	query.Set("keyId", output.KeyID)
	query.Set("signature", output.Signature)

	sUrl := url.URL{}
	sUrl.Scheme = scheme
	sUrl.Host = output.host
	sUrl.Path = output.objectPath
	sUrl.RawQuery = query.Encode()

	return sUrl.String()
}

// SignURL creates a time-expiring URL that can be shared with others.
// This is useful to generate HTML links, for example.
func (s *StorageClient) SignURL(input *SignURLInput) (*SignURLOutput, error) {
	output := &SignURLOutput{
		host:       s.Client.MantaURL.Host,
		objectPath: fmt.Sprintf("/%s%s", s.Client.AccountName, input.ObjectPath),
		Method:     input.Method,
		Algorithm:  strings.ToUpper(s.Client.Authorizers[0].DefaultAlgorithm()),
		Expires:    strconv.FormatInt(time.Now().Add(input.ValidityPeriod).Unix(), 10),
		KeyID:      path.Join("/", s.Client.AccountName, "keys", s.Client.Authorizers[0].KeyFingerprint()),
	}

	toSign := bytes.Buffer{}
	toSign.WriteString(input.Method + "\n")
	toSign.WriteString(s.Client.MantaURL.Host + "\n")
	toSign.WriteString(fmt.Sprintf("/%s%s\n", s.Client.AccountName, input.ObjectPath))

	query := &url.Values{}
	query.Set("algorithm", output.Algorithm)
	query.Set("expires", output.Expires)
	query.Set("keyId", output.KeyID)
	toSign.WriteString(query.Encode())

	signature, _, err := s.Client.Authorizers[0].SignRaw(toSign.String())
	if err != nil {
		return nil, errors.Wrapf(err, "error signing string")
	}

	output.Signature = signature
	return output, nil
}
