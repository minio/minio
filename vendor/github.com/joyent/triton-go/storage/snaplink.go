//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package storage

import (
	"context"
	"fmt"
	"net/http"

	"github.com/joyent/triton-go/client"
	"github.com/pkg/errors"
)

type SnapLinksClient struct {
	client *client.Client
}

// PutSnapLinkInput represents parameters to a PutSnapLink operation.
type PutSnapLinkInput struct {
	LinkPath   string
	SourcePath string
}

// PutSnapLink creates a SnapLink to an object.
func (s *SnapLinksClient) Put(ctx context.Context, input *PutSnapLinkInput) error {
	linkPath := fmt.Sprintf("/%s%s", s.client.AccountName, input.LinkPath)
	sourcePath := fmt.Sprintf("/%s%s", s.client.AccountName, input.SourcePath)
	headers := &http.Header{}
	headers.Set("Content-Type", "application/json; type=link")
	headers.Set("location", sourcePath)
	headers.Set("Accept", "~1.0")
	headers.Set("Accept-Version", "application/json, */*")

	reqInput := client.RequestInput{
		Method:  http.MethodPut,
		Path:    linkPath,
		Headers: headers,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errors.Wrapf(err, "unable to put snaplink")
	}

	return nil
}
