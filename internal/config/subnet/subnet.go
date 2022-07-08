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

package subnet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	xhttp "github.com/minio/minio/internal/http"
)

const (
	respBodyLimit = 1 << 20 // 1 MiB
)

// Post submit 'payload' to specified URL
func (c Config) Post(reqURL string, payload interface{}) (string, error) {
	if len(c.APIKey) == 0 {
		return "", errors.New("Deployment is not registered with SUBNET. Please register the deployment via 'mc support register ALIAS'")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	r, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}

	r.Header.Set("Authorization", c.APIKey)
	r.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: c.transport,
	}

	resp, err := client.Do(r)
	if err != nil {
		return "", err
	}
	defer xhttp.DrainBody(resp.Body)

	respBytes, err := ioutil.ReadAll(io.LimitReader(resp.Body, respBodyLimit))
	if err != nil {
		return "", err
	}
	respStr := string(respBytes)

	if resp.StatusCode == http.StatusOK {
		return respStr, nil
	}

	return respStr, fmt.Errorf("SUBNET request failed with code %d and error: %s", resp.StatusCode, respStr)
}
