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

package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	xhttp "github.com/minio/minio/internal/http"
)

const (
	subnetRespBodyLimit = 1 << 20 // 1 MiB
	callhomeURL         = "https://subnet.min.io/api/callhome"
	callhomeURLDev      = "http://localhost:9000/api/callhome"
)

func httpClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: globalProxyTransport,
	}
}

func subnetHTTPDo(req *http.Request) (*http.Response, error) {
	client := httpClient(10 * time.Second)
	if globalSubnetConfig.ProxyURL != nil {
		client.Transport.(*http.Transport).Proxy = http.ProxyURL((*url.URL)(globalSubnetConfig.ProxyURL))
	}
	return client.Do(req)
}

func subnetReqDo(r *http.Request, authToken string) (string, error) {
	r.Header.Set("Authorization", authToken)
	r.Header.Set("Content-Type", "application/json")

	resp, err := subnetHTTPDo(r)
	if resp != nil {
		defer xhttp.DrainBody(resp.Body)
	}
	if err != nil {
		return "", err
	}

	respBytes, err := ioutil.ReadAll(io.LimitReader(resp.Body, subnetRespBodyLimit))
	if err != nil {
		return "", err
	}
	respStr := string(respBytes)

	if resp.StatusCode == http.StatusOK {
		return respStr, nil
	}
	return respStr, fmt.Errorf("SUBNET request failed with code %d and error: %s", resp.StatusCode, respStr)
}

func subnetPostReq(reqURL string, payload interface{}, authToken string) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	r, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	return subnetReqDo(r, authToken)
}

func sendCallhomeInfo(ch CallhomeInfo) error {
	if len(globalSubnetConfig.APIKey) == 0 {
		return errors.New("Cluster is not registered with SUBNET. Please register by running 'mc support register ALIAS'")
	}

	url := callhomeURL
	if globalIsCICD {
		url = callhomeURLDev
	}
	_, err := subnetPostReq(url, ch, globalSubnetConfig.APIKey)
	return err
}
