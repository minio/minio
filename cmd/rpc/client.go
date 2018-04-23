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

package rpc

import (
	"bytes"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	xnet "github.com/minio/minio/pkg/net"
)

// DefaultRPCTimeout - default RPC timeout is five minutes.
const DefaultRPCTimeout = 5 * time.Minute

// Client - http based RPC client.
type Client struct {
	httpClient *http.Client
	serviceURL *xnet.URL
}

// Call - calls service method on RPC server.
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	replyKind := reflect.TypeOf(reply).Kind()
	if replyKind != reflect.Ptr {
		return fmt.Errorf("rpc reply must be a pointer type, but found %v", replyKind)
	}

	data, err := gobEncode(args)
	if err != nil {
		return err
	}

	callRequest := CallRequest{
		Method:   serviceMethod,
		ArgBytes: data,
	}

	var buf bytes.Buffer
	if err = gob.NewEncoder(&buf).Encode(callRequest); err != nil {
		return err
	}

	response, err := client.httpClient.Post(client.serviceURL.String(), "", &buf)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%v rpc call failed with error code %v", serviceMethod, response.StatusCode)
	}

	var callResponse CallResponse
	if err := gob.NewDecoder(response.Body).Decode(&callResponse); err != nil {
		return err
	}

	if callResponse.Error != "" {
		return errors.New(callResponse.Error)
	}

	return gobDecode(callResponse.ReplyBytes, reply)
}

// Close - does nothing and presents for interface compatibility.
func (client *Client) Close() error {
	return nil
}

// NewClient - returns new RPC client.
func NewClient(serviceURL *xnet.URL, tlsConfig *tls.Config, timeout time.Duration) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		},
		serviceURL: serviceURL,
	}
}
