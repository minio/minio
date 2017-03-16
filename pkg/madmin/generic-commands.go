/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
 *
 */

package madmin

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/url"
)

// setCredsReq - xml to send to the server to set new credentials
type setCredsReq struct {
	Username string `xml:"username"`
	Password string `xml:"password"`
}

// SetCredentials - Call Set Credentials API to set new access and secret keys in the specified Minio server
func (adm *AdminClient) SetCredentials(access, secret string) error {

	// Setup new request
	reqData := requestData{}
	reqData.queryValues = make(url.Values)
	reqData.queryValues.Set("service", "")
	reqData.customHeaders = make(http.Header)
	reqData.customHeaders.Set(minioAdminOpHeader, "set-credentials")

	// Setup request's body
	body, err := xml.Marshal(setCredsReq{Username: access, Password: secret})
	if err != nil {
		return err
	}
	reqData.contentBody = bytes.NewReader(body)
	reqData.contentLength = int64(len(body))
	reqData.contentMD5Bytes = sumMD5(body)
	reqData.contentSHA256Bytes = sum256(body)

	// Execute GET on bucket to list objects.
	resp, err := adm.executeMethod("POST", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	// Return error to the caller if http response code is different from 200
	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}
	return nil
}
