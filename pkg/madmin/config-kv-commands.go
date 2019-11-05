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
 *
 */

package madmin

import (
	"bufio"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
)

// DelConfigKV - delete key from server config.
func (adm *AdminClient) DelConfigKV(k string) (err error) {
	econfigBytes, err := EncryptData(adm.secretAccessKey, []byte(k))
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: adminAPIPrefix + "/del-config-kv",
		content: econfigBytes,
	}

	// Execute DELETE on /minio/admin/v2/del-config-kv to delete config key.
	resp, err := adm.executeMethod(http.MethodDelete, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// SetConfigKV - set key value config to server.
func (adm *AdminClient) SetConfigKV(kv string) (err error) {
	bio := bufio.NewScanner(strings.NewReader(kv))
	var s strings.Builder
	var comment string
	for bio.Scan() {
		if bio.Text() == "" {
			continue
		}
		if strings.HasPrefix(bio.Text(), KvComment) {
			// Join multiple comments for each newline, separated by "\n"
			comments := []string{comment, strings.TrimPrefix(bio.Text(), KvComment)}
			comment = strings.Join(comments, KvNewline)
			continue
		}
		s.WriteString(bio.Text())
		if comment != "" {
			s.WriteString(KvSpaceSeparator)
			s.WriteString(commentKey)
			s.WriteString(KvSeparator)
			s.WriteString(KvDoubleQuote)
			s.WriteString(base64.RawStdEncoding.EncodeToString([]byte(comment)))
			s.WriteString(KvDoubleQuote)
		}
		s.WriteString(KvNewline)
		comment = ""
	}

	econfigBytes, err := EncryptData(adm.secretAccessKey, []byte(s.String()))
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: adminAPIPrefix + "/set-config-kv",
		content: econfigBytes,
	}

	// Execute PUT on /minio/admin/v2/set-config-kv to set config key/value.
	resp, err := adm.executeMethod(http.MethodPut, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// GetConfigKV - returns the key, value of the requested key, incoming data is encrypted.
func (adm *AdminClient) GetConfigKV(key string) (Targets, error) {
	v := url.Values{}
	v.Set("key", key)

	// Execute GET on /minio/admin/v2/get-config-kv?key={key} to get value of key.
	resp, err := adm.executeMethod(http.MethodGet,
		requestData{
			relPath:     adminAPIPrefix + "/get-config-kv",
			queryValues: v,
		})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	defer closeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	data, err := DecryptData(adm.secretAccessKey, resp.Body)
	if err != nil {
		return nil, err
	}

	return ParseSubSysTarget(data)
}
