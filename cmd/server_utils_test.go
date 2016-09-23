/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"

	. "gopkg.in/check.v1"
)

// concurreny level for certain parallel tests.
const (
	testConcurrencyLevel = 10
)

///
/// Excerpts from @lsegal - https://github.com/aws/aws-sdk-js/issues/659#issuecomment-120477258
///
///  User-Agent:
///
///      This is ignored from signing because signing this causes problems with generating pre-signed URLs
///      (that are executed by other agents) or when customers pass requests through proxies, which may
///      modify the user-agent.
///
///  Content-Length:
///
///      This is ignored from signing because generating a pre-signed URL should not provide a content-length
///      constraint, specifically when vending a S3 pre-signed PUT URL. The corollary to this is that when
///      sending regular requests (non-pre-signed), the signature contains a checksum of the body, which
///      implicitly validates the payload length (since changing the number of bytes would change the checksum)
///      and therefore this header is not valuable in the signature.
///
///  Content-Type:
///
///      Signing this header causes quite a number of problems in browser environments, where browsers
///      like to modify and normalize the content-type header in different ways. There is more information
///      on this in https://github.com/aws/aws-sdk-js/issues/244. Avoiding this field simplifies logic
///      and reduces the possibility of future bugs
///
///  Authorization:
///
///      Is skipped for obvious reasons
///
var ignoredHeaders = map[string]bool{
	"Authorization":  true,
	"Content-Type":   true,
	"Content-Length": true,
	"User-Agent":     true,
}

// Headers to ignore in streaming v4
var ignoredStreamingHeaders = map[string]bool{
	"Authorization": true,
	"Content-Type":  true,
	"Content-Md5":   true,
	"User-Agent":    true,
}

// calculateSignedChunkLength - calculates the length of chunk metadata
func calculateSignedChunkLength(chunkDataSize int64) int64 {
	return int64(len(fmt.Sprintf("%x", chunkDataSize))) +
		17 + // ";chunk-signature="
		64 + // e.g. "f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2"
		2 + // CRLF
		chunkDataSize +
		2 // CRLF
}

// calculateSignedChunkLength - calculates the length of the overall stream (data + metadata)
func calculateStreamContentLength(dataLen, chunkSize int64) int64 {
	if dataLen <= 0 {
		return 0
	}
	chunksCount := int64(dataLen / chunkSize)
	remainingBytes := int64(dataLen % chunkSize)
	streamLen := int64(0)
	streamLen += chunksCount * calculateSignedChunkLength(chunkSize)
	if remainingBytes > 0 {
		streamLen += calculateSignedChunkLength(remainingBytes)
	}
	streamLen += calculateSignedChunkLength(0)
	return streamLen
}

// Ask the kernel for a free open port.
func getFreePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func verifyError(c *C, response *http.Response, code, description string, statusCode int) {
	data, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	errorResponse := APIErrorResponse{}
	err = xml.Unmarshal(data, &errorResponse)
	c.Assert(err, IsNil)
	c.Assert(errorResponse.Code, Equals, code)
	c.Assert(errorResponse.Message, Equals, description)
	c.Assert(response.StatusCode, Equals, statusCode)
}
