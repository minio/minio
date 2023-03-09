// Copyright (c) 2015-2023 MinIO, Inc.
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

package event

import "net/http"

// Identity represents access key who caused the event.
type Identity struct {
	Type        string `json:"type"`
	PrincipalID string `json:"principalId"`
	AccessKeyID string `json:"accessKeyId"`
}

// UserRequest user request headers
type UserRequest struct {
	URL     string      `json:"url"`
	Headers http.Header `json:"headers"`
}

// GetObjectContext provides the necessary details to perform
// download of the object, and return back the processed response
// to the server.
type GetObjectContext struct {
	OutputRoute string `json:"outputRoute"`
	OutputToken string `json:"outputToken"`
	InputS3URL  string `json:"inputS3Url"`
}

// Event represents lambda function event, this is undocumented in AWS S3. This
// structure bases itself on this structure but there is no binding.
//
//	{
//	  "xAmzRequestId": "a2871150-1df5-4dc9-ad9f-3da283ca1bf3",
//	  "getObjectContext": {
//	    "outputRoute": "...",
//	    "outputToken": "...",
//	    "inputS3Url": "<presignedURL>"
//	  },
//	  "configuration": { // not useful in MinIO
//	    "accessPointArn": "...",
//	    "supportingAccessPointArn": "...",
//	    "payload": ""
//	  },
//	  "userRequest": {
//	    "url": "...",
//	    "headers": {
//	      "Host": "...",
//	      "X-Amz-Content-SHA256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
//	    }
//	  },
//	  "userIdentity": {
//	    "type": "IAMUser",
//	    "principalId": "AIDAJF5MO57RFXQCE5ZNC",
//	    "arn": "...",
//	    "accountId": "...",
//	    "accessKeyId": "AKIA3WNQJCXE2DYPAU7R"
//	  },
//	  "protocolVersion": "1.00"
//	}
type Event struct {
	ProtocolVersion  string            `json:"protocolVersion"`
	GetObjectContext *GetObjectContext `json:"getObjectContext"`
	UserIdentity     Identity          `json:"userIdentity"`
	UserRequest      UserRequest       `json:"userRequest"`
}
