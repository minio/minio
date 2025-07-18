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

package cmd

import (
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/gzhttp"
	"github.com/lithammer/shortuuid/v4"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"

	"github.com/minio/minio/internal/auth"
	levent "github.com/minio/minio/internal/config/lambda/event"
	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

var getLambdaEventData = func(bucket, object string, cred auth.Credentials, r *http.Request) (levent.Event, error) {
	host := globalLocalNodeName
	secure := globalIsTLS
	if globalMinioEndpointURL != nil {
		host = globalMinioEndpointURL.Host
		secure = globalMinioEndpointURL.Scheme == "https"
	}

	duration := time.Until(cred.Expiration)
	if duration > time.Hour || duration < time.Hour {
		// Always limit to 1 hour.
		duration = time.Hour
	}

	clnt, err := miniogo.New(host, &miniogo.Options{
		Creds:     credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:    secure,
		Transport: globalRemoteTargetTransport,
		Region:    globalSite.Region(),
	})
	if err != nil {
		return levent.Event{}, err
	}

	reqParams := url.Values{}
	if partNumberStr := r.Form.Get("partNumber"); partNumberStr != "" {
		reqParams.Set("partNumber", partNumberStr)
	}
	for k := range supportedHeadGetReqParams {
		if v := r.Form.Get(k); v != "" {
			reqParams.Set(k, v)
		}
	}

	extraHeaders := http.Header{}
	u, err := clnt.PresignHeader(r.Context(), http.MethodGet, bucket, object, duration, reqParams, extraHeaders)
	if err != nil {
		return levent.Event{}, err
	}

	ckSum := sha256.Sum256([]byte(cred.AccessKey + u.RawQuery))

	eventData := levent.Event{
		GetObjectContext: &levent.GetObjectContext{
			InputS3URL:  u.String(),
			OutputRoute: shortuuid.New(),
			OutputToken: hex.EncodeToString(ckSum[:]),
		},
		UserRequest: levent.UserRequest{
			URL:     r.URL.String(),
			Headers: r.Header.Clone(),
		},
		UserIdentity: levent.Identity{
			Type:        "IAMUser",
			PrincipalID: cred.ParentUser,
			AccessKeyID: cred.AccessKey,
		},
	}
	return eventData, nil
}

func fwdHeadersToS3(h http.Header, w http.ResponseWriter) {
	const trim = "x-amz-fwd-header-"
	for k, v := range h {
		if stringsHasPrefixFold(k, trim) {
			w.Header()[k[len(trim):]] = v
		}
	}
}

func fwdStatusToAPIError(statusCode int, resp *http.Response) *APIError {
	if statusCode < http.StatusBadRequest {
		return nil
	}
	desc := resp.Header.Get(xhttp.AmzFwdErrorMessage)
	if strings.TrimSpace(desc) == "" {
		apiErr := errorCodes.ToAPIErr(ErrInvalidRequest)
		return &apiErr
	}
	code := resp.Header.Get(xhttp.AmzFwdErrorCode)
	if strings.TrimSpace(code) == "" {
		apiErr := errorCodes.ToAPIErr(ErrInvalidRequest)
		apiErr.Description = desc
		return &apiErr
	}
	return &APIError{
		HTTPStatusCode: statusCode,
		Description:    desc,
		Code:           code,
	}
}

// GetObjectLambdaHandler - GET Object with transformed data via lambda functions
// ----------
// This implementation of the GET operation applies lambda functions and returns the
// response generated via the lambda functions. To use this API, you must have READ access
// to the object.
func (api objectAPIHandlers) GetObjectLambdaHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObjectLambda")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Check for auth type to return S3 compatible error.
	cred, _, s3Error := checkRequestAuthTypeCredential(ctx, r, policy.GetObjectAction)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	target, err := globalLambdaTargetList.Lookup(r.Form.Get("lambdaArn"))
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	eventData, err := getLambdaEventData(bucket, object, cred, r)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	resp, err := target.Send(eventData)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	defer resp.Body.Close()

	if eventData.GetObjectContext.OutputRoute != resp.Header.Get(xhttp.AmzRequestRoute) {
		tokenErr := errorCodes.ToAPIErr(ErrInvalidRequest)
		tokenErr.Description = "The request route included in the request is invalid"
		writeErrorResponse(ctx, w, tokenErr, r.URL)
		return
	}

	if subtle.ConstantTimeCompare([]byte(resp.Header.Get(xhttp.AmzRequestToken)), []byte(eventData.GetObjectContext.OutputToken)) != 1 {
		tokenErr := errorCodes.ToAPIErr(ErrInvalidToken)
		tokenErr.Description = "The request token included in the request is invalid"
		writeErrorResponse(ctx, w, tokenErr, r.URL)
		return
	}

	statusCode := resp.StatusCode
	if status := resp.Header.Get(xhttp.AmzFwdStatus); status != "" {
		statusCode, err = strconv.Atoi(status)
		if err != nil {
			writeErrorResponse(ctx, w, APIError{
				Code:           "LambdaFunctionStatusError",
				HTTPStatusCode: http.StatusBadRequest,
				Description:    err.Error(),
			}, r.URL)
			return
		}
	}

	// Set all the relevant lambda forward headers if found.
	fwdHeadersToS3(resp.Header, w)

	if apiErr := fwdStatusToAPIError(statusCode, resp); apiErr != nil {
		writeErrorResponse(ctx, w, *apiErr, r.URL)
		return
	}

	if !globalAPIConfig.shouldGzipObjects() {
		w.Header().Set(gzhttp.HeaderNoCompression, "true")
	}

	w.WriteHeader(statusCode)
	io.Copy(w, resp.Body)
}
