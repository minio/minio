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
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lithammer/shortuuid/v4"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/mux"
	"github.com/minio/pkg/bucket/policy"

	"github.com/minio/minio/internal/auth"
	levent "github.com/minio/minio/internal/config/lambda/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

func getLambdaEventData(bucket, object string, cred auth.Credentials, r *http.Request) (levent.Event, error) {
	host := globalLocalNodeName
	secure := globalIsTLS
	if globalMinioEndpointURL != nil {
		host = globalMinioEndpointURL.Host
		secure = globalMinioEndpointURL.Scheme == "https"
	}

	duration := time.Since(cred.Expiration)
	if cred.Expiration.IsZero() {
		duration = time.Hour
	}

	clnt, err := miniogo.New(host, &miniogo.Options{
		Creds:     credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
		Secure:    secure,
		Transport: globalRemoteTargetTransport,
		Region:    globalSite.Region,
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
	if rng := r.Header.Get(xhttp.Range); rng != "" {
		extraHeaders.Set(xhttp.Range, r.Header.Get(xhttp.Range))
	}

	u, err := clnt.PresignHeader(r.Context(), http.MethodGet, bucket, object, duration, reqParams, extraHeaders)
	if err != nil {
		return levent.Event{}, err
	}

	token, err := authenticateNode(cred.AccessKey, cred.SecretKey, u.RawQuery)
	if err != nil {
		return levent.Event{}, err
	}

	eventData := levent.Event{
		GetObjectContext: &levent.GetObjectContext{
			InputS3URL:  u.String(),
			OutputRoute: shortuuid.New(),
			OutputToken: token,
		},
		UserRequest: levent.UserRequest{
			URL:     r.URL.String(),
			Headers: r.Header.Clone(),
		},
		UserIdentity: levent.Identity{
			Type:        "IAMUser",
			PrincipalID: cred.AccessKey,
			AccessKeyID: cred.SecretKey,
		},
	}
	return eventData, nil
}

// StatusCode returns a HTTP Status code for the HTTP text. It returns -1
// if the text is unknown.
func StatusCode(text string) int {
	switch text {
	case "Continue":
		return http.StatusContinue
	case "Switching Protocols":
		return http.StatusSwitchingProtocols
	case "Processing":
		return http.StatusProcessing
	case "Early Hints":
		return http.StatusEarlyHints
	case "OK":
		return http.StatusOK
	case "Created":
		return http.StatusCreated
	case "Accepted":
		return http.StatusAccepted
	case "Non-Authoritative Information":
		return http.StatusNonAuthoritativeInfo
	case "No Content":
		return http.StatusNoContent
	case "Reset Content":
		return http.StatusResetContent
	case "Partial Content":
		return http.StatusPartialContent
	case "Multi-Status":
		return http.StatusMultiStatus
	case "Already Reported":
		return http.StatusAlreadyReported
	case "IM Used":
		return http.StatusIMUsed
	case "Multiple Choices":
		return http.StatusMultipleChoices
	case "Moved Permanently":
		return http.StatusMovedPermanently
	case "Found":
		return http.StatusFound
	case "See Other":
		return http.StatusSeeOther
	case "Not Modified":
		return http.StatusNotModified
	case "Use Proxy":
		return http.StatusUseProxy
	case "Temporary Redirect":
		return http.StatusTemporaryRedirect
	case "Permanent Redirect":
		return http.StatusPermanentRedirect
	case "Bad Request":
		return http.StatusBadRequest
	case "Unauthorized":
		return http.StatusUnauthorized
	case "Payment Required":
		return http.StatusPaymentRequired
	case "Forbidden":
		return http.StatusForbidden
	case "Not Found":
		return http.StatusNotFound
	case "Method Not Allowed":
		return http.StatusMethodNotAllowed
	case "Not Acceptable":
		return http.StatusNotAcceptable
	case "Proxy Authentication Required":
		return http.StatusProxyAuthRequired
	case "Request Timeout":
		return http.StatusRequestTimeout
	case "Conflict":
		return http.StatusConflict
	case "Gone":
		return http.StatusGone
	case "Length Required":
		return http.StatusLengthRequired
	case "Precondition Failed":
		return http.StatusPreconditionFailed
	case "Request Entity Too Large":
		return http.StatusRequestEntityTooLarge
	case "Request URI Too Long":
		return http.StatusRequestURITooLong
	case "Unsupported Media Type":
		return http.StatusUnsupportedMediaType
	case "Requested Range Not Satisfiable":
		return http.StatusRequestedRangeNotSatisfiable
	case "Expectation Failed":
		return http.StatusExpectationFailed
	case "I'm a teapot":
		return http.StatusTeapot
	case "Misdirected Request":
		return http.StatusMisdirectedRequest
	case "Unprocessable Entity":
		return http.StatusUnprocessableEntity
	case "Locked":
		return http.StatusLocked
	case "Failed Dependency":
		return http.StatusFailedDependency
	case "Too Early":
		return http.StatusTooEarly
	case "Upgrade Required":
		return http.StatusUpgradeRequired
	case "Precondition Required":
		return http.StatusPreconditionRequired
	case "Too Many Requests":
		return http.StatusTooManyRequests
	case "Request Header Fields Too Large":
		return http.StatusRequestHeaderFieldsTooLarge
	case "Unavailable For Legal Reasons":
		return http.StatusUnavailableForLegalReasons
	case "Internal Server Error":
		return http.StatusInternalServerError
	case "Not Implemented":
		return http.StatusNotImplemented
	case "Bad Gateway":
		return http.StatusBadGateway
	case "Service Unavailable":
		return http.StatusServiceUnavailable
	case "Gateway Timeout":
		return http.StatusGatewayTimeout
	case "HTTP Version Not Supported":
		return http.StatusHTTPVersionNotSupported
	case "Variant Also Negotiates":
		return http.StatusVariantAlsoNegotiates
	case "Insufficient Storage":
		return http.StatusInsufficientStorage
	case "Loop Detected":
		return http.StatusLoopDetected
	case "Not Extended":
		return http.StatusNotExtended
	case "Network Authentication Required":
		return http.StatusNetworkAuthenticationRequired
	default:
		return -1
	}
}

func fwdHeadersToS3(h http.Header, w http.ResponseWriter) {
	for k, v := range h {
		if !strings.HasPrefix(strings.ToLower(k), "x-amz-fwd-header-") {
			continue
		}
		nk := strings.TrimPrefix(k, "x-amz-fwd-header-")
		nk = strings.TrimPrefix(nk, "X-Amz-Fwd-Header-")
		w.Header()[nk] = v
	}
}

func fwdStatusToAPIError(h http.Header) *APIError {
	if status := h.Get(xhttp.AmzFwdStatus); status != "" && StatusCode(status) > -1 {
		apiErr := &APIError{
			HTTPStatusCode: StatusCode(status),
			Description:    h.Get(xhttp.AmzFwdErrorMessage),
			Code:           h.Get(xhttp.AmzFwdErrorCode),
		}
		if apiErr.HTTPStatusCode == http.StatusOK {
			return nil
		}
		return apiErr
	}
	return nil
}

// GetObjectLamdbaHandler - GET Object with transformed data via lambda functions
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

	// Set all the relevant lambda forward headers if found.
	fwdHeadersToS3(resp.Header, w)

	if apiErr := fwdStatusToAPIError(resp.Header); apiErr != nil {
		writeErrorResponse(ctx, w, *apiErr, r.URL)
		return
	}

	io.Copy(w, resp.Body)
}
