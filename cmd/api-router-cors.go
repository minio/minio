// Copyright (c) 2015-2024 MinIO, Inc.
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
	"errors"
	"net/http"
	"strings"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/mux"
	xcors "github.com/minio/pkg/v3/cors"
	"github.com/minio/pkg/v3/wildcard"
	"golang.org/x/exp/slices"

	rscors "github.com/rs/cors"
)

// There are two ways to apply CORS in MinIO:
// 1. Global CORS settings: This was the original method that applied to the entire API surface,
//    and was limited to just allowed origins.
// 2. Bucket-specific CORS settings: This is a new method that allows for more fine-grained
//    control over CORS settings, and can be applied on a per-bucket basis. It uses AWS's own spec
//    that allows a number of rules to be specified. Compatible with the global settings, takes precedence
//    https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html

// corsHandler returns a middleware that is able to either apply bucket-specific CORS settings if they exist,
// or fallback to the global CORS settings if they don't.
func corsHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		bucket := vars["bucket"]
		if bucket == "" {
			// No bucket, continue the request using global CORS.
			corsGlobalHandler(handler).ServeHTTP(w, r)
			return
		}

		// Check if the bucket has a CORS config set.
		config, _, err := globalBucketMetadataSys.GetCorsConfig(bucket)
		if err != nil {
			if errors.Is(err, BucketCorsNotFound{Bucket: bucket}) {
				// No CORS config set on the bucket, continue with the request using global CORS.
				corsGlobalHandler(handler).ServeHTTP(w, r)
				return
			}

			if r.Method == http.MethodOptions {
				// Don't fall back to global CORS on preflight as that presents a security risk.
				writeErrorResponse(r.Context(), w, toAPIError(r.Context(), err), r.URL)
				return
			}
			// Continue with the non-preflight request
			handler.ServeHTTP(w, r)
			return
		}

		// We have bucket specific CORS settings, use them instead of global CORS handling.
		opts := findCorsRule(r, config)
		if opts == nil {
			if r.Method == http.MethodOptions {
				// Preflight request with no matching rule is an error.
				writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrCORSRequestNotAllowed), r.URL)
				return
			}
			// Non-preflight request with no matching rule is allowed.
			handler.ServeHTTP(w, r)
			return
		}

		// fetch() spec guarantees all values of Access-Control-Request-Headers are lowercase,
		// however Go canonicalizes header values to Title-Case. We need to convert them to lowercase
		// in order to match correctly in rs/cors. Access the map directly, as recommended by http.Header docs.
		if r.Method == http.MethodOptions {
			for i, header := range r.Header["Access-Control-Request-Headers"] {
				r.Header["Access-Control-Request-Headers"][i] = strings.ToLower(header)
			}
		}

		// To match S3 behavior, do not send Allow-Credentials header for certain methods.
		skipCredentialsMethods := []string{http.MethodGet, http.MethodHead}
		methodToCheck := r.Header.Get("Access-Control-Request-Method")
		if methodToCheck == "" {
			methodToCheck = r.Method
		}
		if slices.Contains(skipCredentialsMethods, methodToCheck) {
			opts.AllowCredentials = false
		}

		rscors.New(*opts).ServeHTTP(w, r, handler.ServeHTTP)
		return
	})
}

func findCorsRule(r *http.Request, config *xcors.Config) *rscors.Options {
	// S3 Docs on how matching works: https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html
	acrHeaders := []string{}
	acrHeadersCSV := r.Header.Get("Access-Control-Request-Headers")
	if acrHeadersCSV != "" {
		acrHeaders = strings.FieldsFunc(acrHeadersCSV, func(r rune) bool {
			return r == ',' || r == ' '
		})
	}

	// Find the first matching rule
	var opts *rscors.Options
	for _, rule := range config.CORSRules {
		opts = tryMatch(&rule,
			r.Header.Get("Origin"),
			r.Method,
			r.Header.Get("Access-Control-Request-Method"),
			acrHeaders,
		)
		if opts != nil {
			break
		}
	}
	return opts
}

func tryMatch(rule *xcors.Rule, origin string, method string, acrMethod string, acrHeaders []string) *rscors.Options {
	if !rule.HasAllowedOrigin(origin) {
		return nil
	}

	allowedHeaders, ok := rule.FilterAllowedHeaders(acrHeaders)
	if !ok {
		return nil
	}

	if method == http.MethodOptions {
		// The request is an OPTIONS pre-flight request, the Access-Control-Request-Method
		// header specifies the method to test.
		if !rule.HasAllowedMethod(acrMethod) {
			return nil
		}
		return ruleToCorsOptions(rule, allowedHeaders)
	}

	if acrMethod != "" {
		// Request is not OPTIONS preflight, but Access-Control-Request-Method is set. S3
		// handles this case by returning CORS info.
		if !rule.HasAllowedMethod(acrMethod) {
			return nil
		}
		rule.AllowedMethod = []string{method, acrMethod}
		return ruleToCorsOptions(rule, allowedHeaders)
	}

	// Request is not OPTIONS preflight, just a regular request.
	if !rule.HasAllowedMethod(method) {
		return nil
	}

	return ruleToCorsOptions(rule, allowedHeaders)
}

func ruleToCorsOptions(rule *xcors.Rule, allowedHeaders []string) *rscors.Options {
	return &rscors.Options{
		AllowedOrigins:       rule.AllowedOrigin,
		AllowedMethods:       rule.AllowedMethod,
		AllowedHeaders:       allowedHeaders,
		ExposedHeaders:       rule.ExposeHeader,
		MaxAge:               rule.MaxAgeSeconds,
		AllowCredentials:     true,
		OptionsSuccessStatus: http.StatusOK, // To match S3
	}
}

func initGlobalCors() *rscors.Cors {
	commonS3Headers := []string{
		xhttp.Date,
		xhttp.ETag,
		xhttp.ServerInfo,
		xhttp.Connection,
		xhttp.AcceptRanges,
		xhttp.ContentRange,
		xhttp.ContentEncoding,
		xhttp.ContentLength,
		xhttp.ContentType,
		xhttp.ContentDisposition,
		xhttp.LastModified,
		xhttp.ContentLanguage,
		xhttp.CacheControl,
		xhttp.RetryAfter,
		xhttp.AmzBucketRegion,
		xhttp.Expires,
		"X-Amz*",
		"x-amz*",
		"*",
	}
	opts := rscors.Options{
		AllowOriginFunc: func(origin string) bool {
			for _, allowedOrigin := range globalAPIConfig.getCorsAllowOrigins() {
				if wildcard.MatchSimple(allowedOrigin, origin) {
					return true
				}
			}
			return false
		},
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPut,
			http.MethodHead,
			http.MethodPost,
			http.MethodDelete,
			http.MethodOptions,
			http.MethodPatch,
		},
		AllowedHeaders:   commonS3Headers,
		ExposedHeaders:   commonS3Headers,
		AllowCredentials: true,
	}
	return rscors.New(opts)
}

var corsRSGlobal *rscors.Cors

func init() {
	corsRSGlobal = initGlobalCors()
}

// corsGlobalHandler returns a handler for CORS settings that have been applied globally
// via the "cors_allow_origin" setting. These can be overridden by bucket-specific CORS.
func corsGlobalHandler(handler http.Handler) http.Handler {
	return corsRSGlobal.Handler(handler)
}
