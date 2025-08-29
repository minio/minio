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

package plugin

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/minio/minio/internal/arn"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/env"
	xnet "github.com/minio/pkg/v3/net"
)

func authNLogIf(ctx context.Context, err error) {
	logger.LogIf(ctx, "authN", err)
}

// Authentication Plugin config and env variables
const (
	URL        = "url"
	AuthToken  = "auth_token"
	RolePolicy = "role_policy"
	RoleID     = "role_id"

	EnvIdentityPluginURL        = "MINIO_IDENTITY_PLUGIN_URL"
	EnvIdentityPluginAuthToken  = "MINIO_IDENTITY_PLUGIN_AUTH_TOKEN"
	EnvIdentityPluginRolePolicy = "MINIO_IDENTITY_PLUGIN_ROLE_POLICY"
	EnvIdentityPluginRoleID     = "MINIO_IDENTITY_PLUGIN_ROLE_ID"
)

var (
	// DefaultKVS - default config for AuthN plugin config
	DefaultKVS = config.KVS{
		config.KV{
			Key:   URL,
			Value: "",
		},
		config.KV{
			Key:   AuthToken,
			Value: "",
		},
		config.KV{
			Key:   RolePolicy,
			Value: "",
		},
		config.KV{
			Key:   RoleID,
			Value: "",
		},
	}

	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help for Identity Plugin
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         URL,
			Description: `plugin hook endpoint (HTTP(S)) e.g. "http://localhost:8181/path/to/endpoint"` + defaultHelpPostfix(URL),
			Type:        "url",
		},
		config.HelpKV{
			Key:         AuthToken,
			Description: "authorization token for plugin hook endpoint" + defaultHelpPostfix(AuthToken),
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         RolePolicy,
			Description: "policies to apply for plugin authorized users" + defaultHelpPostfix(RolePolicy),
			Type:        "string",
		},
		config.HelpKV{
			Key:         RoleID,
			Description: "unique ID to generate the ARN" + defaultHelpPostfix(RoleID),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)

// Allows only Base64 URL encoding characters.
var validRoleIDRegex = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Args for authentication plugin.
type Args struct {
	URL         *xnet.URL
	AuthToken   string
	Transport   http.RoundTripper
	CloseRespFn func(r io.ReadCloser)

	RolePolicy string
	RoleARN    arn.ARN
}

// Validate - validate configuration params.
func (a *Args) Validate() error {
	req, err := http.NewRequest(http.MethodPost, a.URL.String(), bytes.NewReader([]byte("")))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if a.AuthToken != "" {
		req.Header.Set("Authorization", a.AuthToken)
	}

	client := &http.Client{Transport: a.Transport}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer a.CloseRespFn(resp.Body)

	return nil
}

type serviceRTTMinuteStats struct {
	statsTime           time.Time
	rttMsSum, maxRttMs  float64
	successRequestCount int64
	failedRequestCount  int64
}

type metrics struct {
	sync.Mutex
	LastCheckSuccess time.Time
	LastCheckFailure time.Time
	lastFullMinute   serviceRTTMinuteStats
	currentMinute    serviceRTTMinuteStats
}

func (h *metrics) setConnSuccess(reqStartTime time.Time) {
	h.Lock()
	defer h.Unlock()
	h.LastCheckSuccess = reqStartTime
}

func (h *metrics) setConnFailure(reqStartTime time.Time) {
	h.Lock()
	defer h.Unlock()
	h.LastCheckFailure = reqStartTime
}

func (h *metrics) updateLastFullMinute(currReqMinute time.Time) {
	// Assumes the caller has h.Lock()'ed
	h.lastFullMinute = h.currentMinute
	h.currentMinute = serviceRTTMinuteStats{
		statsTime: currReqMinute,
	}
}

func (h *metrics) accumRequestRTT(reqStartTime time.Time, rttMs float64, isSuccess bool) {
	h.Lock()
	defer h.Unlock()

	// Update connectivity times
	if isSuccess {
		if reqStartTime.After(h.LastCheckSuccess) {
			h.LastCheckSuccess = reqStartTime
		}
	} else {
		if reqStartTime.After(h.LastCheckFailure) {
			h.LastCheckFailure = reqStartTime
		}
	}

	// Round the request time *down* to whole minute.
	reqTimeMinute := reqStartTime.Truncate(time.Minute)
	if reqTimeMinute.After(h.currentMinute.statsTime) {
		// Drop the last full minute now, since we got a request for a time we
		// are not yet tracking.
		h.updateLastFullMinute(reqTimeMinute)
	}
	var entry *serviceRTTMinuteStats
	switch {
	case reqTimeMinute.Equal(h.currentMinute.statsTime):
		entry = &h.currentMinute
	case reqTimeMinute.Equal(h.lastFullMinute.statsTime):
		entry = &h.lastFullMinute
	default:
		// This request is too old, it should never happen, ignore it as we
		// cannot return an error.
		return
	}

	// Update stats
	if isSuccess {
		if entry.maxRttMs < rttMs {
			entry.maxRttMs = rttMs
		}
		entry.rttMsSum += rttMs
		entry.successRequestCount++
	} else {
		entry.failedRequestCount++
	}
}

// AuthNPlugin - implements pluggable authentication via webhook.
type AuthNPlugin struct {
	args           Args
	client         *http.Client
	shutdownCtx    context.Context
	serviceMetrics *metrics
}

// Enabled returns if AuthNPlugin is enabled.
func Enabled(kvs config.KVS) bool {
	return kvs.Get(URL) != ""
}

// LookupConfig lookup AuthNPlugin from config, override with any ENVs.
func LookupConfig(kv config.KVS, transport *http.Transport, closeRespFn func(io.ReadCloser), serverRegion string) (Args, error) {
	args := Args{}

	if err := config.CheckValidKeys(config.IdentityPluginSubSys, kv, DefaultKVS); err != nil {
		return args, err
	}

	pluginURL := env.Get(EnvIdentityPluginURL, kv.Get(URL))
	if pluginURL == "" {
		return args, nil
	}

	authToken := env.Get(EnvIdentityPluginAuthToken, kv.Get(AuthToken))

	u, err := xnet.ParseHTTPURL(pluginURL)
	if err != nil {
		return args, err
	}

	rolePolicy := env.Get(EnvIdentityPluginRolePolicy, kv.Get(RolePolicy))
	if rolePolicy == "" {
		return args, config.Errorf("A role policy must be specified for Identity Management Plugin")
	}

	resourceID := "idmp-"
	roleID := env.Get(EnvIdentityPluginRoleID, kv.Get(RoleID))
	if roleID == "" {
		// We use a hash of the plugin URL so that the ARN remains
		// constant across restarts.
		h := sha1.New()
		h.Write([]byte(pluginURL))
		bs := h.Sum(nil)
		resourceID += base64.RawURLEncoding.EncodeToString(bs)
	} else {
		// Check that the roleID is restricted to URL safe characters
		// (base64 URL encoding chars).
		if !validRoleIDRegex.MatchString(roleID) {
			return args, config.Errorf("Role ID must match the regexp `^[a-zA-Z0-9_-]+$`")
		}

		// Use the user provided ID here.
		resourceID += roleID
	}

	roleArn, err := arn.NewIAMRoleARN(resourceID, serverRegion)
	if err != nil {
		return args, config.Errorf("unable to generate ARN from the plugin config: %v", err)
	}

	args = Args{
		URL:         u,
		AuthToken:   authToken,
		Transport:   transport,
		CloseRespFn: closeRespFn,
		RolePolicy:  rolePolicy,
		RoleARN:     roleArn,
	}
	if err = args.Validate(); err != nil {
		return args, err
	}
	return args, nil
}

// New - initializes Authorization Management Plugin.
func New(shutdownCtx context.Context, args Args) *AuthNPlugin {
	if args.URL == nil || args.URL.Scheme == "" && args.AuthToken == "" {
		return nil
	}
	plugin := AuthNPlugin{
		args:        args,
		client:      &http.Client{Transport: args.Transport},
		shutdownCtx: shutdownCtx,
		serviceMetrics: &metrics{
			Mutex:            sync.Mutex{},
			LastCheckSuccess: time.Unix(0, 0),
			LastCheckFailure: time.Unix(0, 0),
			lastFullMinute:   serviceRTTMinuteStats{},
			currentMinute:    serviceRTTMinuteStats{},
		},
	}
	go plugin.doPeriodicHealthCheck()
	return &plugin
}

// AuthNSuccessResponse - represents the response from the authentication plugin
// service.
type AuthNSuccessResponse struct {
	User               string         `json:"user"`
	MaxValiditySeconds int            `json:"maxValiditySeconds"`
	Claims             map[string]any `json:"claims"`
}

// AuthNErrorResponse - represents an error response from the authN plugin.
type AuthNErrorResponse struct {
	Reason string `json:"reason"`
}

// AuthNResponse - represents a result of the authentication operation.
type AuthNResponse struct {
	Success *AuthNSuccessResponse
	Failure *AuthNErrorResponse
}

const (
	minValidityDurationSeconds int = 900
	maxValidityDurationSeconds int = 365 * 24 * 3600
)

// Authenticate authenticates the token with the external hook endpoint and
// returns a parent user, max expiry duration for the authentication and a set
// of claims.
func (o *AuthNPlugin) Authenticate(roleArn arn.ARN, token string) (AuthNResponse, error) {
	if o == nil {
		return AuthNResponse{}, nil
	}

	if roleArn != o.args.RoleARN {
		return AuthNResponse{}, fmt.Errorf("Invalid role ARN value: %s", roleArn.String())
	}

	u := url.URL(*o.args.URL)
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return AuthNResponse{}, err
	}

	if o.args.AuthToken != "" {
		req.Header.Set("Authorization", o.args.AuthToken)
	}

	reqStartTime := time.Now()
	resp, err := o.client.Do(req)
	if err != nil {
		o.serviceMetrics.accumRequestRTT(reqStartTime, 0, false)
		return AuthNResponse{}, err
	}
	defer o.args.CloseRespFn(resp.Body)
	reqDurNanos := time.Since(reqStartTime).Nanoseconds()
	o.serviceMetrics.accumRequestRTT(reqStartTime, float64(reqDurNanos)/1e6, true)

	switch resp.StatusCode {
	case 200:
		var result AuthNSuccessResponse
		if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return AuthNResponse{}, err
		}

		if result.MaxValiditySeconds < minValidityDurationSeconds || result.MaxValiditySeconds > maxValidityDurationSeconds {
			return AuthNResponse{}, fmt.Errorf("Plugin returned an invalid validity duration (%d) - should be between %d and %d",
				result.MaxValiditySeconds, minValidityDurationSeconds, maxValidityDurationSeconds)
		}

		return AuthNResponse{
			Success: &result,
		}, nil

	case 403:
		var result AuthNErrorResponse
		if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return AuthNResponse{}, err
		}
		return AuthNResponse{
			Failure: &result,
		}, nil

	default:
		return AuthNResponse{}, fmt.Errorf("Invalid status code %d from auth plugin", resp.StatusCode)
	}
}

// GetRoleInfo - returns ARN to policies map.
func (o *AuthNPlugin) GetRoleInfo() map[arn.ARN]string {
	return map[arn.ARN]string{
		o.args.RoleARN: o.args.RolePolicy,
	}
}

// checkConnectivity returns true if we are able to connect to the plugin
// service.
func (o *AuthNPlugin) checkConnectivity(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()
	u := url.URL(*o.args.URL)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		authNLogIf(ctx, err)
		return false
	}

	if o.args.AuthToken != "" {
		req.Header.Set("Authorization", o.args.AuthToken)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return false
	}
	defer o.args.CloseRespFn(resp.Body)
	return true
}

var (
	healthCheckInterval = 1 * time.Minute
	healthCheckTimeout  = 5 * time.Second
)

func (o *AuthNPlugin) doPeriodicHealthCheck() {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			isConnected := o.checkConnectivity(o.shutdownCtx)
			if isConnected {
				o.serviceMetrics.setConnSuccess(now)
			} else {
				o.serviceMetrics.setConnFailure(now)
			}
		case <-o.shutdownCtx.Done():
			return
		}
	}
}

// Metrics contains metrics about the authentication plugin service.
type Metrics struct {
	LastReachableSecs, LastUnreachableSecs float64

	// Last whole minute stats
	TotalRequests, FailedRequests int64
	AvgSuccRTTMs                  float64
	MaxSuccRTTMs                  float64
}

// Metrics reports metrics related to plugin service reachability and stats for the last whole minute
func (o *AuthNPlugin) Metrics() Metrics {
	if o == nil {
		// Return empty metrics when not configured.
		return Metrics{}
	}
	o.serviceMetrics.Lock()
	defer o.serviceMetrics.Unlock()
	l := &o.serviceMetrics.lastFullMinute
	var avg float64
	if l.successRequestCount > 0 {
		avg = l.rttMsSum / float64(l.successRequestCount)
	}
	now := time.Now().UTC()
	return Metrics{
		LastReachableSecs:   now.Sub(o.serviceMetrics.LastCheckSuccess).Seconds(),
		LastUnreachableSecs: now.Sub(o.serviceMetrics.LastCheckFailure).Seconds(),
		TotalRequests:       l.failedRequestCount + l.successRequestCount,
		FailedRequests:      l.failedRequestCount,
		AvgSuccRTTMs:        avg,
		MaxSuccRTTMs:        l.maxRttMs,
	}
}
