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
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/minio/pkg/v3/policy"
)

// Authorization Plugin config and env variables
const (
	URL         = "url"
	AuthToken   = "auth_token"
	EnableHTTP2 = "enable_http2"

	EnvPolicyPluginURL         = "MINIO_POLICY_PLUGIN_URL"
	EnvPolicyPluginAuthToken   = "MINIO_POLICY_PLUGIN_AUTH_TOKEN"
	EnvPolicyPluginEnableHTTP2 = "MINIO_POLICY_PLUGIN_ENABLE_HTTP2"
)

// DefaultKVS - default config for Authz plugin config
var (
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
			Key:   EnableHTTP2,
			Value: "off",
		},
	}
)

// Args for general purpose policy engine configuration.
type Args struct {
	URL         *xnet.URL             `json:"url"`
	AuthToken   string                `json:"authToken"`
	Transport   http.RoundTripper     `json:"-"`
	CloseRespFn func(r io.ReadCloser) `json:"-"`
}

// Validate - validate opa configuration params.
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

// UnmarshalJSON - decodes JSON data.
func (a *Args) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subArgs Args
	var so subArgs

	if err := json.Unmarshal(data, &so); err != nil {
		return err
	}

	oa := Args(so)
	if oa.URL == nil || oa.URL.String() == "" {
		*a = oa
		return nil
	}

	*a = oa
	return nil
}

// AuthZPlugin - implements opa policy agent calls.
type AuthZPlugin struct {
	args   Args
	client *http.Client
}

// Enabled returns if AuthZPlugin is enabled.
func Enabled(kvs config.KVS) bool {
	return kvs.Get(URL) != ""
}

// LookupConfig lookup AuthZPlugin from config, override with any ENVs.
func LookupConfig(s config.Config, httpSettings xhttp.ConnSettings, closeRespFn func(io.ReadCloser)) (Args, error) {
	args := Args{}

	if err := s.CheckValidKeys(config.PolicyPluginSubSys, nil); err != nil {
		return args, err
	}

	getCfg := func(cfgParam string) string {
		// As parameters are already validated, we skip checking
		// if the config param was found.
		val, _, _ := s.ResolveConfigParam(config.PolicyPluginSubSys, config.Default, cfgParam, false)
		return val
	}

	pluginURL := getCfg(URL)
	if pluginURL == "" {
		return args, nil
	}

	u, err := xnet.ParseHTTPURL(pluginURL)
	if err != nil {
		return args, err
	}

	enableHTTP2 := false
	if v := getCfg(EnableHTTP2); v != "" {
		enableHTTP2, err = config.ParseBool(v)
		if err != nil {
			return args, err
		}
	}
	httpSettings.EnableHTTP2 = enableHTTP2
	transport := httpSettings.NewHTTPTransportWithTimeout(time.Minute)

	args = Args{
		URL:         u,
		AuthToken:   getCfg(AuthToken),
		Transport:   transport,
		CloseRespFn: closeRespFn,
	}
	if err = args.Validate(); err != nil {
		return args, err
	}
	return args, nil
}

// New - initializes Authorization Management Plugin.
func New(args Args) *AuthZPlugin {
	if args.URL == nil || args.URL.Scheme == "" && args.AuthToken == "" {
		return nil
	}
	return &AuthZPlugin{
		args:   args,
		client: &http.Client{Transport: args.Transport},
	}
}

// IsAllowed - checks given policy args is allowed to continue the REST API.
func (o *AuthZPlugin) IsAllowed(args policy.Args) (bool, error) {
	if o == nil {
		return false, nil
	}

	// Access Management Plugin Input
	body := make(map[string]any)
	body["input"] = args

	inputBytes, err := json.Marshal(body)
	if err != nil {
		return false, err
	}

	req, err := http.NewRequest(http.MethodPost, o.args.URL.String(), bytes.NewReader(inputBytes))
	if err != nil {
		return false, err
	}

	req.Header.Set("Content-Type", "application/json")
	if o.args.AuthToken != "" {
		req.Header.Set("Authorization", o.args.AuthToken)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return false, err
	}
	defer o.args.CloseRespFn(resp.Body)

	// Read the body to be saved later.
	opaRespBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	// Handle large OPA responses when OPA URL is of
	// form http://localhost:8181/v1/data/httpapi/authz
	type opaResultAllow struct {
		Result struct {
			Allow bool `json:"allow"`
		} `json:"result"`
	}

	// Handle simpler OPA responses when OPA URL is of
	// form http://localhost:8181/v1/data/httpapi/authz/allow
	type opaResult struct {
		Result bool `json:"result"`
	}

	respBody := bytes.NewReader(opaRespBytes)

	var result opaResult
	if err = json.NewDecoder(respBody).Decode(&result); err != nil {
		respBody.Seek(0, 0)
		var resultAllow opaResultAllow
		if err = json.NewDecoder(respBody).Decode(&resultAllow); err != nil {
			return false, err
		}
		return resultAllow.Result.Allow, nil
	}

	return result.Result, nil
}
