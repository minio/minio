package api

import (
	"context"
	"fmt"

	"github.com/mitchellh/mapstructure"
)

func (c *Sys) ListMounts() (map[string]*MountOutput, error) {
	r := c.c.NewRequest("GET", "/v1/sys/mounts")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	err = resp.DecodeJSON(&result)
	if err != nil {
		return nil, err
	}

	mounts := map[string]*MountOutput{}
	for k, v := range result {
		switch v.(type) {
		case map[string]interface{}:
		default:
			continue
		}
		var res MountOutput
		err = mapstructure.Decode(v, &res)
		if err != nil {
			return nil, err
		}
		// Not a mount, some other api.Secret data
		if res.Type == "" {
			continue
		}
		mounts[k] = &res
	}

	return mounts, nil
}

func (c *Sys) Mount(path string, mountInfo *MountInput) error {
	r := c.c.NewRequest("POST", fmt.Sprintf("/v1/sys/mounts/%s", path))
	if err := r.SetJSONBody(mountInfo); err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *Sys) Unmount(path string) error {
	r := c.c.NewRequest("DELETE", fmt.Sprintf("/v1/sys/mounts/%s", path))

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

func (c *Sys) Remount(from, to string) error {
	body := map[string]interface{}{
		"from": from,
		"to":   to,
	}

	r := c.c.NewRequest("POST", "/v1/sys/remount")
	if err := r.SetJSONBody(body); err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

func (c *Sys) TuneMount(path string, config MountConfigInput) error {
	r := c.c.NewRequest("POST", fmt.Sprintf("/v1/sys/mounts/%s/tune", path))
	if err := r.SetJSONBody(config); err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

func (c *Sys) MountConfig(path string) (*MountConfigOutput, error) {
	r := c.c.NewRequest("GET", fmt.Sprintf("/v1/sys/mounts/%s/tune", path))

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result MountConfigOutput
	err = resp.DecodeJSON(&result)
	if err != nil {
		return nil, err
	}

	return &result, err
}

type MountInput struct {
	Type        string            `json:"type"`
	Description string            `json:"description"`
	Config      MountConfigInput  `json:"config"`
	Options     map[string]string `json:"options"`
	Local       bool              `json:"local"`
	PluginName  string            `json:"plugin_name,omitempty"`
	SealWrap    bool              `json:"seal_wrap" mapstructure:"seal_wrap"`
}

type MountConfigInput struct {
	Options                   map[string]string `json:"options" mapstructure:"options"`
	DefaultLeaseTTL           string            `json:"default_lease_ttl" mapstructure:"default_lease_ttl"`
	Description               *string           `json:"description,omitempty" mapstructure:"description"`
	MaxLeaseTTL               string            `json:"max_lease_ttl" mapstructure:"max_lease_ttl"`
	ForceNoCache              bool              `json:"force_no_cache" mapstructure:"force_no_cache"`
	PluginName                string            `json:"plugin_name,omitempty" mapstructure:"plugin_name"`
	AuditNonHMACRequestKeys   []string          `json:"audit_non_hmac_request_keys,omitempty" mapstructure:"audit_non_hmac_request_keys"`
	AuditNonHMACResponseKeys  []string          `json:"audit_non_hmac_response_keys,omitempty" mapstructure:"audit_non_hmac_response_keys"`
	ListingVisibility         string            `json:"listing_visibility,omitempty" mapstructure:"listing_visibility"`
	PassthroughRequestHeaders []string          `json:"passthrough_request_headers,omitempty" mapstructure:"passthrough_request_headers"`
}

type MountOutput struct {
	Type        string            `json:"type"`
	Description string            `json:"description"`
	Accessor    string            `json:"accessor"`
	Config      MountConfigOutput `json:"config"`
	Options     map[string]string `json:"options"`
	Local       bool              `json:"local"`
	SealWrap    bool              `json:"seal_wrap" mapstructure:"seal_wrap"`
}

type MountConfigOutput struct {
	DefaultLeaseTTL           int      `json:"default_lease_ttl" mapstructure:"default_lease_ttl"`
	MaxLeaseTTL               int      `json:"max_lease_ttl" mapstructure:"max_lease_ttl"`
	ForceNoCache              bool     `json:"force_no_cache" mapstructure:"force_no_cache"`
	PluginName                string   `json:"plugin_name,omitempty" mapstructure:"plugin_name"`
	AuditNonHMACRequestKeys   []string `json:"audit_non_hmac_request_keys,omitempty" mapstructure:"audit_non_hmac_request_keys"`
	AuditNonHMACResponseKeys  []string `json:"audit_non_hmac_response_keys,omitempty" mapstructure:"audit_non_hmac_response_keys"`
	ListingVisibility         string   `json:"listing_visibility,omitempty" mapstructure:"listing_visibility"`
	PassthroughRequestHeaders []string `json:"passthrough_request_headers,omitempty" mapstructure:"passthrough_request_headers"`
}
