package api

import (
	"context"
	"errors"

	"github.com/mitchellh/mapstructure"
)

func (c *Sys) CORSStatus() (*CORSResponse, error) {
	r := c.c.NewRequest("GET", "/v1/sys/config/cors")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	secret, err := ParseSecret(resp.Body)
	if err != nil {
		return nil, err
	}
	if secret == nil || secret.Data == nil {
		return nil, errors.New("data from server response is empty")
	}

	var result CORSResponse
	err = mapstructure.Decode(secret.Data, &result)
	if err != nil {
		return nil, err
	}

	return &result, err
}

func (c *Sys) ConfigureCORS(req *CORSRequest) (*CORSResponse, error) {
	r := c.c.NewRequest("PUT", "/v1/sys/config/cors")
	if err := r.SetJSONBody(req); err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	secret, err := ParseSecret(resp.Body)
	if err != nil {
		return nil, err
	}
	if secret == nil || secret.Data == nil {
		return nil, errors.New("data from server response is empty")
	}

	var result CORSResponse
	err = mapstructure.Decode(secret.Data, &result)
	if err != nil {
		return nil, err
	}

	return &result, err
}

func (c *Sys) DisableCORS() (*CORSResponse, error) {
	r := c.c.NewRequest("DELETE", "/v1/sys/config/cors")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	secret, err := ParseSecret(resp.Body)
	if err != nil {
		return nil, err
	}
	if secret == nil || secret.Data == nil {
		return nil, errors.New("data from server response is empty")
	}

	var result CORSResponse
	err = mapstructure.Decode(secret.Data, &result)
	if err != nil {
		return nil, err
	}

	return &result, err
}

type CORSRequest struct {
	AllowedOrigins string `json:"allowed_origins" mapstructure:"allowed_origins"`
	Enabled        bool   `json:"enabled" mapstructure:"enabled"`
}

type CORSResponse struct {
	AllowedOrigins string `json:"allowed_origins" mapstructure:"allowed_origins"`
	Enabled        bool   `json:"enabled" mapstructure:"enabled"`
}
