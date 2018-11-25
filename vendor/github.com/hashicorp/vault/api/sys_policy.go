package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"
)

func (c *Sys) ListPolicies() ([]string, error) {
	r := c.c.NewRequest("LIST", "/v1/sys/policies/acl")

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

	var result []string
	err = mapstructure.Decode(secret.Data["keys"], &result)
	if err != nil {
		return nil, err
	}

	return result, err
}

func (c *Sys) GetPolicy(name string) (string, error) {
	r := c.c.NewRequest("GET", fmt.Sprintf("/v1/sys/policies/acl/%s", name))

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if resp != nil {
		defer resp.Body.Close()
		if resp.StatusCode == 404 {
			return "", nil
		}
	}
	if err != nil {
		return "", err
	}

	secret, err := ParseSecret(resp.Body)
	if err != nil {
		return "", err
	}
	if secret == nil || secret.Data == nil {
		return "", errors.New("data from server response is empty")
	}

	if policyRaw, ok := secret.Data["policy"]; ok {
		return policyRaw.(string), nil
	}

	return "", fmt.Errorf("no policy found in response")
}

func (c *Sys) PutPolicy(name, rules string) error {
	body := map[string]string{
		"policy": rules,
	}

	r := c.c.NewRequest("PUT", fmt.Sprintf("/v1/sys/policies/acl/%s", name))
	if err := r.SetJSONBody(body); err != nil {
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

func (c *Sys) DeletePolicy(name string) error {
	r := c.c.NewRequest("DELETE", fmt.Sprintf("/v1/sys/policies/acl/%s", name))

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

type getPoliciesResp struct {
	Rules string `json:"rules"`
}

type listPoliciesResp struct {
	Policies []string `json:"policies"`
}
