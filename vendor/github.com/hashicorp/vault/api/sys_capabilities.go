package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"
)

func (c *Sys) CapabilitiesSelf(path string) ([]string, error) {
	return c.Capabilities(c.c.Token(), path)
}

func (c *Sys) Capabilities(token, path string) ([]string, error) {
	body := map[string]string{
		"token": token,
		"path":  path,
	}

	reqPath := "/v1/sys/capabilities"
	if token == c.c.Token() {
		reqPath = fmt.Sprintf("%s-self", reqPath)
	}

	r := c.c.NewRequest("POST", reqPath)
	if err := r.SetJSONBody(body); err != nil {
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

	var res []string
	err = mapstructure.Decode(secret.Data[path], &res)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		_, ok := secret.Data["capabilities"]
		if ok {
			err = mapstructure.Decode(secret.Data["capabilities"], &res)
			if err != nil {
				return nil, err
			}
		}
	}

	return res, nil
}
