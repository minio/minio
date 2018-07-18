package api

import (
	"context"
	"fmt"
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

	var result map[string]interface{}
	err = resp.DecodeJSON(&result)
	if err != nil {
		return nil, err
	}

	if result["capabilities"] == nil {
		return nil, nil
	}
	var capabilities []string
	capabilitiesRaw, ok := result["capabilities"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("error interpreting returned capabilities")
	}
	for _, capability := range capabilitiesRaw {
		capabilities = append(capabilities, capability.(string))
	}
	return capabilities, nil
}
