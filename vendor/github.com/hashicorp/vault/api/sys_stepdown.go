package api

import "context"

func (c *Sys) StepDown() error {
	r := c.c.NewRequest("PUT", "/v1/sys/step-down")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}
