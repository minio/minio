package api

import "context"

func (c *Sys) StepDown() error {
	r := c.c.NewRequest("PUT", "/v1/sys/step-down")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	return err
}
