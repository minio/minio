package api

import (
	"context"
	"errors"
)

func (c *Sys) Renew(id string, increment int) (*Secret, error) {
	r := c.c.NewRequest("PUT", "/v1/sys/leases/renew")

	body := map[string]interface{}{
		"increment": increment,
		"lease_id":  id,
	}
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

	return ParseSecret(resp.Body)
}

func (c *Sys) Revoke(id string) error {
	r := c.c.NewRequest("PUT", "/v1/sys/leases/revoke/"+id)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

func (c *Sys) RevokePrefix(id string) error {
	r := c.c.NewRequest("PUT", "/v1/sys/leases/revoke-prefix/"+id)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

func (c *Sys) RevokeForce(id string) error {
	r := c.c.NewRequest("PUT", "/v1/sys/leases/revoke-force/"+id)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

func (c *Sys) RevokeWithOptions(opts *RevokeOptions) error {
	if opts == nil {
		return errors.New("nil options provided")
	}

	// Construct path
	path := "/v1/sys/leases/revoke/"
	switch {
	case opts.Force:
		path = "/v1/sys/leases/revoke-force/"
	case opts.Prefix:
		path = "/v1/sys/leases/revoke-prefix/"
	}
	path += opts.LeaseID

	r := c.c.NewRequest("PUT", path)
	if !opts.Force {
		body := map[string]interface{}{
			"sync": opts.Sync,
		}
		if err := r.SetJSONBody(body); err != nil {
			return err
		}
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

type RevokeOptions struct {
	LeaseID string
	Force   bool
	Prefix  bool
	Sync    bool
}
