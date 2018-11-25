package api

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

func (c *Sys) Rotate() error {
	r := c.c.NewRequest("POST", "/v1/sys/rotate")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

func (c *Sys) KeyStatus() (*KeyStatus, error) {
	r := c.c.NewRequest("GET", "/v1/sys/key-status")

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

	var result KeyStatus

	termRaw, ok := secret.Data["term"]
	if !ok {
		return nil, errors.New("term not found in response")
	}
	term, ok := termRaw.(json.Number)
	if !ok {
		return nil, errors.New("could not convert term to a number")
	}
	term64, err := term.Int64()
	if err != nil {
		return nil, err
	}
	result.Term = int(term64)

	installTimeRaw, ok := secret.Data["install_time"]
	if !ok {
		return nil, errors.New("install_time not found in response")
	}
	installTimeStr, ok := installTimeRaw.(string)
	if !ok {
		return nil, errors.New("could not convert install_time to a string")
	}
	installTime, err := time.Parse(time.RFC3339Nano, installTimeStr)
	if err != nil {
		return nil, err
	}
	result.InstallTime = installTime

	return &result, err
}

type KeyStatus struct {
	Term        int       `json:"term"`
	InstallTime time.Time `json:"install_time"`
}
