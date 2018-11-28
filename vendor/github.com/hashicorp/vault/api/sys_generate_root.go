package api

import "context"

func (c *Sys) GenerateRootStatus() (*GenerateRootStatusResponse, error) {
	return c.generateRootStatusCommon("/v1/sys/generate-root/attempt")
}

func (c *Sys) GenerateDROperationTokenStatus() (*GenerateRootStatusResponse, error) {
	return c.generateRootStatusCommon("/v1/sys/replication/dr/secondary/generate-operation-token/attempt")
}

func (c *Sys) generateRootStatusCommon(path string) (*GenerateRootStatusResponse, error) {
	r := c.c.NewRequest("GET", path)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result GenerateRootStatusResponse
	err = resp.DecodeJSON(&result)
	return &result, err
}

func (c *Sys) GenerateRootInit(otp, pgpKey string) (*GenerateRootStatusResponse, error) {
	return c.generateRootInitCommon("/v1/sys/generate-root/attempt", otp, pgpKey)
}

func (c *Sys) GenerateDROperationTokenInit(otp, pgpKey string) (*GenerateRootStatusResponse, error) {
	return c.generateRootInitCommon("/v1/sys/replication/dr/secondary/generate-operation-token/attempt", otp, pgpKey)
}

func (c *Sys) generateRootInitCommon(path, otp, pgpKey string) (*GenerateRootStatusResponse, error) {
	body := map[string]interface{}{
		"otp":     otp,
		"pgp_key": pgpKey,
	}

	r := c.c.NewRequest("PUT", path)
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

	var result GenerateRootStatusResponse
	err = resp.DecodeJSON(&result)
	return &result, err
}

func (c *Sys) GenerateRootCancel() error {
	return c.generateRootCancelCommon("/v1/sys/generate-root/attempt")
}

func (c *Sys) GenerateDROperationTokenCancel() error {
	return c.generateRootCancelCommon("/v1/sys/replication/dr/secondary/generate-operation-token/attempt")
}

func (c *Sys) generateRootCancelCommon(path string) error {
	r := c.c.NewRequest("DELETE", path)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, r)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

func (c *Sys) GenerateRootUpdate(shard, nonce string) (*GenerateRootStatusResponse, error) {
	return c.generateRootUpdateCommon("/v1/sys/generate-root/update", shard, nonce)
}

func (c *Sys) GenerateDROperationTokenUpdate(shard, nonce string) (*GenerateRootStatusResponse, error) {
	return c.generateRootUpdateCommon("/v1/sys/replication/dr/secondary/generate-operation-token/update", shard, nonce)
}

func (c *Sys) generateRootUpdateCommon(path, shard, nonce string) (*GenerateRootStatusResponse, error) {
	body := map[string]interface{}{
		"key":   shard,
		"nonce": nonce,
	}

	r := c.c.NewRequest("PUT", path)
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

	var result GenerateRootStatusResponse
	err = resp.DecodeJSON(&result)
	return &result, err
}

type GenerateRootStatusResponse struct {
	Nonce            string `json:"nonce"`
	Started          bool   `json:"started"`
	Progress         int    `json:"progress"`
	Required         int    `json:"required"`
	Complete         bool   `json:"complete"`
	EncodedToken     string `json:"encoded_token"`
	EncodedRootToken string `json:"encoded_root_token"`
	PGPFingerprint   string `json:"pgp_fingerprint"`
	OTP              string `json:"otp"`
	OTPLength        int    `json:"otp_length"`
}
