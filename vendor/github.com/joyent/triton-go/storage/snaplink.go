package storage

import (
	"context"
	"fmt"
	"net/http"

	"github.com/hashicorp/errwrap"
	"github.com/joyent/triton-go/client"
)

type SnapLinksClient struct {
	client *client.Client
}

// PutSnapLinkInput represents parameters to a PutSnapLink operation.
type PutSnapLinkInput struct {
	LinkPath   string
	SourcePath string
}

// PutSnapLink creates a SnapLink to an object.
func (s *SnapLinksClient) Put(ctx context.Context, input *PutSnapLinkInput) error {
	linkPath := fmt.Sprintf("/%s%s", s.client.AccountName, input.LinkPath)
	sourcePath := fmt.Sprintf("/%s%s", s.client.AccountName, input.SourcePath)
	headers := &http.Header{}
	headers.Set("Content-Type", "application/json; type=link")
	headers.Set("location", sourcePath)
	headers.Set("Accept", "~1.0")
	headers.Set("Accept-Version", "application/json, */*")

	reqInput := client.RequestInput{
		Method:  http.MethodPut,
		Path:    linkPath,
		Headers: headers,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing PutSnapLink request: {{err}}", err)
	}

	return nil
}
