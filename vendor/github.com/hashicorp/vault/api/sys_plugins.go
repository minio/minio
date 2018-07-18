package api

import (
	"context"
	"fmt"
	"net/http"
)

// ListPluginsInput is used as input to the ListPlugins function.
type ListPluginsInput struct{}

// ListPluginsResponse is the response from the ListPlugins call.
type ListPluginsResponse struct {
	// Names is the list of names of the plugins.
	Names []string
}

// ListPlugins lists all plugins in the catalog and returns their names as a
// list of strings.
func (c *Sys) ListPlugins(i *ListPluginsInput) (*ListPluginsResponse, error) {
	path := "/v1/sys/plugins/catalog"
	req := c.c.NewRequest("LIST", path)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Data struct {
			Keys []string `json:"keys"`
		} `json:"data"`
	}
	if err := resp.DecodeJSON(&result); err != nil {
		return nil, err
	}

	return &ListPluginsResponse{Names: result.Data.Keys}, nil
}

// GetPluginInput is used as input to the GetPlugin function.
type GetPluginInput struct {
	Name string `json:"-"`
}

// GetPluginResponse is the response from the GetPlugin call.
type GetPluginResponse struct {
	Args    []string `json:"args"`
	Builtin bool     `json:"builtin"`
	Command string   `json:"command"`
	Name    string   `json:"name"`
	SHA256  string   `json:"sha256"`
}

func (c *Sys) GetPlugin(i *GetPluginInput) (*GetPluginResponse, error) {
	path := fmt.Sprintf("/v1/sys/plugins/catalog/%s", i.Name)
	req := c.c.NewRequest(http.MethodGet, path)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Data GetPluginResponse
	}
	err = resp.DecodeJSON(&result)
	if err != nil {
		return nil, err
	}
	return &result.Data, err
}

// RegisterPluginInput is used as input to the RegisterPlugin function.
type RegisterPluginInput struct {
	// Name is the name of the plugin. Required.
	Name string `json:"-"`

	// Args is the list of args to spawn the process with.
	Args []string `json:"args,omitempty"`

	// Command is the command to run.
	Command string `json:"command,omitempty"`

	// SHA256 is the shasum of the plugin.
	SHA256 string `json:"sha256,omitempty"`
}

// RegisterPlugin registers the plugin with the given information.
func (c *Sys) RegisterPlugin(i *RegisterPluginInput) error {
	path := fmt.Sprintf("/v1/sys/plugins/catalog/%s", i.Name)
	req := c.c.NewRequest(http.MethodPut, path)
	if err := req.SetJSONBody(i); err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, req)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}

// DeregisterPluginInput is used as input to the DeregisterPlugin function.
type DeregisterPluginInput struct {
	// Name is the name of the plugin. Required.
	Name string `json:"-"`
}

// DeregisterPlugin removes the plugin with the given name from the plugin
// catalog.
func (c *Sys) DeregisterPlugin(i *DeregisterPluginInput) error {
	path := fmt.Sprintf("/v1/sys/plugins/catalog/%s", i.Name)
	req := c.c.NewRequest(http.MethodDelete, path)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	resp, err := c.c.RawRequestWithContext(ctx, req)
	if err == nil {
		defer resp.Body.Close()
	}
	return err
}
