package api

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

// Client holds fields to make requests to a Sia API.
type Client struct {
	address  string
	password string
}

// NewClient creates a new api.Client using the provided address and password.
// If password is not the empty string, HTTP basic authentication will be used
// to communicate with the API.
func NewClient(address string, password string) *Client {
	return &Client{
		address:  address,
		password: password,
	}
}

// Get requests the resource at `resource` and decodes it into `obj`, returning an
// error if requesting or decoding the resource fails.  A non-2xx status code
// constitutes a request failure.
func (c *Client) Get(resource string, obj interface{}) error {
	url := "http://" + c.address + resource
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	if c.password != "" {
		req.SetBasicAuth("", c.password)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// res.Body should always be fully read even when discarding its content,
		// such that the underlying connection can be reused.
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()

	if res.StatusCode == http.StatusNotFound {
		return errors.New("API call not recognized: " + resource)
	}

	// Decode the body as an Error and return this error if the status code is
	// not 2xx.
	if res.StatusCode < 200 || res.StatusCode > 299 {
		var apiErr Error
		err = json.NewDecoder(res.Body).Decode(&apiErr)
		if err != nil {
			return err
		}
		return apiErr
	}

	if res.StatusCode != http.StatusNoContent && obj != nil {
		return json.NewDecoder(res.Body).Decode(obj)
	}
	return nil
}

// Post makes a POST request to the resource at `resource`, using `data` as the
// request body.  The response, if provided, will be decoded into `obj`.
func (c *Client) Post(resource string, data string, obj interface{}) error {
	url := "http://" + c.address + resource
	req, err := http.NewRequest("POST", url, strings.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.password != "" {
		req.SetBasicAuth("", c.password)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// res.Body should always be fully read even when discarding its content,
		// such that the underlying connection can be reused.
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()

	if res.StatusCode == http.StatusNotFound {
		return errors.New("API call not recognized: " + resource)
	}

	if res.StatusCode < 200 || res.StatusCode > 299 {
		var apiErr Error
		err = json.NewDecoder(res.Body).Decode(&apiErr)
		if err != nil {
			return err
		}
		return apiErr
	}

	if res.StatusCode != http.StatusNoContent && obj != nil {
		return json.NewDecoder(res.Body).Decode(&obj)
	}
	return nil
}
