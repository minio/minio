package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/hashicorp/errwrap"
	"github.com/joyent/triton-go/authentication"
)

const nilContext = "nil context"

var MissingKeyIdError = errors.New("Default SSH agent authentication requires SDC_KEY_ID")

// Client represents a connection to the Triton Compute or Object Storage APIs.
type Client struct {
	HTTPClient  *http.Client
	Authorizers []authentication.Signer
	TritonURL   url.URL
	MantaURL    url.URL
	AccountName string
	Endpoint    string
}

// New is used to construct a Client in order to make API
// requests to the Triton API.
//
// At least one signer must be provided - example signers include
// authentication.PrivateKeySigner and authentication.SSHAgentSigner.
func New(tritonURL string, mantaURL string, accountName string, signers ...authentication.Signer) (*Client, error) {
	cloudURL, err := url.Parse(tritonURL)
	if err != nil {
		return nil, errwrap.Wrapf("invalid endpoint URL: {{err}}", err)
	}

	storageURL, err := url.Parse(mantaURL)
	if err != nil {
		return nil, errwrap.Wrapf("invalid manta URL: {{err}}", err)
	}

	if accountName == "" {
		return nil, errors.New("account name can not be empty")
	}

	httpClient := &http.Client{
		Transport:     httpTransport(false),
		CheckRedirect: doNotFollowRedirects,
	}

	newClient := &Client{
		HTTPClient:  httpClient,
		Authorizers: signers,
		TritonURL:   *cloudURL,
		MantaURL:    *storageURL,
		AccountName: accountName,
		// TODO(justinwr): Deprecated?
		// Endpoint:    tritonURL,
	}

	var authorizers []authentication.Signer
	for _, key := range signers {
		if key != nil {
			authorizers = append(authorizers, key)
		}
	}

	// Default to constructing an SSHAgentSigner if there are no other signers
	// passed into NewClient and there's an SDC_KEY_ID value available in the
	// user environ.
	if len(authorizers) == 0 {
		keyID := os.Getenv("SDC_KEY_ID")
		if len(keyID) != 0 {
			keySigner, err := authentication.NewSSHAgentSigner(keyID, accountName)
			if err != nil {
				return nil, errwrap.Wrapf("Problem initializing NewSSHAgentSigner: {{err}}", err)
			}
			newClient.Authorizers = append(authorizers, keySigner)
		} else {
			return nil, MissingKeyIdError
		}
	}

	return newClient, nil
}

// InsecureSkipTLSVerify turns off TLS verification for the client connection. This
// allows connection to an endpoint with a certificate which was signed by a non-
// trusted CA, such as self-signed certificates. This can be useful when connecting
// to temporary Triton installations such as Triton Cloud-On-A-Laptop.
func (c *Client) InsecureSkipTLSVerify() {
	if c.HTTPClient == nil {
		return
	}

	c.HTTPClient.Transport = httpTransport(true)
}

func httpTransport(insecureSkipTLSVerify bool) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableKeepAlives:   true,
		MaxIdleConnsPerHost: -1,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: insecureSkipTLSVerify,
		},
	}
}

func doNotFollowRedirects(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// TODO(justinwr): Deprecated?
// func (c *Client) FormatURL(path string) string {
// 	return fmt.Sprintf("%s%s", c.Endpoint, path)
// }

func (c *Client) DecodeError(statusCode int, body io.Reader) error {
	err := &TritonError{
		StatusCode: statusCode,
	}

	errorDecoder := json.NewDecoder(body)
	if err := errorDecoder.Decode(err); err != nil {
		return errwrap.Wrapf("Error decoding error response: {{err}}", err)
	}

	return err
}

// -----------------------------------------------------------------------------

type RequestInput struct {
	Method  string
	Path    string
	Query   *url.Values
	Headers *http.Header
	Body    interface{}
}

func (c *Client) ExecuteRequestURIParams(ctx context.Context, inputs RequestInput) (io.ReadCloser, error) {
	method := inputs.Method
	path := inputs.Path
	body := inputs.Body
	query := inputs.Query

	var requestBody io.ReadSeeker
	if body != nil {
		marshaled, err := json.MarshalIndent(body, "", "    ")
		if err != nil {
			return nil, err
		}
		requestBody = bytes.NewReader(marshaled)
	}

	endpoint := c.TritonURL
	endpoint.Path = path
	if query != nil {
		endpoint.RawQuery = query.Encode()
	}

	req, err := http.NewRequest(method, endpoint.String(), requestBody)
	if err != nil {
		return nil, errwrap.Wrapf("Error constructing HTTP request: {{err}}", err)
	}

	dateHeader := time.Now().UTC().Format(time.RFC1123)
	req.Header.Set("date", dateHeader)

	// NewClient ensures there's always an authorizer (unless this is called
	// outside that constructor).
	authHeader, err := c.Authorizers[0].Sign(dateHeader)
	if err != nil {
		return nil, errwrap.Wrapf("Error signing HTTP request: {{err}}", err)
	}
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Version", "8")
	req.Header.Set("User-Agent", "triton-go Client API")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errwrap.Wrapf("Error executing HTTP request: {{err}}", err)
	}

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return resp.Body, nil
	}

	return nil, c.DecodeError(resp.StatusCode, resp.Body)
}

func (c *Client) ExecuteRequest(ctx context.Context, inputs RequestInput) (io.ReadCloser, error) {
	return c.ExecuteRequestURIParams(ctx, inputs)
}

func (c *Client) ExecuteRequestRaw(ctx context.Context, inputs RequestInput) (*http.Response, error) {
	method := inputs.Method
	path := inputs.Path
	body := inputs.Body

	var requestBody io.ReadSeeker
	if body != nil {
		marshaled, err := json.MarshalIndent(body, "", "    ")
		if err != nil {
			return nil, err
		}
		requestBody = bytes.NewReader(marshaled)
	}

	endpoint := c.TritonURL
	endpoint.Path = path

	req, err := http.NewRequest(method, endpoint.String(), requestBody)
	if err != nil {
		return nil, errwrap.Wrapf("Error constructing HTTP request: {{err}}", err)
	}

	dateHeader := time.Now().UTC().Format(time.RFC1123)
	req.Header.Set("date", dateHeader)

	// NewClient ensures there's always an authorizer (unless this is called
	// outside that constructor).
	authHeader, err := c.Authorizers[0].Sign(dateHeader)
	if err != nil {
		return nil, errwrap.Wrapf("Error signing HTTP request: {{err}}", err)
	}
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Version", "8")
	req.Header.Set("User-Agent", "triton-go c API")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errwrap.Wrapf("Error executing HTTP request: {{err}}", err)
	}

	return resp, nil
}

func (c *Client) ExecuteRequestStorage(ctx context.Context, inputs RequestInput) (io.ReadCloser, http.Header, error) {
	method := inputs.Method
	path := inputs.Path
	query := inputs.Query
	headers := inputs.Headers
	body := inputs.Body

	endpoint := c.MantaURL
	endpoint.Path = path

	var requestBody io.ReadSeeker
	if body != nil {
		marshaled, err := json.MarshalIndent(body, "", "    ")
		if err != nil {
			return nil, nil, err
		}
		requestBody = bytes.NewReader(marshaled)
	}

	req, err := http.NewRequest(method, endpoint.String(), requestBody)
	if err != nil {
		return nil, nil, errwrap.Wrapf("Error constructing HTTP request: {{err}}", err)
	}

	if body != nil && (headers == nil || headers.Get("Content-Type") == "") {
		req.Header.Set("Content-Type", "application/json")
	}
	if headers != nil {
		for key, values := range *headers {
			for _, value := range values {
				req.Header.Set(key, value)
			}
		}
	}

	dateHeader := time.Now().UTC().Format(time.RFC1123)
	req.Header.Set("date", dateHeader)

	authHeader, err := c.Authorizers[0].Sign(dateHeader)
	if err != nil {
		return nil, nil, errwrap.Wrapf("Error signing HTTP request: {{err}}", err)
	}
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", "manta-go client API")

	if query != nil {
		req.URL.RawQuery = query.Encode()
	}

	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, nil, errwrap.Wrapf("Error executing HTTP request: {{err}}", err)
	}

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return resp.Body, resp.Header, nil
	}

	mantaError := &MantaError{
		StatusCode: resp.StatusCode,
	}

	errorDecoder := json.NewDecoder(resp.Body)
	if err := errorDecoder.Decode(mantaError); err != nil {
		return nil, nil, errwrap.Wrapf("Error decoding error response: {{err}}", err)
	}
	return nil, nil, mantaError
}

type RequestNoEncodeInput struct {
	Method  string
	Path    string
	Query   *url.Values
	Headers *http.Header
	Body    io.ReadSeeker
}

func (c *Client) ExecuteRequestNoEncode(ctx context.Context, inputs RequestNoEncodeInput) (io.ReadCloser, http.Header, error) {
	method := inputs.Method
	path := inputs.Path
	query := inputs.Query
	headers := inputs.Headers
	body := inputs.Body

	endpoint := c.MantaURL
	endpoint.Path = path

	req, err := http.NewRequest(method, endpoint.String(), body)
	if err != nil {
		return nil, nil, errwrap.Wrapf("Error constructing HTTP request: {{err}}", err)
	}

	if headers != nil {
		for key, values := range *headers {
			for _, value := range values {
				req.Header.Set(key, value)
			}
		}
	}

	dateHeader := time.Now().UTC().Format(time.RFC1123)
	req.Header.Set("date", dateHeader)

	authHeader, err := c.Authorizers[0].Sign(dateHeader)
	if err != nil {
		return nil, nil, errwrap.Wrapf("Error signing HTTP request: {{err}}", err)
	}
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", "manta-go client API")

	if query != nil {
		req.URL.RawQuery = query.Encode()
	}

	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, nil, errwrap.Wrapf("Error executing HTTP request: {{err}}", err)
	}

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return resp.Body, resp.Header, nil
	}

	mantaError := &MantaError{
		StatusCode: resp.StatusCode,
	}

	errorDecoder := json.NewDecoder(resp.Body)
	if err := errorDecoder.Decode(mantaError); err != nil {
		return nil, nil, errwrap.Wrapf("Error decoding error response: {{err}}", err)
	}
	return nil, nil, mantaError
}
