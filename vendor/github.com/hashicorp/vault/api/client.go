package api

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/hashicorp/go-rootcerts"
	"github.com/hashicorp/vault/helper/consts"
	"github.com/hashicorp/vault/helper/parseutil"
	"golang.org/x/net/http2"
	"golang.org/x/time/rate"
)

const EnvVaultAddress = "VAULT_ADDR"
const EnvVaultCACert = "VAULT_CACERT"
const EnvVaultCAPath = "VAULT_CAPATH"
const EnvVaultClientCert = "VAULT_CLIENT_CERT"
const EnvVaultClientKey = "VAULT_CLIENT_KEY"
const EnvVaultClientTimeout = "VAULT_CLIENT_TIMEOUT"
const EnvVaultInsecure = "VAULT_SKIP_VERIFY"
const EnvVaultTLSServerName = "VAULT_TLS_SERVER_NAME"
const EnvVaultWrapTTL = "VAULT_WRAP_TTL"
const EnvVaultMaxRetries = "VAULT_MAX_RETRIES"
const EnvVaultToken = "VAULT_TOKEN"
const EnvVaultMFA = "VAULT_MFA"
const EnvRateLimit = "VAULT_RATE_LIMIT"

// WrappingLookupFunc is a function that, given an HTTP verb and a path,
// returns an optional string duration to be used for response wrapping (e.g.
// "15s", or simply "15"). The path will not begin with "/v1/" or "v1/" or "/",
// however, end-of-path forward slashes are not trimmed, so must match your
// called path precisely.
type WrappingLookupFunc func(operation, path string) string

// Config is used to configure the creation of the client.
type Config struct {
	modifyLock sync.RWMutex

	// Address is the address of the Vault server. This should be a complete
	// URL such as "http://vault.example.com". If you need a custom SSL
	// cert or want to enable insecure mode, you need to specify a custom
	// HttpClient.
	Address string

	// HttpClient is the HTTP client to use. Vault sets sane defaults for the
	// http.Client and its associated http.Transport created in DefaultConfig.
	// If you must modify Vault's defaults, it is suggested that you start with
	// that client and modify as needed rather than start with an empty client
	// (or http.DefaultClient).
	HttpClient *http.Client

	// MaxRetries controls the maximum number of times to retry when a 5xx
	// error occurs. Set to 0 to disable retrying. Defaults to 2 (for a total
	// of three tries).
	MaxRetries int

	// Timeout is for setting custom timeout parameter in the HttpClient
	Timeout time.Duration

	// If there is an error when creating the configuration, this will be the
	// error
	Error error

	// The Backoff function to use; a default is used if not provided
	Backoff retryablehttp.Backoff

	// Limiter is the rate limiter used by the client.
	// If this pointer is nil, then there will be no limit set.
	// In contrast, if this pointer is set, even to an empty struct,
	// then that limiter will be used. Note that an empty Limiter
	// is equivalent blocking all events.
	Limiter *rate.Limiter
}

// TLSConfig contains the parameters needed to configure TLS on the HTTP client
// used to communicate with Vault.
type TLSConfig struct {
	// CACert is the path to a PEM-encoded CA cert file to use to verify the
	// Vault server SSL certificate.
	CACert string

	// CAPath is the path to a directory of PEM-encoded CA cert files to verify
	// the Vault server SSL certificate.
	CAPath string

	// ClientCert is the path to the certificate for Vault communication
	ClientCert string

	// ClientKey is the path to the private key for Vault communication
	ClientKey string

	// TLSServerName, if set, is used to set the SNI host when connecting via
	// TLS.
	TLSServerName string

	// Insecure enables or disables SSL verification
	Insecure bool
}

// DefaultConfig returns a default configuration for the client. It is
// safe to modify the return value of this function.
//
// The default Address is https://127.0.0.1:8200, but this can be overridden by
// setting the `VAULT_ADDR` environment variable.
//
// If an error is encountered, this will return nil.
func DefaultConfig() *Config {
	config := &Config{
		Address:    "https://127.0.0.1:8200",
		HttpClient: cleanhttp.DefaultPooledClient(),
	}
	config.HttpClient.Timeout = time.Second * 60

	transport := config.HttpClient.Transport.(*http.Transport)
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if err := http2.ConfigureTransport(transport); err != nil {
		config.Error = err
		return config
	}

	if err := config.ReadEnvironment(); err != nil {
		config.Error = err
		return config
	}

	// Ensure redirects are not automatically followed
	// Note that this is sane for the API client as it has its own
	// redirect handling logic (and thus also for command/meta),
	// but in e.g. http_test actual redirect handling is necessary
	config.HttpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		// Returning this value causes the Go net library to not close the
		// response body and to nil out the error. Otherwise retry clients may
		// try three times on every redirect because it sees an error from this
		// function (to prevent redirects) passing through to it.
		return http.ErrUseLastResponse
	}

	config.Backoff = retryablehttp.LinearJitterBackoff
	config.MaxRetries = 2

	return config
}

// ConfigureTLS takes a set of TLS configurations and applies those to the the
// HTTP client.
func (c *Config) ConfigureTLS(t *TLSConfig) error {
	if c.HttpClient == nil {
		c.HttpClient = DefaultConfig().HttpClient
	}
	clientTLSConfig := c.HttpClient.Transport.(*http.Transport).TLSClientConfig

	var clientCert tls.Certificate
	foundClientCert := false

	switch {
	case t.ClientCert != "" && t.ClientKey != "":
		var err error
		clientCert, err = tls.LoadX509KeyPair(t.ClientCert, t.ClientKey)
		if err != nil {
			return err
		}
		foundClientCert = true
	case t.ClientCert != "" || t.ClientKey != "":
		return fmt.Errorf("both client cert and client key must be provided")
	}

	if t.CACert != "" || t.CAPath != "" {
		rootConfig := &rootcerts.Config{
			CAFile: t.CACert,
			CAPath: t.CAPath,
		}
		if err := rootcerts.ConfigureTLS(clientTLSConfig, rootConfig); err != nil {
			return err
		}
	}

	if t.Insecure {
		clientTLSConfig.InsecureSkipVerify = true
	}

	if foundClientCert {
		// We use this function to ignore the server's preferential list of
		// CAs, otherwise any CA used for the cert auth backend must be in the
		// server's CA pool
		clientTLSConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &clientCert, nil
		}
	}

	if t.TLSServerName != "" {
		clientTLSConfig.ServerName = t.TLSServerName
	}

	return nil
}

// ReadEnvironment reads configuration information from the environment. If
// there is an error, no configuration value is updated.
func (c *Config) ReadEnvironment() error {
	var envAddress string
	var envCACert string
	var envCAPath string
	var envClientCert string
	var envClientKey string
	var envClientTimeout time.Duration
	var envInsecure bool
	var envTLSServerName string
	var envMaxRetries *uint64
	var limit *rate.Limiter

	// Parse the environment variables
	if v := os.Getenv(EnvVaultAddress); v != "" {
		envAddress = v
	}
	if v := os.Getenv(EnvVaultMaxRetries); v != "" {
		maxRetries, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return err
		}
		envMaxRetries = &maxRetries
	}
	if v := os.Getenv(EnvVaultCACert); v != "" {
		envCACert = v
	}
	if v := os.Getenv(EnvVaultCAPath); v != "" {
		envCAPath = v
	}
	if v := os.Getenv(EnvVaultClientCert); v != "" {
		envClientCert = v
	}
	if v := os.Getenv(EnvVaultClientKey); v != "" {
		envClientKey = v
	}
	if v := os.Getenv(EnvRateLimit); v != "" {
		rateLimit, burstLimit, err := parseRateLimit(v)
		if err != nil {
			return err
		}
		limit = rate.NewLimiter(rate.Limit(rateLimit), burstLimit)
	}
	if t := os.Getenv(EnvVaultClientTimeout); t != "" {
		clientTimeout, err := parseutil.ParseDurationSecond(t)
		if err != nil {
			return fmt.Errorf("could not parse %q", EnvVaultClientTimeout)
		}
		envClientTimeout = clientTimeout
	}
	if v := os.Getenv(EnvVaultInsecure); v != "" {
		var err error
		envInsecure, err = strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("could not parse VAULT_SKIP_VERIFY")
		}
	}
	if v := os.Getenv(EnvVaultTLSServerName); v != "" {
		envTLSServerName = v
	}

	// Configure the HTTP clients TLS configuration.
	t := &TLSConfig{
		CACert:        envCACert,
		CAPath:        envCAPath,
		ClientCert:    envClientCert,
		ClientKey:     envClientKey,
		TLSServerName: envTLSServerName,
		Insecure:      envInsecure,
	}

	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	c.Limiter = limit

	if err := c.ConfigureTLS(t); err != nil {
		return err
	}

	if envAddress != "" {
		c.Address = envAddress
	}

	if envMaxRetries != nil {
		c.MaxRetries = int(*envMaxRetries)
	}

	if envClientTimeout != 0 {
		c.Timeout = envClientTimeout
	}

	return nil
}

func parseRateLimit(val string) (rate float64, burst int, err error) {

	_, err = fmt.Sscanf(val, "%f:%d", &rate, &burst)
	if err != nil {
		rate, err = strconv.ParseFloat(val, 64)
		if err != nil {
			err = fmt.Errorf("%v was provided but incorrectly formatted", EnvRateLimit)
		}
		burst = int(rate)
	}

	return rate, burst, err

}

// Client is the client to the Vault API. Create a client with NewClient.
type Client struct {
	modifyLock         sync.RWMutex
	addr               *url.URL
	config             *Config
	token              string
	headers            http.Header
	wrappingLookupFunc WrappingLookupFunc
	mfaCreds           []string
	policyOverride     bool
}

// NewClient returns a new client for the given configuration.
//
// If the configuration is nil, Vault will use configuration from
// DefaultConfig(), which is the recommended starting configuration.
//
// If the environment variable `VAULT_TOKEN` is present, the token will be
// automatically added to the client. Otherwise, you must manually call
// `SetToken()`.
func NewClient(c *Config) (*Client, error) {
	def := DefaultConfig()
	if def == nil {
		return nil, fmt.Errorf("could not create/read default configuration")
	}
	if def.Error != nil {
		return nil, errwrap.Wrapf("error encountered setting up default configuration: {{err}}", def.Error)
	}

	if c == nil {
		c = def
	}

	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	u, err := url.Parse(c.Address)
	if err != nil {
		return nil, err
	}

	if c.HttpClient == nil {
		c.HttpClient = def.HttpClient
	}
	if c.HttpClient.Transport == nil {
		c.HttpClient.Transport = def.HttpClient.Transport
	}

	client := &Client{
		addr:   u,
		config: c,
	}

	if token := os.Getenv(EnvVaultToken); token != "" {
		client.token = token
	}

	return client, nil
}

// Sets the address of Vault in the client. The format of address should be
// "<Scheme>://<Host>:<Port>". Setting this on a client will override the
// value of VAULT_ADDR environment variable.
func (c *Client) SetAddress(addr string) error {
	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	parsedAddr, err := url.Parse(addr)
	if err != nil {
		return errwrap.Wrapf("failed to set address: {{err}}", err)
	}

	c.addr = parsedAddr
	return nil
}

// Address returns the Vault URL the client is configured to connect to
func (c *Client) Address() string {
	c.modifyLock.RLock()
	defer c.modifyLock.RUnlock()

	return c.addr.String()
}

// SetLimiter will set the rate limiter for this client.
// This method is thread-safe.
// rateLimit and burst are specified according to https://godoc.org/golang.org/x/time/rate#NewLimiter
func (c *Client) SetLimiter(rateLimit float64, burst int) {
	c.modifyLock.RLock()
	c.config.modifyLock.Lock()
	defer c.config.modifyLock.Unlock()
	c.modifyLock.RUnlock()

	c.config.Limiter = rate.NewLimiter(rate.Limit(rateLimit), burst)
}

// SetMaxRetries sets the number of retries that will be used in the case of certain errors
func (c *Client) SetMaxRetries(retries int) {
	c.modifyLock.RLock()
	c.config.modifyLock.Lock()
	defer c.config.modifyLock.Unlock()
	c.modifyLock.RUnlock()

	c.config.MaxRetries = retries
}

// SetClientTimeout sets the client request timeout
func (c *Client) SetClientTimeout(timeout time.Duration) {
	c.modifyLock.RLock()
	c.config.modifyLock.Lock()
	defer c.config.modifyLock.Unlock()
	c.modifyLock.RUnlock()

	c.config.Timeout = timeout
}

// CurrentWrappingLookupFunc sets a lookup function that returns desired wrap TTLs
// for a given operation and path
func (c *Client) CurrentWrappingLookupFunc() WrappingLookupFunc {
	c.modifyLock.RLock()
	defer c.modifyLock.RUnlock()

	return c.wrappingLookupFunc
}

// SetWrappingLookupFunc sets a lookup function that returns desired wrap TTLs
// for a given operation and path
func (c *Client) SetWrappingLookupFunc(lookupFunc WrappingLookupFunc) {
	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	c.wrappingLookupFunc = lookupFunc
}

// SetMFACreds sets the MFA credentials supplied either via the environment
// variable or via the command line.
func (c *Client) SetMFACreds(creds []string) {
	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	c.mfaCreds = creds
}

// SetNamespace sets the namespace supplied either via the environment
// variable or via the command line.
func (c *Client) SetNamespace(namespace string) {
	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	if c.headers == nil {
		c.headers = make(http.Header)
	}

	c.headers.Set(consts.NamespaceHeaderName, namespace)
}

// Token returns the access token being used by this client. It will
// return the empty string if there is no token set.
func (c *Client) Token() string {
	c.modifyLock.RLock()
	defer c.modifyLock.RUnlock()

	return c.token
}

// SetToken sets the token directly. This won't perform any auth
// verification, it simply sets the token properly for future requests.
func (c *Client) SetToken(v string) {
	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	c.token = v
}

// ClearToken deletes the token if it is set or does nothing otherwise.
func (c *Client) ClearToken() {
	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	c.token = ""
}

// Headers gets the current set of headers used for requests. This returns a
// copy; to modify it make modifications locally and use SetHeaders.
func (c *Client) Headers() http.Header {
	c.modifyLock.RLock()
	defer c.modifyLock.RUnlock()

	if c.headers == nil {
		return nil
	}

	ret := make(http.Header)
	for k, v := range c.headers {
		for _, val := range v {
			ret[k] = append(ret[k], val)
		}
	}

	return ret
}

// SetHeaders sets the headers to be used for future requests.
func (c *Client) SetHeaders(headers http.Header) {
	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	c.headers = headers
}

// SetBackoff sets the backoff function to be used for future requests.
func (c *Client) SetBackoff(backoff retryablehttp.Backoff) {
	c.modifyLock.RLock()
	c.config.modifyLock.Lock()
	defer c.config.modifyLock.Unlock()
	c.modifyLock.RUnlock()

	c.config.Backoff = backoff
}

// Clone creates a new client with the same configuration. Note that the same
// underlying http.Client is used; modifying the client from more than one
// goroutine at once may not be safe, so modify the client as needed and then
// clone.
//
// Also, only the client's config is currently copied; this means items not in
// the api.Config struct, such as policy override and wrapping function
// behavior, must currently then be set as desired on the new client.
func (c *Client) Clone() (*Client, error) {
	c.modifyLock.RLock()
	c.config.modifyLock.RLock()
	config := c.config
	c.modifyLock.RUnlock()

	newConfig := &Config{
		Address:    config.Address,
		HttpClient: config.HttpClient,
		MaxRetries: config.MaxRetries,
		Timeout:    config.Timeout,
		Backoff:    config.Backoff,
		Limiter:    config.Limiter,
	}
	config.modifyLock.RUnlock()

	return NewClient(newConfig)
}

// SetPolicyOverride sets whether requests should be sent with the policy
// override flag to request overriding soft-mandatory Sentinel policies (both
// RGPs and EGPs)
func (c *Client) SetPolicyOverride(override bool) {
	c.modifyLock.Lock()
	defer c.modifyLock.Unlock()

	c.policyOverride = override
}

// NewRequest creates a new raw request object to query the Vault server
// configured for this client. This is an advanced method and generally
// doesn't need to be called externally.
func (c *Client) NewRequest(method, requestPath string) *Request {
	c.modifyLock.RLock()
	addr := c.addr
	token := c.token
	mfaCreds := c.mfaCreds
	wrappingLookupFunc := c.wrappingLookupFunc
	headers := c.headers
	policyOverride := c.policyOverride
	c.modifyLock.RUnlock()

	// if SRV records exist (see https://tools.ietf.org/html/draft-andrews-http-srv-02), lookup the SRV
	// record and take the highest match; this is not designed for high-availability, just discovery
	var host string = addr.Host
	if addr.Port() == "" {
		// Internet Draft specifies that the SRV record is ignored if a port is given
		_, addrs, err := net.LookupSRV("http", "tcp", addr.Hostname())
		if err == nil && len(addrs) > 0 {
			host = fmt.Sprintf("%s:%d", addrs[0].Target, addrs[0].Port)
		}
	}

	req := &Request{
		Method: method,
		URL: &url.URL{
			User:   addr.User,
			Scheme: addr.Scheme,
			Host:   host,
			Path:   path.Join(addr.Path, requestPath),
		},
		ClientToken: token,
		Params:      make(map[string][]string),
	}

	var lookupPath string
	switch {
	case strings.HasPrefix(requestPath, "/v1/"):
		lookupPath = strings.TrimPrefix(requestPath, "/v1/")
	case strings.HasPrefix(requestPath, "v1/"):
		lookupPath = strings.TrimPrefix(requestPath, "v1/")
	default:
		lookupPath = requestPath
	}

	req.MFAHeaderVals = mfaCreds

	if wrappingLookupFunc != nil {
		req.WrapTTL = wrappingLookupFunc(method, lookupPath)
	} else {
		req.WrapTTL = DefaultWrappingLookupFunc(method, lookupPath)
	}

	if headers != nil {
		req.Headers = headers
	}

	req.PolicyOverride = policyOverride

	return req
}

// RawRequest performs the raw request given. This request may be against
// a Vault server not configured with this client. This is an advanced operation
// that generally won't need to be called externally.
func (c *Client) RawRequest(r *Request) (*Response, error) {
	return c.RawRequestWithContext(context.Background(), r)
}

// RawRequestWithContext performs the raw request given. This request may be against
// a Vault server not configured with this client. This is an advanced operation
// that generally won't need to be called externally.
func (c *Client) RawRequestWithContext(ctx context.Context, r *Request) (*Response, error) {
	c.modifyLock.RLock()
	token := c.token

	c.config.modifyLock.RLock()
	limiter := c.config.Limiter
	maxRetries := c.config.MaxRetries
	backoff := c.config.Backoff
	httpClient := c.config.HttpClient
	timeout := c.config.Timeout
	c.config.modifyLock.RUnlock()

	c.modifyLock.RUnlock()

	if limiter != nil {
		limiter.Wait(ctx)
	}

	// Sanity check the token before potentially erroring from the API
	idx := strings.IndexFunc(token, func(c rune) bool {
		return !unicode.IsPrint(c)
	})
	if idx != -1 {
		return nil, fmt.Errorf("configured Vault token contains non-printable characters and cannot be used")
	}

	redirectCount := 0
START:
	req, err := r.toRetryableHTTP()
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, fmt.Errorf("nil request created")
	}

	if timeout != 0 {
		ctx, _ = context.WithTimeout(ctx, timeout)
	}
	req.Request = req.Request.WithContext(ctx)

	if backoff == nil {
		backoff = retryablehttp.LinearJitterBackoff
	}

	client := &retryablehttp.Client{
		HTTPClient:   httpClient,
		RetryWaitMin: 1000 * time.Millisecond,
		RetryWaitMax: 1500 * time.Millisecond,
		RetryMax:     maxRetries,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      backoff,
		ErrorHandler: retryablehttp.PassthroughErrorHandler,
	}

	var result *Response
	resp, err := client.Do(req)
	if resp != nil {
		result = &Response{Response: resp}
	}
	if err != nil {
		if strings.Contains(err.Error(), "tls: oversized") {
			err = errwrap.Wrapf(
				"{{err}}\n\n"+
					"This error usually means that the server is running with TLS disabled\n"+
					"but the client is configured to use TLS. Please either enable TLS\n"+
					"on the server or run the client with -address set to an address\n"+
					"that uses the http protocol:\n\n"+
					"    vault <command> -address http://<address>\n\n"+
					"You can also set the VAULT_ADDR environment variable:\n\n\n"+
					"    VAULT_ADDR=http://<address> vault <command>\n\n"+
					"where <address> is replaced by the actual address to the server.",
				err)
		}
		return result, err
	}

	// Check for a redirect, only allowing for a single redirect
	if (resp.StatusCode == 301 || resp.StatusCode == 302 || resp.StatusCode == 307) && redirectCount == 0 {
		// Parse the updated location
		respLoc, err := resp.Location()
		if err != nil {
			return result, err
		}

		// Ensure a protocol downgrade doesn't happen
		if req.URL.Scheme == "https" && respLoc.Scheme != "https" {
			return result, fmt.Errorf("redirect would cause protocol downgrade")
		}

		// Update the request
		r.URL = respLoc

		// Reset the request body if any
		if err := r.ResetJSONBody(); err != nil {
			return result, err
		}

		// Retry the request
		redirectCount++
		goto START
	}

	if err := result.Error(); err != nil {
		return result, err
	}

	return result, nil
}
