package cmd

import (
	"errors"
	"encoding/json"
	"net/http"
	"path/filepath"
	"net"
	"github.com/bgentry/speakeasy"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/api"
)

// User-supplied password, cached.
var apiPassword string

// bySiaPath implements sort.Interface for [] modules.FileInfo based on the
// SiaPath field.
type bySiaPath []modules.FileInfo

func (s bySiaPath) Len() int           { return len(s) }
func (s bySiaPath) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s bySiaPath) Less(i, j int) bool { return s[i].SiaPath < s[j].SiaPath }

// abs returns the absolute representation of a path.
// TODO: bad things can happen if you run siac from a non-existent directory.
// Implement some checks to catch this problem.
func abs(path string) string {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abspath
}

// non2xx returns true for non-success HTTP status codes.
func non2xx(code int) bool {
	return code < 200 || code > 299
}

// decodeError returns the api.Error from a API response. This method should
// only be called if the response's status code is non-2xx. The error returned
// may not be of type api.Error in the event of an error unmarshalling the
// JSON.
func decodeError(resp *http.Response) error {
	var apiErr api.Error
	err := json.NewDecoder(resp.Body).Decode(&apiErr)
	if err != nil {
		return err
	}
	return apiErr
}

// apiGet wraps a GET request with a status code check, such that if the GET does
// not return 2xx, the error will be read and returned. The response body is
// not closed.
func apiGet(addr, call string) (*http.Response, error) {
	if host, port, _ := net.SplitHostPort(addr); host == "" {
		addr = net.JoinHostPort("localhost", port)
	}
	resp, err := api.HttpGET("http://" + addr + call)
	if err != nil {
		return nil, errors.New("no response from daemon")
	}
	// check error code
	if resp.StatusCode == http.StatusUnauthorized {
		// retry request with authentication.
		resp.Body.Close()
		if apiPassword == "" {
			// prompt for password and store it in a global var for subsequent
			// calls
			apiPassword, err = speakeasy.Ask("API password: ")
			if err != nil {
				return nil, err
			}
		}
		resp, err = api.HttpGETAuthenticated("http://"+addr+call, apiPassword)
		if err != nil {
			return nil, errors.New("no response from daemon - authentication failed")
		}
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errors.New("API call not recognized: " + call)
	}
	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// apiPost wraps a POST request with a status code check, such that if the POST
// does not return 2xx, the error will be read and returned. The response body
// is not closed.
func apiPost(addr, call, vals string) (*http.Response, error) {
	
	if host, port, _ := net.SplitHostPort(addr); host == "" {
		addr = net.JoinHostPort("localhost", port)
	}

	resp, err := api.HttpPOST("http://"+addr+call, vals)
	if err != nil {
		return nil, errors.New("no response from daemon")
	}
	// check error code
	if resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		// Prompt for password and retry request with authentication.
		password, err := speakeasy.Ask("API password: ")
		if err != nil {
			return nil, err
		}
		resp, err = api.HttpPOSTAuthenticated("http://"+addr+call, vals, password)
		if err != nil {
			return nil, errors.New("no response from daemon - authentication failed")
		}
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errors.New("API call not recognized: " + call)
	}
	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// post makes an API call and discards the response. An error is returned if
// the response status is not 2xx.
func post(addr, call, vals string) error {
	resp, err := apiPost(addr, call, vals)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// getAPI makes a GET API call and decodes the response. An error is returned
// if the response status is not 2xx.
func getAPI(addr string, call string, obj interface{}) error {
	resp, err := apiGet(addr, call)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return errors.New("expecting a response, but API returned status code 204 No Content")
	}

	err = json.NewDecoder(resp.Body).Decode(obj)
	if err != nil {
		return err
	}
	return nil
}

// get makes an API call and discards the response. An error is returned if the
// response status is not 2xx.
func get(addr, call string) error {
	resp, err := apiGet(addr, call)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}