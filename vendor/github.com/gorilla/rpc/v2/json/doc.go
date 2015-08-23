// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package gorilla/rpc/json provides a codec for JSON-RPC over HTTP services.

To register the codec in a RPC server:

	import (
		"http"
		"github.com/gorilla/rpc/v2"
		"github.com/gorilla/rpc/v2/json"
	)

	func init() {
		s := rpc.NewServer()
		s.RegisterCodec(json.NewCodec(), "application/json")
		// [...]
		http.Handle("/rpc", s)
	}

A codec is tied to a content type. In the example above, the server will use
the JSON codec for requests with "application/json" as the value for the
"Content-Type" header.

This package follows the JSON-RPC 1.0 specification:

	http://json-rpc.org/wiki/specification

Request format is:

	method:
		The name of the method to be invoked, as a string in dotted notation
		as in "Service.Method".
	params:
		An array with a single object to pass as argument to the method.
	id:
		The request id, a uint. It is used to match the response with the
		request that it is replying to.

Response format is:

	result:
		The Object that was returned by the invoked method,
		or null in case there was an error invoking the method.
	error:
		An Error object if there was an error invoking the method,
		or null if there was no error.
	id:
		The same id as the request it is responding to.

Check the gorilla/rpc documentation for more details:

	http://gorilla-web.appspot.com/pkg/rpc
*/
package json
